import argparse
import json
import os
import shutil
import subprocess
import sys


def get_gcloud_command():
    """
    Resolve the Google Cloud CLI executable across platforms.
    """
    gcloud = shutil.which("gcloud")

    if gcloud and not gcloud.lower().endswith(".ps1"):
        return [gcloud]

    if os.name == "nt":
        powershell = shutil.which("powershell")

        if powershell:
            return [powershell, "-Command"]

    if gcloud:
        return [gcloud]

    print(
        "ERROR: Google Cloud CLI executable was not found.",
        file=sys.stderr,
    )
    sys.exit(1)


def get_job(job_name, job_id, project_id, region):
    gcloud_command = get_gcloud_command()

    gcloud_arguments = [
        "dataflow",
        "jobs",
        "describe",
        job_id,
        "--region",
        region,
        "--project",
        project_id,
        "--format=json",
    ]

    if os.name == "nt" and gcloud_command[-1] == "-Command":
        command = gcloud_command + [
            "gcloud " + " ".join(
                f'"{argument}"' if " " in argument else argument
                for argument in gcloud_arguments
            )
        ]
    else:
        command = gcloud_command + gcloud_arguments

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        print(
            "ERROR: Unable to retrieve Dataflow job.",
            file=sys.stderr,
        )
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        print(
            "ERROR: Invalid JSON returned by gcloud.",
            file=sys.stderr,
        )
        print(result.stdout, file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Validate a Dataflow deployment target."
    )

    parser.add_argument(
        "--job-name",
        required=True,
        help="Expected Dataflow job name.",
    )

    parser.add_argument(
        "--job-id",
        required=True,
        help="Expected Dataflow job ID.",
    )

    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID.",
    )

    parser.add_argument(
        "--region",
        required=True,
        help="Dataflow region.",
    )

    args = parser.parse_args()

    job = get_job(
        args.job_name,
        args.job_id,
        args.project,
        args.region
    )

    actual_name = job.get("name")
    actual_id = job.get("id")
    current_state = job.get("currentState")

    print(f"Job name:      {actual_name}")
    print(f"Job ID:        {actual_id}")
    print(f"Current state: {current_state}")

    if actual_name != args.job_name:
        print(
            "ERROR: Dataflow job name does not match deployment target.",
            file=sys.stderr,
        )
        sys.exit(1)

    if actual_id != args.job_id:
        print(
            "ERROR: Dataflow job ID does not match deployment target.",
            file=sys.stderr,
        )
        sys.exit(1)

    if current_state != "JOB_STATE_RUNNING":
        print(
            "ERROR: Dataflow job is not RUNNING.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Dataflow deployment target validation: PASS")


if __name__ == "__main__":
    main()