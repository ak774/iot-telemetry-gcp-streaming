import argparse
import json
import os
import subprocess
from datetime import datetime, timezone

from google.cloud import bigquery


PROJECT_ID = "iot-gcp-streaming"
DATASET_ID = "iot_silver"
TABLE_ID = "pipeline_deployments"

APPLICATION = "iot-telemetry-stream"
ENVIRONMENT = "prod"


def run_gcloud(args):
    command = ["gcloud"] + args

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        command = ["powershell", "-Command", "gcloud"] + args

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )

    return result.stdout.strip()


def get_active_deployment(client):
    query = f"""
        SELECT
          application,
          environment,
          deployment_id,
          job_name,
          job_id,
          commit_sha,
          deployed_at,
          status,
          previous_job_name
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE application = @application
          AND environment = @environment
          AND status = 'ACTIVE'
        ORDER BY deployed_at DESC
        LIMIT 2
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "application",
                "STRING",
                APPLICATION,
            ),
            bigquery.ScalarQueryParameter(
                "environment",
                "STRING",
                ENVIRONMENT,
            ),
        ]
    )

    rows = list(
        client.query(
            query,
            job_config=job_config,
        ).result()
    )

    if len(rows) > 1:
        raise RuntimeError(
            "Production identity invariant violated: "
            "more than one ACTIVE deployment exists."
        )

    return dict(rows[0]) if rows else None


def validate_candidate(job_name, job_id):
    output = run_gcloud(
        [
            "dataflow",
            "jobs",
            "describe",
            job_id,
            "--region=asia-south1",
            f"--project={PROJECT_ID}",
            "--format=json",
        ]
    )

    job = json.loads(output)

    actual_name = job.get("name")
    actual_id = job.get("id")
    state = job.get("currentState")

    if actual_id != job_id:
        raise RuntimeError(
            "Dataflow job ID validation failed."
        )

    if actual_name != job_name:
        raise RuntimeError(
            "Dataflow job name validation failed."
        )

    if state != "JOB_STATE_RUNNING":
        raise RuntimeError(
            f"Candidate Dataflow job is not RUNNING: {state}"
        )

    print(
        f"Candidate Dataflow job validated: "
        f"{job_name} ({job_id})"
    )

    return job


def insert_candidate(
    client,
    deployment_id,
    job_name,
    job_id,
    commit_sha,
):
    active = get_active_deployment(client)

    previous_job_name = (
        active["job_name"]
        if active
        else None
    )

    row = {
        "application": APPLICATION,
        "environment": ENVIRONMENT,
        "deployment_id": deployment_id,
        "job_name": job_name,
        "job_id": job_id,
        "commit_sha": commit_sha,
        "deployed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "CANDIDATE",
        "previous_job_name": previous_job_name,
    }

    errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        [row],
    )

    if errors:
        raise RuntimeError(
            f"Failed to insert candidate: {errors}"
        )

    print(
        f"Candidate deployment recorded: "
        f"{deployment_id}"
    )


def promote_candidate(
    client,
    deployment_id,
):
    query = f"""
        DECLARE candidate_count INT64;
        DECLARE active_count INT64;

        SET candidate_count = (
          SELECT COUNT(*)
          FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
          WHERE application = @application
            AND environment = @environment
            AND deployment_id = @deployment_id
            AND status = 'CANDIDATE'
        );

        SET active_count = (
          SELECT COUNT(*)
          FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
          WHERE application = @application
            AND environment = @environment
            AND status = 'ACTIVE'
        );

        ASSERT candidate_count = 1
        AS 'Expected exactly one CANDIDATE deployment.';

        ASSERT active_count <= 1
        AS 'More than one ACTIVE deployment exists.';

        UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        SET status = 'SUPERSEDED'
        WHERE application = @application
          AND environment = @environment
          AND status = 'ACTIVE';

        UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        SET status = 'ACTIVE'
        WHERE application = @application
          AND environment = @environment
          AND deployment_id = @deployment_id
          AND status = 'CANDIDATE';
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "application",
                "STRING",
                APPLICATION,
            ),
            bigquery.ScalarQueryParameter(
                "environment",
                "STRING",
                ENVIRONMENT,
            ),
            bigquery.ScalarQueryParameter(
                "deployment_id",
                "STRING",
                deployment_id,
            ),
        ]
    )

    client.query(
        query,
        job_config=job_config,
    ).result()

    print(
        f"Deployment promoted to ACTIVE: "
        f"{deployment_id}"
    )


def mark_failed(
    client,
    deployment_id,
):
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        SET status = 'FAILED'
        WHERE application = @application
          AND environment = @environment
          AND deployment_id = @deployment_id
          AND status = 'CANDIDATE'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "application",
                "STRING",
                APPLICATION,
            ),
            bigquery.ScalarQueryParameter(
                "environment",
                "STRING",
                ENVIRONMENT,
            ),
            bigquery.ScalarQueryParameter(
                "deployment_id",
                "STRING",
                deployment_id,
            ),
        ]
    )

    client.query(
        query,
        job_config=job_config,
    ).result()

    print(
        f"Deployment marked FAILED: "
        f"{deployment_id}"
    )


def validate_identity_invariant(client):
    query = f"""
        SELECT
          COUNTIF(status = 'ACTIVE') AS active_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE application = @application
          AND environment = @environment
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "application",
                "STRING",
                APPLICATION,
            ),
            bigquery.ScalarQueryParameter(
                "environment",
                "STRING",
                ENVIRONMENT,
            ),
        ]
    )

    row = next(
        iter(
            client.query(
                query,
                job_config=job_config,
            ).result()
        )
    )

    active_count = row["active_count"]

    if active_count != 1:
        raise RuntimeError(
            "Production identity invariant failed: "
            f"expected 1 ACTIVE deployment, "
            f"found {active_count}."
        )

    print(
        "Production identity invariant: PASS"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--deployment-id",
        required=True,
    )

    parser.add_argument(
        "--job-name",
        required=True,
    )

    parser.add_argument(
        "--job-id",
        required=True,
    )

    parser.add_argument(
        "--commit-sha",
        default=None,
    )

    parser.add_argument(
        "--promote",
        action="store_true",
    )

    parser.add_argument(
        "--fail",
        action="store_true",
    )

    args = parser.parse_args()

    if args.promote and args.fail:
        raise RuntimeError(
            "--promote and --fail are mutually exclusive."
        )

    client = bigquery.Client(
        project=PROJECT_ID
    )

    print("=== Stable Production Deployment Identity ===")

    print("\n[1] Validating candidate Dataflow job...")

    validate_candidate(
        args.job_name,
        args.job_id,
    )

    print("\n[2] Checking current production identity...")

    active = get_active_deployment(client)

    if active:
        print(
            f"Current ACTIVE: "
            f"{active['deployment_id']} "
            f"({active['job_name']})"
        )
    else:
        print("Current ACTIVE: none")

    print("\n[3] Recording candidate deployment...")

    insert_candidate(
        client,
        args.deployment_id,
        args.job_name,
        args.job_id,
        args.commit_sha,
    )

    if args.fail:
        print("\n[4] Marking candidate FAILED...")

        mark_failed(
            client,
            args.deployment_id,
        )

    elif args.promote:
        print("\n[4] Promoting candidate...")

        promote_candidate(
            client,
            args.deployment_id,
        )

    else:
        print(
            "\n[4] Candidate recorded. "
            "No promotion requested."
        )

    print("\n[5] Validating production identity...")

    if args.fail:
        # The previous ACTIVE deployment must remain.
        validate_identity_invariant(client)
    elif args.promote:
        validate_identity_invariant(client)

    print(
        "\n=== DEPLOYMENT IDENTITY OPERATION COMPLETE ==="
    )


if __name__ == "__main__":
    main()