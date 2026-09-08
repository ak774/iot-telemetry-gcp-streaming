import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ID = "iot-gcp-streaming"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MONITORING_DIR = PROJECT_ROOT / "monitoring"
GENERATED_DIR = MONITORING_DIR / "generated"
BACKUP_DIR = MONITORING_DIR / "backups"

ALERT_FILES = [
    "alert-invalid-telemetry.json",
    "alert-dataflow-lag.json",
    "alert-processing-latency.json",
    "alert-spanner-failure.json",
    "alert-payload-conflict.json",
]


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def api_request(token, url, method="GET", body=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(request) as response:
            content = response.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} calling {method} {url}\n{response_body}"
        ) from exc


def validate_dataflow_job(token, job_id, expected_job_name):
    url = (
        "https://dataflow.googleapis.com/v1b3/projects/"
        f"{PROJECT_ID}/locations/asia-south1/jobs/{job_id}"
    )

    job = api_request(token, url)

    actual_id = job.get("id")
    actual_name = job.get("name")
    state = job.get("currentState")

    print(f"Job name:      {actual_name}")
    print(f"Job ID:        {actual_id}")
    print(f"Current state: {state}")

    if actual_id != job_id:
        raise RuntimeError(
            f"Dataflow job ID mismatch: expected {job_id}, got {actual_id}"
        )

    if actual_name != expected_job_name:
        raise RuntimeError(
            f"Dataflow job name mismatch: expected {expected_job_name}, "
            f"got {actual_name}"
        )

    if state != "JOB_STATE_RUNNING":
        raise RuntimeError(
            f"Dataflow job is not RUNNING: {state}"
        )

    print("Dataflow deployment target validation: PASS")


def validate_generated_configs(job_name, job_id):
    print("\nValidating generated Monitoring configurations...")

    for filename in ALERT_FILES:
        path = GENERATED_DIR / filename

        if not path.exists():
            raise FileNotFoundError(
                f"Generated configuration missing: {path}"
            )

        data = load_json(path)
        serialized = json.dumps(data)

        if "__DATAFLOW_JOB_NAME__" in serialized:
            raise RuntimeError(
                f"{filename} still contains __DATAFLOW_JOB_NAME__"
            )

        if "__DATAFLOW_JOB_ID__" in serialized:
            raise RuntimeError(
                f"{filename} still contains __DATAFLOW_JOB_ID__"
            )

        if job_name not in serialized and job_id not in serialized:
            raise RuntimeError(
                f"{filename} contains neither supplied job name nor job ID"
            )

        if "name" not in data:
            raise RuntimeError(
                f"{filename} does not contain an alert policy name"
            )

        if "conditions" not in data or not data["conditions"]:
            raise RuntimeError(
                f"{filename} does not contain conditions"
            )

    print("Generated Monitoring configuration validation: PASS")


def get_policy_id(policy):
    name = policy.get("name")

    if not name:
        raise RuntimeError("Generated policy has no name")

    return name.split("/")[-1]


def build_condition_patch(generated):
    return {
        "name": generated["name"],
        "conditions": generated["conditions"],
    }


def compare_condition_targets(live, generated):
    live_conditions = live.get("conditions", [])
    generated_conditions = generated.get("conditions", [])

    if len(live_conditions) != len(generated_conditions):
        return False, (
            f"condition count mismatch: "
            f"live={len(live_conditions)}, "
            f"generated={len(generated_conditions)}"
        )

    for index, generated_condition in enumerate(generated_conditions):
        live_condition = live_conditions[index]

        if live_condition.get("name") != generated_condition.get("name"):
            return False, (
                f"condition {index} name mismatch: "
                f"live={live_condition.get('name')}, "
                f"generated={generated_condition.get('name')}"
            )

        live_pql = live_condition.get(
            "conditionPrometheusQueryLanguage", {}
        )
        generated_pql = generated_condition.get(
            "conditionPrometheusQueryLanguage", {}
        )

        if live_pql != generated_pql:
            return False, (
                f"condition {index} PQL configuration differs"
            )

    return True, "conditions match"


def verify_live_policy(token, policy_id, generated):
    url = (
        "https://monitoring.googleapis.com/v3/projects/"
        f"{PROJECT_ID}/alertPolicies/{policy_id}"
    )

    live = api_request(token, url)

    if live.get("name") != generated.get("name"):
        raise RuntimeError(
            f"Policy name mismatch for {policy_id}"
        )

    matches, reason = compare_condition_targets(
        live,
        generated,
    )

    if not matches:
        raise RuntimeError(
            f"Policy {policy_id} verification failed: {reason}"
        )

    print(
        f"Verified policy {policy_id}: PASS"
    )


def backup_policy(token, policy_id, deployment_id):
    backup_dir = BACKUP_DIR / deployment_id
    backup_dir.mkdir(parents=True, exist_ok=True)

    url = (
        "https://monitoring.googleapis.com/v3/projects/"
        f"{PROJECT_ID}/alertPolicies/{policy_id}"
    )

    live = api_request(token, url)

    output_path = backup_dir / f"{policy_id}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(live, file, indent=2)
        file.write("\n")

    print(f"Backup: {output_path}")


def update_policy(token, generated):
    policy_id = get_policy_id(generated)

    url = (
        "https://monitoring.googleapis.com/v3/projects/"
        f"{PROJECT_ID}/alertPolicies/{policy_id}"
        "?updateMask=conditions"
    )

    patch_body = build_condition_patch(generated)

    api_request(
        token,
        url,
        method="PATCH",
        body=patch_body,
    )

    print(
        f"Updated policy {policy_id}: conditions"
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Safely deploy Dataflow-targeted Cloud Monitoring "
            "alert policies."
        )
    )

    parser.add_argument(
        "--job-name",
        required=True,
        help="Expected active Dataflow job name.",
    )

    parser.add_argument(
        "--job-id",
        required=True,
        help="Expected active Dataflow job ID.",
    )

    parser.add_argument(
        "--deployment-id",
        required=True,
        help="Deployment identifier used for backups.",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually update alert policies.",
    )

    args = parser.parse_args()

    token = os.environ.get("GOOGLE_ACCESS_TOKEN")

    if not token:
        raise RuntimeError(
            "GOOGLE_ACCESS_TOKEN environment variable is not set."
        )

    print("=== 10.8.2I.5B Alert Deployment ===")
    print(f"Project:       {PROJECT_ID}")
    print(f"Dataflow job:  {args.job_name}")
    print(f"Dataflow ID:   {args.job_id}")
    print(f"Deployment:    {args.deployment_id}")
    print(
        f"Mode:          "
        f"{'APPLY' if args.apply else 'DRY-RUN'}"
    )

    print("\n[1/5] Validating Dataflow job...")
    validate_dataflow_job(
        token,
        args.job_id,
        args.job_name,
    )

    print("\n[2/5] Validating generated configurations...")
    validate_generated_configs(
        args.job_name,
        args.job_id,
    )

    generated_configs = []

    for filename in ALERT_FILES:
        generated = load_json(
            GENERATED_DIR / filename
        )

        policy_id = get_policy_id(generated)

        generated_configs.append(
            (filename, policy_id, generated)
        )

    print("\n[3/5] Preparing policies...")

    for filename, policy_id, generated in generated_configs:
        print(
            f"{filename}: policy {policy_id}"
        )

    if not args.apply:
        print("\nDRY-RUN: no Monitoring resources were changed.")
        print(
            "Validation complete. Re-run with --apply "
            "to update the policies."
        )
        return

    print("\n[4/5] Backing up current live policies...")

    for _, policy_id, _ in generated_configs:
        backup_policy(
            token,
            policy_id,
            args.deployment_id,
        )

    print("\n[5/5] Updating alert policies...")

    for filename, policy_id, generated in generated_configs:
        print(f"\nDeploying {filename}...")
        update_policy(
            token,
            generated,
        )

        verify_live_policy(
            token,
            policy_id,
            generated,
        )

    print("\n=== ALERT DEPLOYMENT COMPLETE ===")
    print("All five alert policies verified successfully.")


if __name__ == "__main__":
    main()