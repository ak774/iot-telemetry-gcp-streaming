import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ID = "iot-gcp-streaming"
DASHBOARD_ID = "f2a1f52c-f1d1-4dd9-b90f-c5ece590550b"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MONITORING_DIR = PROJECT_ROOT / "monitoring"
GENERATED_DIR = MONITORING_DIR / "generated"
BACKUP_DIR = MONITORING_DIR / "backups"


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
        file.write("\n")


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
        response_body = exc.read().decode(
            "utf-8",
            errors="replace",
        )

        raise RuntimeError(
            f"HTTP {exc.code} calling {method} {url}\n"
            f"{response_body}"
        ) from exc


def dashboard_url():
    return (
        "https://monitoring.googleapis.com/v1/"
        f"projects/{PROJECT_ID}/dashboards/{DASHBOARD_ID}"
    )


def fetch_live_dashboard(token):
    return api_request(
        token,
        dashboard_url(),
    )


def validate_generated_dashboard(
    generated,
    job_name,
    job_id,
):
    if generated.get("name") != (
        f"projects/391196644919/dashboards/{DASHBOARD_ID}"
    ):
        raise RuntimeError(
            "Generated dashboard name does not match expected dashboard."
        )

    widgets = generated.get(
        "gridLayout",
        {},
    ).get(
        "widgets",
        [],
    )

    if len(widgets) != 15:
        raise RuntimeError(
            f"Expected 15 generated widgets, found {len(widgets)}."
        )

    serialized = json.dumps(generated)

    if "__DATAFLOW_JOB_NAME__" in serialized:
        raise RuntimeError(
            "Generated dashboard still contains "
            "__DATAFLOW_JOB_NAME__."
        )

    if "__DATAFLOW_JOB_ID__" in serialized:
        raise RuntimeError(
            "Generated dashboard still contains "
            "__DATAFLOW_JOB_ID__."
        )

    if "iot-telemetry-stream-v51" in serialized:
        raise RuntimeError(
            "Generated dashboard still references v51."
        )

    if "2026-09-06_06_04_06-13782454674942107052" in serialized:
        raise RuntimeError(
            "Generated dashboard still references the old Dataflow job ID."
        )

    if job_name not in serialized:
        raise RuntimeError(
            f"Generated dashboard does not contain {job_name}."
        )

    if job_id not in serialized:
        raise RuntimeError(
            f"Generated dashboard does not contain {job_id}."
        )

    print("Generated dashboard validation: PASS")


def validate_live_dashboard(
    live,
    generated,
    job_name,
    job_id,
):
    if live.get("name") != generated.get("name"):
        raise RuntimeError(
            "Live and generated dashboard names do not match."
        )

    live_widgets = live.get(
        "gridLayout",
        {},
    ).get(
        "widgets",
        [],
    )

    generated_widgets = generated.get(
        "gridLayout",
        {},
    ).get(
        "widgets",
        [],
    )

    if len(live_widgets) != len(generated_widgets):
        raise RuntimeError(
            "Live/generated widget count mismatch: "
            f"live={len(live_widgets)}, "
            f"generated={len(generated_widgets)}"
        )

    live_serialized = json.dumps(live)

    if job_name not in live_serialized:
        raise RuntimeError(
            f"Live dashboard does not contain {job_name}."
        )

    if job_id not in live_serialized:
        raise RuntimeError(
            f"Live dashboard does not contain {job_id}."
        )

    if "iot-telemetry-stream-v51" in live_serialized:
        raise RuntimeError(
            "Live dashboard still references v51."
        )

    if "2026-09-06_06_04_06-13782454674942107052" in live_serialized:
        raise RuntimeError(
            "Live dashboard still references the old Dataflow job ID."
        )

    print("Live dashboard verification: PASS")


def backup_live_dashboard(
    live,
    deployment_id,
):
    backup_path = (
        BACKUP_DIR
        / deployment_id
        / "production-dashboard.json"
    )

    write_json(
        backup_path,
        live,
    )

    print(
        f"Dashboard backup: {backup_path}"
    )


def deploy_dashboard(
    token,
    generated,
    live_etag,
):
    deployment_body = dict(generated)

    # Always use the freshest live etag.
    deployment_body["etag"] = live_etag

    return api_request(
        token,
        dashboard_url(),
        method="PATCH",
        body=deployment_body,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Safely deploy the production Cloud Monitoring dashboard."
        )
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
        "--deployment-id",
        required=True,
    )

    parser.add_argument(
        "--apply",
        action="store_true",
    )

    args = parser.parse_args()

    token = os.environ.get(
        "GOOGLE_ACCESS_TOKEN"
    )

    if not token:
        raise RuntimeError(
            "GOOGLE_ACCESS_TOKEN environment variable is not set."
        )

    generated_path = (
        GENERATED_DIR
        / "production-dashboard.json"
    )

    generated = load_json(
        generated_path
    )

    print(
        "=== 10.8.2I.6 Dashboard Deployment ==="
    )

    print(
        f"Project:      {PROJECT_ID}"
    )

    print(
        f"Dashboard:    {DASHBOARD_ID}"
    )

    print(
        f"Dataflow job: {args.job_name}"
    )

    print(
        f"Dataflow ID:  {args.job_id}"
    )

    print(
        f"Deployment:   {args.deployment_id}"
    )

    print(
        f"Mode:         "
        f"{'APPLY' if args.apply else 'DRY-RUN'}"
    )

    print(
        "\n[1/4] Fetching current live dashboard..."
    )

    live = fetch_live_dashboard(token)

    live_etag = live.get("etag")

    if not live_etag:
        raise RuntimeError(
            "Live dashboard does not contain an etag."
        )

    print(
        f"Live etag: {live_etag}"
    )

    print(
        "\n[2/4] Validating generated dashboard..."
    )

    validate_generated_dashboard(
        generated,
        args.job_name,
        args.job_id,
    )

    print(
        "\n[3/4] Comparing dashboard identity..."
    )

    if live.get("name") != generated.get("name"):
        raise RuntimeError(
            "Live and generated dashboard identities differ."
        )

    print(
        "Dashboard identity validation: PASS"
    )

    if not args.apply:
        print(
            "\nDRY-RUN: no dashboard changes made."
        )

        print(
            "Fresh live etag captured successfully."
        )

        print(
            "Re-run with --apply to deploy."
        )

        return

    print(
        "\n[4/4] Backing up and deploying dashboard..."
    )

    backup_live_dashboard(
        live,
        args.deployment_id,
    )

    updated = deploy_dashboard(
        token,
        generated,
        live_etag,
    )

    updated_etag = updated.get("etag")

    print(
        f"Dashboard updated. New etag: {updated_etag}"
    )

    print(
        "\nVerifying live dashboard..."
    )

    verified_live = fetch_live_dashboard(token)

    validate_live_dashboard(
        verified_live,
        generated,
        args.job_name,
        args.job_id,
    )

    print(
        "\n=== DASHBOARD DEPLOYMENT COMPLETE ==="
    )


if __name__ == "__main__":
    main()