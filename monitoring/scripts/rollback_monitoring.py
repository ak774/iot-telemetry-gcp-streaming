import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ID = "iot-gcp-streaming"
DASHBOARD_ID = "f2a1f52c-f1d1-4dd9-b90f-c5ece590550b"

MONITORING_DIR = Path(__file__).resolve().parents[1]
BACKUP_DIR = MONITORING_DIR / "backups"

ALERT_POLICY_FILES = {
    "11342595123112398710": "11342595123112398710.json",
    "14645557611667265163": "14645557611667265163.json",
    "14740662748221802159": "14740662748221802159.json",
    "17119592956886294201": "17119592956886294201.json",
    "4870568466769178038": "4870568466769178038.json",
}

DASHBOARD_BACKUP = "production-dashboard.json"


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


def alert_policy_url(policy_id):
    return (
        "https://monitoring.googleapis.com/v3/"
        f"projects/{PROJECT_ID}/alertPolicies/{policy_id}"
    )


def dashboard_url():
    return (
        "https://monitoring.googleapis.com/v1/"
        f"projects/{PROJECT_ID}/dashboards/{DASHBOARD_ID}"
    )


ALERT_BACKUP_ID = "v53-alert-rollout"
DASHBOARD_BACKUP_ID = "v53-dashboard-rollout"


def alert_backup_path(filename):
    return BACKUP_DIR / ALERT_BACKUP_ID / filename


def dashboard_backup_path():
    return (
        BACKUP_DIR
        / DASHBOARD_BACKUP_ID
        / DASHBOARD_BACKUP
    )


def validate_backup_directories():
    alert_directory = BACKUP_DIR / ALERT_BACKUP_ID
    dashboard_directory = BACKUP_DIR / DASHBOARD_BACKUP_ID

    if not alert_directory.exists():
        raise RuntimeError(
            f"Alert backup directory does not exist: "
            f"{alert_directory}"
        )

    if not dashboard_directory.exists():
        raise RuntimeError(
            f"Dashboard backup directory does not exist: "
            f"{dashboard_directory}"
        )

    for filename in ALERT_POLICY_FILES.values():
        path = alert_directory / filename

        if not path.exists():
            raise RuntimeError(
                f"Missing alert backup: {path}"
            )

        load_json(path)

    dashboard_path = dashboard_directory / DASHBOARD_BACKUP

    if not dashboard_path.exists():
        raise RuntimeError(
            f"Missing dashboard backup: {dashboard_path}"
        )

    load_json(dashboard_path)

    print("Backup completeness validation: PASS")


def backup_current_live_state(token, rollback_id):
    destination = BACKUP_DIR / rollback_id

    print(f"Creating rollback safety backup: {destination}")

    for policy_id in ALERT_POLICY_FILES:
        live = api_request(
            token,
            alert_policy_url(policy_id),
        )

        output_path = destination / f"{policy_id}.json"
        write_json(output_path, live)

        print(f"Current alert backup: {output_path}")

    live_dashboard = api_request(
        token,
        dashboard_url(),
    )

    dashboard_path = destination / DASHBOARD_BACKUP
    write_json(
        dashboard_path,
        live_dashboard,
    )

    print(
        f"Current dashboard backup: {dashboard_path}"
    )

    print("Current production backup: PASS")


def restore_alert_policy(
    token,
    policy_id,
    backup,
):
    conditions = backup.get("conditions")

    if conditions is None:
        raise RuntimeError(
            f"Backup {policy_id} does not contain conditions."
        )

    url = (
        alert_policy_url(policy_id)
        + "?updateMask=conditions"
    )

    body = {
        "name": backup["name"],
        "conditions": conditions,
    }

    api_request(
        token,
        url,
        method="PATCH",
        body=body,
    )

    print(
        f"Restored alert policy {policy_id}: PASS"
    )


def restore_dashboard(
    token,
    backup,
):
    live = api_request(
        token,
        dashboard_url(),
    )

    live_etag = live.get("etag")

    if not live_etag:
        raise RuntimeError(
            "Live dashboard does not contain an etag."
        )

    deployment_body = dict(backup)
    deployment_body["etag"] = live_etag

    api_request(
        token,
        dashboard_url(),
        method="PATCH",
        body=deployment_body,
    )

    print(
        f"Restored dashboard with fresh etag "
        f"{live_etag}: PASS"
    )


def verify_alert_policy(
    token,
    policy_id,
    expected,
):
    live = api_request(
        token,
        alert_policy_url(policy_id),
    )

    expected_conditions = expected.get(
        "conditions",
        [],
    )

    actual_conditions = live.get(
        "conditions",
        [],
    )

    if actual_conditions != expected_conditions:
        raise RuntimeError(
            f"Alert policy {policy_id} verification failed."
        )

    if live.get("name") != expected.get("name"):
        raise RuntimeError(
            f"Alert policy {policy_id} identity mismatch."
        )

    print(
        f"Verified alert policy {policy_id}: PASS"
    )


def verify_dashboard(
    token,
    expected,
):
    live = api_request(
        token,
        dashboard_url(),
    )

    expected_widgets = (
        expected
        .get("gridLayout", {})
        .get("widgets", [])
    )

    actual_widgets = (
        live
        .get("gridLayout", {})
        .get("widgets", [])
    )

    if live.get("name") != expected.get("name"):
        raise RuntimeError(
            "Dashboard identity verification failed."
        )

    if len(actual_widgets) != len(expected_widgets):
        raise RuntimeError(
            "Dashboard widget count verification failed."
        )

    if actual_widgets != expected_widgets:
        raise RuntimeError(
            "Dashboard widget configuration verification failed."
        )

    print("Verified dashboard: PASS")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Rollback production Cloud Monitoring "
            "configuration from a saved deployment backup."
        )
    )

    parser.add_argument(
        "--backup-id",
        required=True,
        help="Backup deployment ID to restore.",
    )

    parser.add_argument(
        "--rollback-id",
        required=True,
        help=(
            "ID for the safety backup created immediately "
            "before rollback."
        ),
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform rollback.",
    )

    args = parser.parse_args()

    token = os.environ.get(
        "GOOGLE_ACCESS_TOKEN"
    )

    if not token:
        raise RuntimeError(
            "GOOGLE_ACCESS_TOKEN environment variable "
            "is not set."
        )

    print("=== 10.8.2I.7 Monitoring Rollback ===")
    print(f"Project:       {PROJECT_ID}")
    print(f"Backup:        {args.backup_id}")
    print(f"Rollback ID:   {args.rollback_id}")
    print(
        f"Mode:          "
        f"{'APPLY' if args.apply else 'DRY-RUN'}"
    )

    print("\n[1/5] Validating rollback backup...")
    validate_backup_directories()

    if not args.apply:
        print(
            "\nDRY-RUN: no Monitoring resources changed."
        )
        print(
            "Rollback backup is complete and valid."
        )
        print(
            "Re-run with --apply to perform rollback."
        )
        return

    print(
        "\n[2/5] Backing up current production state..."
    )

    backup_current_live_state(
        token,
        args.rollback_id,
    )

    print("\n[3/5] Restoring alert policies...")

    for policy_id, filename in ALERT_POLICY_FILES.items():
        path = alert_backup_path(filename)

        backup = load_json(path)

        restore_alert_policy(
            token,
            policy_id,
            backup,
        )

    print("\n[4/5] Restoring dashboard...")

    dashboard_backup = load_json(
        dashboard_backup_path()
    )

    restore_dashboard(
        token,
        dashboard_backup,
    )

    print("\n[5/5] Verifying rollback...")

    for policy_id, filename in ALERT_POLICY_FILES.items():
        expected = load_json(
            alert_backup_path(filename)
        )

        verify_alert_policy(
            token,
            policy_id,
            expected,
        )

    verify_dashboard(
        token,
        dashboard_backup,
    )

    print(
        "\n=== MONITORING ROLLBACK COMPLETE ==="
    )


if __name__ == "__main__":
    main()