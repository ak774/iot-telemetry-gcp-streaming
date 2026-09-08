import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MONITORING_DIR = PROJECT_ROOT / "monitoring"
TEMPLATES_DIR = MONITORING_DIR / "templates"
GENERATED_DIR = MONITORING_DIR / "generated"


ALERT_FILES = [
    "alert-invalid-telemetry.json",
    "alert-dataflow-lag.json",
    "alert-processing-latency.json",
    "alert-spanner-failure.json",
    "alert-payload-conflict.json",
]

DASHBOARD_FILE = "production-dashboard.json"

JOB_NAME_PLACEHOLDER = "__DATAFLOW_JOB_NAME__"
JOB_ID_PLACEHOLDER = "__DATAFLOW_JOB_ID__"


def replace_deployment_targets(value, job_name, job_id):
    if isinstance(value, dict):
        return {
            key: replace_deployment_targets(item, job_name, job_id)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            replace_deployment_targets(item, job_name, job_id)
            for item in value
        ]

    if isinstance(value, str):
        return (
            value
            .replace(JOB_NAME_PLACEHOLDER, job_name)
            .replace(JOB_ID_PLACEHOLDER, job_id)
        )

    return value


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
        file.write("\n")


def validate_template(data, filename):
    serialized = json.dumps(data)

    has_job_name = JOB_NAME_PLACEHOLDER in serialized
    has_job_id = JOB_ID_PLACEHOLDER in serialized

    if not has_job_name and not has_job_id:
        raise ValueError(
            f"{filename} contains neither "
            f"{JOB_NAME_PLACEHOLDER} nor {JOB_ID_PLACEHOLDER}"
        )


def validate_generated_config(data, filename, job_name, job_id):
    serialized = json.dumps(data)

    if JOB_NAME_PLACEHOLDER in serialized:
        raise ValueError(
            f"{filename} still contains {JOB_NAME_PLACEHOLDER}"
        )

    if JOB_ID_PLACEHOLDER in serialized:
        raise ValueError(
            f"{filename} still contains {JOB_ID_PLACEHOLDER}"
        )

    if job_name not in serialized and job_id not in serialized:
        raise ValueError(
            f"{filename} contains neither the supplied "
            f"job name nor job ID"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate deployment-specific Monitoring "
            "configurations from templates."
        )
    )

    parser.add_argument(
        "--job-name",
        required=True,
        help="Active Dataflow job name.",
    )

    parser.add_argument(
        "--job-id",
        required=True,
        help="Active Dataflow job ID.",
    )

    args = parser.parse_args()

    if not args.job_name.strip():
        raise ValueError("job-name cannot be empty.")

    if not args.job_id.strip():
        raise ValueError("job-id cannot be empty.")

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    input_files = ALERT_FILES + [DASHBOARD_FILE]

    for filename in input_files:
        source_path = TEMPLATES_DIR / filename
        output_path = GENERATED_DIR / filename

        if not source_path.exists():
            raise FileNotFoundError(
                f"Template not found: {source_path}"
            )

        data = load_json(source_path)

        validate_template(data, filename)

        updated_data = replace_deployment_targets(
            data,
            args.job_name,
            args.job_id,
        )

        validate_generated_config(
            updated_data,
            filename,
            args.job_name,
            args.job_id,
        )

        write_json(output_path, updated_data)

        print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()