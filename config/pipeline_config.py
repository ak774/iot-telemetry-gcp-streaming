import os


PROJECT_ID = os.environ["GCP_PROJECT_ID"]

PUBSUB_SUBSCRIPTION = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    f"projects/{PROJECT_ID}/subscriptions/dataflow-telemetry-sub"
)

DLQ_TOPIC = os.getenv(
    "DLQ_TOPIC",
    f"projects/{PROJECT_ID}/topics/iot-telemetry-dlq"
)

RAW_TABLE = (
    f"{PROJECT_ID}:iot_raw.raw_telemetry"
)

SILVER_TABLE = (
    f"{PROJECT_ID}:iot_silver.telemetry_silver"
)

GOLD_TABLE = (
    f"{PROJECT_ID}:iot_gold.telemetry_gold"
)

CONFLICT_TABLE = (
    f"{PROJECT_ID}:iot_silver.telemetry_conflicts"
)

WINDOW_SIZE_SECONDS = 60

ALLOWED_LATENESS_SECONDS = 120

DEDUPLICATION_DURATION_SECONDS = 600

SPANNER_INSTANCE_ID = "iot-streaming"
SPANNER_DATABASE_ID = "iot_registry"