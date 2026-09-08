import json
import time
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1


PROJECT_ID = "iot-gcp-streaming"
TOPIC_ID = "iot-telemetry"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


now = datetime.now(timezone.utc)

# Previous complete minute.
window_start = (
    now.replace(
        second=0,
        microsecond=0
    )
    - timedelta(minutes=1)
)


def event_time(offset_seconds):
    return (
        window_start + timedelta(seconds=offset_seconds)
    ).isoformat().replace("+00:00", "Z")


def publish(payload):

    message = json.dumps(payload).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message
    )

    message_id = future.result()

    print(
        f"Published {payload['event_id']} "
        f"timestamp={payload['event_timestamp']} "
        f"message_id={message_id}"
    )


# ============================================================
# INITIAL EVENTS
# ============================================================

initial_messages = [

    {
        "event_id": "phase8d-initial-001",
        "device_id": "phase-8d-device",
        "event_timestamp": event_time(10),
        "temperature": 20.0,
        "humidity": 50.0,
        "pressure": 1000.0,
        "battery_level": 90.0
    },

    {
        "event_id": "phase8d-initial-002",
        "device_id": "phase-8d-device",
        "event_timestamp": event_time(20),
        "temperature": 30.0,
        "humidity": 60.0,
        "pressure": 1010.0,
        "battery_level": 80.0
    }
]


print("\n=== PHASE 8D: INITIAL EVENTS ===\n")

for payload in initial_messages:
    publish(payload)


print("\nInitial events published.")

print(
    "\nWaiting 90 seconds before publishing "
    "late event..."
)

time.sleep(90)


# ============================================================
# LATE EVENT
# ============================================================

late_message = {

    "event_id": "phase8d-late-001",

    "device_id": "phase-8d-device",

    "event_timestamp": event_time(30),

    "temperature": 100.0,

    "humidity": 70.0,

    "pressure": 1020.0,

    "battery_level": 70.0
}


print("\n=== PHASE 8D: LATE EVENT ===\n")

publish(late_message)

print("\nPhase 8D publishing complete.")