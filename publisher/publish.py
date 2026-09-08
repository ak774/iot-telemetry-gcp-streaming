import json
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1


PROJECT_ID = "iot-gcp-streaming"
TOPIC_ID = "iot-telemetry"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)


# ---------------------------------------------------------
# Use one identical event timestamp for both payloads.
# This ensures the only meaningful difference is the payload.
# ---------------------------------------------------------

event_timestamp = (
    datetime.now(timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)


EVENT_ID = "phase10-iam-v53-001"
DEVICE_ID = "phase10-iam-device"


# ---------------------------------------------------------
# FIRST PAYLOAD
# ---------------------------------------------------------

first_payload = {
    "event_id": EVENT_ID,
    "device_id": DEVICE_ID,
    "event_timestamp": event_timestamp,
    "temperature": 25.0,
    "humidity": 50.0,
    "pressure": 1000.0,
    "battery_level": 90.0
}


# ---------------------------------------------------------
# SECOND PAYLOAD
#
# Same event_id
# Same device_id
# Same event_timestamp
# DIFFERENT telemetry payload
# ---------------------------------------------------------

second_payload = {
    "event_id": EVENT_ID,
    "device_id": DEVICE_ID,
    "event_timestamp": event_timestamp,
    "temperature": 25.0,
    "humidity": 50.0,
    "pressure": 1100.0,
    "battery_level": 80.0
}


# ---------------------------------------------------------
# Publish helper
# ---------------------------------------------------------

def publish_event(label, payload):

    message = json.dumps(
        payload
    ).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message
    )

    message_id = future.result()

    print(
        f"{label}\n"
        f"  event_id={payload['event_id']}\n"
        f"  device_id={payload['device_id']}\n"
        f"  event_timestamp={payload['event_timestamp']}\n"
        f"  temperature={payload['temperature']}\n"
        f"  humidity={payload['humidity']}\n"
        f"  pressure={payload['pressure']}\n"
        f"  battery_level={payload['battery_level']}\n"
        f"  pubsub_message_id={message_id}\n"
    )


# ---------------------------------------------------------
# PHASE 10-IAM-V53-001
# Conflicting payload test
# ---------------------------------------------------------

print(
    "\n"
    "=========================================================\n"
    "PHASE 9G-2: CONFLICTING PAYLOAD TEST\n"
    "=========================================================\n"
)

print(f"event_id        : {EVENT_ID}")
print(f"device_id       : {DEVICE_ID}")
print(f"event_timestamp : {event_timestamp}")
print()


# First observation
publish_event(
    "FIRST PAYLOAD",
    first_payload
)


# Give Dataflow time to process the first event before
# sending the conflicting event.
'''time.sleep(200)


# Second observation with same event_id but different payload
publish_event(
    "SECOND PAYLOAD",
    second_payload
)


print(
    "=========================================================\n"
    "PHASE 10-IAM-V52-002 PUBLISH COMPLETE\n"
    "=========================================================\n"
)'''
