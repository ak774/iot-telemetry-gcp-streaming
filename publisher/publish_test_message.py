
import json

from google.cloud import pubsub_v1


PROJECT_ID = "iot-gcp-streaming"
TOPIC_ID = "iot-telemetry"


publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)


payload = {
    "event_id": "phase8d-initial-001",
    "device_id": "phase-8d-device",
    "event_timestamp": "2026-09-04T16:36:10Z",
    "temperature": 20.0,
    "humidity": 50.0,
    "pressure": 1000.0,
    "battery_level": 90.0
}


message = json.dumps(payload).encode("utf-8")

future = publisher.publish(
    topic_path,
    message
)

message_id = future.result()

print(f"Published message: {message_id}")
print(f"Payload: {message.decode('utf-8')}")
