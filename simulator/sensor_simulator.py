import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1


PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID = "iot-telemetry"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)


def generate_sensor_event(device_id: str) -> dict:
    """Generate one simulated IoT telemetry event."""

    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "temperature": round(
            random.uniform(20, 40),
            2
        ),
        "humidity": round(
            random.uniform(30, 80),
            2
        ),
        "pressure": round(
            random.uniform(990, 1030),
            2
        ),
        "battery_level": round(
            random.uniform(20, 100),
            2
        )
    }


def publish_event(event: dict) -> None:
    """Publish one event to Pub/Sub."""

    message = json.dumps(event).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message
    )

    message_id = future.result()

    print(
        f"Published | "
        f"message_id={message_id} | "
        f"device={event['device_id']} | "
        f"temperature={event['temperature']}"
    )


def main():

    devices = [
        "device-001",
        "device-002",
        "device-003",
        "device-004",
        "device-005"
    ]

    print("Starting IoT sensor simulator...")
    print(f"Project: {PROJECT_ID}")
    print(f"Topic: {TOPIC_ID}")

    try:
        while True:

            for device_id in devices:

                event = generate_sensor_event(
                    device_id
                )

                publish_event(event)

            print("-" * 80)

            time.sleep(2)

    except KeyboardInterrupt:

        print("\nSensor simulator stopped.")


if __name__ == "__main__":
    main()