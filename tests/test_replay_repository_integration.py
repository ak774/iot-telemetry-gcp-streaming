import os
import uuid
from datetime import datetime, timezone
from dataflow.transforms.replay_repository import (
    ReplayRegistryRepository,
)


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "iot-gcp-streaming")
INSTANCE_ID = os.getenv("SPANNER_INSTANCE_ID", "iot-streaming")
DATABASE_ID = os.getenv("SPANNER_DATABASE_ID", "iot_registry_test")


def make_record(event_id):
    return {
        "event_id": event_id,
        "device_id": "integration-test-device",
        "event_timestamp": "2026-09-04T11:00:00Z",
        "payload_hash": "integration-test-hash-001",
    }


def test_new_replay_and_conflict_against_spanner():
    repository = ReplayRegistryRepository(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
    )

    repository.connect()

    event_id = f"spanner-test-{uuid.uuid4()}"

    record = make_record(event_id)

    # 1. First arrival → NEW
    first_result = repository.register_event(record)

    assert first_result["status"] == "new"

    # 2. Same event_id + same hash → REPLAY
    replay_result = repository.register_event(record)

    assert replay_result["status"] == "replay"
    assert (
        replay_result["registry_record"]["event_id"]
        == event_id
    )
    assert (
        replay_result["registry_record"]["payload_hash"]
        == "integration-test-hash-001"
    )

    # 3. Same event_id + different hash → CONFLICT
    conflicting_record = dict(record)

    conflicting_record["payload_hash"] = (
        "integration-test-hash-002"
    )

    conflict_result = repository.register_event(
        conflicting_record
    )

    assert conflict_result["status"] == "conflict"

    assert (
        conflict_result["registry_record"]["payload_hash"]
        == "integration-test-hash-001"
    )