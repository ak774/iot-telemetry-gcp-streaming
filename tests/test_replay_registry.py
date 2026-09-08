import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from dataflow.transforms.replay_registry import (
    PrepareReplayRegistryRecord,
)


def check_registry_record(actual):
    assert len(actual) == 1

    record = actual[0]

    assert record["event_id"] == "event-001"
    assert record["device_id"] == "device-001"
    assert (
        record["event_timestamp"]
        == "2026-09-04T12:00:00Z"
    )
    assert record["payload_hash"] == "hash-a"
    assert record["first_seen_at"] is not None
    assert record["last_seen_at"] is not None


def test_prepare_replay_registry_record():

    record = {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T12:00:00Z",
        "payload_hash": "hash-a",
    }

    with TestPipeline() as p:

        output = (
            p
            | "CreateRegistryInput"
            >> beam.Create([record])
            | "PrepareRegistryRecord"
            >> beam.ParDo(
                PrepareReplayRegistryRecord()
            )
        )

        assert_that(
            output,
            check_registry_record,
            label="CheckRegistryRecord",
        )