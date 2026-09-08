import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.conflict_detection import (
    DetectTelemetryConflicts,
)


def make_record(
    event_id,
    payload_hash,
    temperature=25.0,
):
    return {
        "event_id": event_id,
        "device_id": "device-001",
        "event_timestamp": "2026-08-30T15:00:00Z",
        "temperature": temperature,
        "humidity": 50.0,
        "pressure": 1000.0,
        "battery_level": 90.0,
        "payload_hash": payload_hash,
    }


def test_same_payload_is_not_conflict():

    records = [
        make_record("event-001", "hash-A"),
        make_record("event-001", "hash-A"),
    ]

    with TestPipeline() as p:

        outputs = (
            p
            | beam.Create(records)
            | DetectTelemetryConflicts()
        )

        assert_that(
            outputs.clean,
            equal_to(records),
            label="CleanRecords",
        )

        assert_that(
            outputs.conflict,
            equal_to([]),
            label="Conflicts",
        )


def test_different_payload_same_event_id_is_conflict():

    first = make_record(
        "event-002",
        "hash-A",
        temperature=25.0,
    )

    second = make_record(
        "event-002",
        "hash-B",
        temperature=100.0,
    )

    with TestPipeline() as p:

        outputs = (
            p
            | beam.Create([first, second])
            | DetectTelemetryConflicts()
        )

        assert_that(
            outputs.clean,
            equal_to([first]),
            label="CleanRecords",
        )

        expected_conflict = dict(second)

        expected_conflict["conflict_reason"] = (
            "same_event_id_different_payload_hash"
        )
        expected_conflict["original_payload_hash"] = "hash-A"
        expected_conflict["conflicting_payload_hash"] = "hash-B"

        assert_that(
            outputs.conflict,
            equal_to([expected_conflict]),
            label="Conflicts",
        )


def test_different_event_ids_are_independent():

    first = make_record(
        "event-003",
        "hash-A",
    )

    second = make_record(
        "event-004",
        "hash-B",
    )

    with TestPipeline() as p:

        outputs = (
            p
            | beam.Create([first, second])
            | DetectTelemetryConflicts()
        )

        assert_that(
            outputs.clean,
            equal_to([first, second]),
            label="CleanRecords",
        )

        assert_that(
            outputs.conflict,
            equal_to([]),
            label="Conflicts",
        )