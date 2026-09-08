import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from dataflow.transforms.replay_detection import (
    ClassifyReplay,
)


def check_new(actual):
    assert len(actual) == 1
    assert actual[0]["event_id"] == "event-001"


def check_replay(actual):
    assert len(actual) == 1
    assert actual[0]["event_id"] == "event-002"
    assert (
        actual[0]["replay_reason"]
        == "event_id_and_payload_hash_already_seen"
    )


def check_conflict(actual):
    assert len(actual) == 1
    assert actual[0]["event_id"] == "event-003"
    assert (
        actual[0]["conflict_reason"]
        == "same_event_id_different_payload_hash"
    )
    assert actual[0]["original_payload_hash"] == "hash-a"
    assert actual[0]["conflicting_payload_hash"] == "hash-b"


def test_new_event():
    record = {
        "event_id": "event-001",
        "payload_hash": "hash-a",
    }

    with TestPipeline() as p:
        result = (
            p
            | "CreateNewEvent"
            >> beam.Create([
                (record, None)
            ])
            | "ClassifyNewEvent"
            >> beam.ParDo(
                ClassifyReplay()
            ).with_outputs(
                "new",
                "replay",
                "conflict",
            )
        )

        assert_that(
            result.new,
            check_new,
            label="CheckNewEvent",
        )


def test_replay_same_hash():
    record = {
        "event_id": "event-002",
        "payload_hash": "hash-a",
    }

    registry_record = {
        "event_id": "event-002",
        "payload_hash": "hash-a",
        "first_seen_at": "2026-09-04T12:00:00Z",
    }

    with TestPipeline() as p:
        result = (
            p
            | "CreateReplayEvent"
            >> beam.Create([
                (record, registry_record)
            ])
            | "ClassifyReplayEvent"
            >> beam.ParDo(
                ClassifyReplay()
            ).with_outputs(
                "new",
                "replay",
                "conflict",
            )
        )

        assert_that(
            result.replay,
            check_replay,
            label="CheckReplay",
        )


def test_conflicting_payload():
    record = {
        "event_id": "event-003",
        "payload_hash": "hash-b",
    }

    registry_record = {
        "event_id": "event-003",
        "payload_hash": "hash-a",
        "first_seen_at": "2026-09-04T12:00:00Z",
    }

    with TestPipeline() as p:
        result = (
            p
            | "CreateConflictEvent"
            >> beam.Create([
                (record, registry_record)
            ])
            | "ClassifyConflictEvent"
            >> beam.ParDo(
                ClassifyReplay()
            ).with_outputs(
                "new",
                "replay",
                "conflict",
            )
        )

        assert_that(
            result.conflict,
            check_conflict,
            label="CheckConflict",
        )