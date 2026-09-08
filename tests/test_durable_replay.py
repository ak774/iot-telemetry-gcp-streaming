from datetime import datetime, timezone

import apache_beam as beam

from dataflow.transforms.durable_replay import DurableReplayDecision


class FakeRepository:
    def __init__(self, result):
        self.result = result

    def register_event(self, record):
        return self.result


def make_record():
    return {
        "event_id": "test-event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T11:00:00Z",
        "payload_hash": "abc123",
        "temperature": 25.0,
        "humidity": 50.0,
    }


def collect_outputs(dofn, record):
    outputs = list(dofn.process(record))

    new = []
    replay = []
    conflict = []

    for output in outputs:
        if isinstance(output, beam.pvalue.TaggedOutput):
            if output.tag == "new":
                new.append(output.value)
            elif output.tag == "replay":
                replay.append(output.value)
            elif output.tag == "conflict":
                conflict.append(output.value)
        else:
            raise AssertionError(
                f"Unexpected output type: {type(output)}"
            )

    return new, replay, conflict


def test_new_event():
    record = make_record()

    result = {
        "status": "new",
        "record": record,
    }

    dofn = DurableReplayDecision(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )

    dofn.repository = FakeRepository(result)

    new, replay, conflict = collect_outputs(
    dofn,
    record,
    )

    assert len(new) == 1
    assert new[0]["event_id"] == "test-event-001"
    assert len(replay) == 0
    assert len(conflict) == 0


def test_replay_event():
    record = make_record()

    registry_record = {
        "event_id": "test-event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T11:00:00Z",
        "payload_hash": "abc123",
        "first_seen_at": datetime.now(timezone.utc),
        "last_seen_at": datetime.now(timezone.utc),
    }

    result = {
        "status": "replay",
        "registry_record": registry_record,
    }

    dofn = DurableReplayDecision(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )

    dofn.repository = FakeRepository(result)

    new, replay, conflict = collect_outputs(
        dofn,
        record,
    )

    assert len(new) == 0
    assert len(replay) == 1
    assert (
        replay[0]["replay_reason"]
        == "event_id_and_payload_hash_already_seen"
    )
    assert len(conflict) == 0


def test_conflicting_event():
    record = make_record()

    registry_record = {
        "event_id": "test-event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T11:00:00Z",
        "payload_hash": "original-hash",
        "first_seen_at": datetime.now(timezone.utc),
        "last_seen_at": datetime.now(timezone.utc),
    }

    result = {
        "status": "conflict",
        "registry_record": registry_record,
    }

    dofn = DurableReplayDecision(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )

    dofn.repository = FakeRepository(result)

    new, replay, conflict = collect_outputs(
        dofn,
        record,
    )

    assert len(new) == 0
    assert len(replay) == 0
    assert len(conflict) == 1

    assert (
        conflict[0]["conflict_reason"]
        == "same_event_id_different_payload_hash"
    )

    assert (
        conflict[0]["original_payload_hash"]
        == "original-hash"
    )

    assert (
        conflict[0]["conflicting_payload_hash"]
        == "abc123"
    )

def test_spanner_failure_propagates():
    record = make_record()


    class FailingRepository:
        def register_event(self, record):
            raise RuntimeError("simulated Spanner failure")

    dofn = DurableReplayDecision(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )

    dofn.repository = FailingRepository()

    try:
        list(dofn.process(record))
        raise AssertionError(
            "Expected Spanner failure to propagate"
        )
    except RuntimeError as exc:
        assert str(exc) == "simulated Spanner failure"

def test_transient_spanner_failure_then_recovery():
    record = make_record()

    class FlakyRepository:
        def __init__(self):
            self.calls = 0

        def register_event(self, record):
            self.calls += 1

            if self.calls == 1:
                raise RuntimeError(
                    "simulated transient Spanner failure"
                )

            return {
                "status": "new",
                "record": record,
            }

    repository = FlakyRepository()

    dofn = DurableReplayDecision(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )

    dofn.repository = repository

    # First processing attempt fails.
    try:
        list(dofn.process(record))
        raise AssertionError(
            "Expected transient Spanner failure"
        )
    except RuntimeError as exc:
        assert str(exc) == "simulated transient Spanner failure"

    # Second processing attempt succeeds.
    new, replay, conflict = collect_outputs(
        dofn,
        record,
    )

    assert len(new) == 1
    assert new[0]["event_id"] == "test-event-001"
    assert len(replay) == 0
    assert len(conflict) == 0

    assert repository.calls == 2
