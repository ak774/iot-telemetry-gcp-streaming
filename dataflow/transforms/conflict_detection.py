import time

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Duration




CONFLICT_STATE_DURATION_SECONDS = 600


class DetectEventConflicts(beam.DoFn):
    """
    Detects conflicting payloads for the same event_id.

    Input:
        (event_id, record)

    Behavior:
        - First occurrence of event_id:
            store payload_hash and emit record.
        - Same event_id + same payload_hash:
            emit record as a normal duplicate.
        - Same event_id + different payload_hash:
            emit to conflict side output.
        - State expires after the configured processing-time duration.
    """

    SEEN_HASH = ReadModifyWriteStateSpec(
        "seen_hash",
        StrUtf8Coder(),
    )

    CLEAR_TIMER = TimerSpec(
        "clear_timer",
        TimeDomain.REAL_TIME,
    )

    payload_conflict_counter = beam.metrics.Metrics.counter(
    "telemetry",
    "telemetry_payload_conflict",
    )

    def process(
        self,
        element,
        seen_hash=beam.DoFn.StateParam(SEEN_HASH),
        clear_timer=beam.DoFn.TimerParam(CLEAR_TIMER),
    ):
        event_id, record = element

        current_hash = record["payload_hash"]
        previous_hash = seen_hash.read()

        # First occurrence of this event_id.
        if previous_hash is None:
            seen_hash.write(current_hash)

            clear_timer.set(
                time.time() + CONFLICT_STATE_DURATION_SECONDS
            )

            yield record
            return

        # Same event_id + same payload = legitimate duplicate.
        if previous_hash == current_hash:
            yield record
            return

        self.payload_conflict_counter.inc()
        # Same event_id + different payload = conflict.
        conflict_record = dict(record)

        conflict_record["conflict_reason"] = (
            "same_event_id_different_payload_hash"
        )
        conflict_record["original_payload_hash"] = previous_hash
        conflict_record["conflicting_payload_hash"] = current_hash

        yield beam.pvalue.TaggedOutput(
            "conflict",
            conflict_record,
        )

    @on_timer(CLEAR_TIMER)
    def clear_seen_hash(
        self,
        seen_hash=beam.DoFn.StateParam(SEEN_HASH),
    ):
        seen_hash.clear()


class DetectTelemetryConflicts(beam.PTransform):

    def expand(self, pcoll):

        keyed = (
            pcoll
            | "KeyByEventIdForConflictDetection"
            >> beam.Map(
                lambda record: (
                    record["event_id"],
                    record,
                )
            )
        )

        outputs = (
            keyed
            | "DetectEventConflicts"
            >> beam.ParDo(
                DetectEventConflicts()
            ).with_outputs(
                "conflict",
                main="clean",
            )
        )

        return outputs
