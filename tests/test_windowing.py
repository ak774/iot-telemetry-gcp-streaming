import apache_beam as beam

from datetime import timezone

from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.event_time import AssignEventTimestamp
from dataflow.transforms.windowing import ApplyTelemetryWindow


def test_events_are_assigned_to_event_time_windows():

    records = [
        {
            "event_id": "event-001",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:00:10Z",
        },
        {
            "event_id": "event-002",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:00:50Z",
        },
        {
            "event_id": "event-003",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:01:05Z",
        },
    ]

    with BeamTestPipeline() as pipeline:

        events = (
            pipeline
            | "CreateRecords"
            >> beam.Create(records)

            | "AssignEventTimestamps"
            >> beam.ParDo(
                AssignEventTimestamp()
            )

            | "ApplyTelemetryWindow"
            >> ApplyTelemetryWindow()

            | "ExtractWindow"
            >> beam.Map(
                lambda element,
                window=beam.DoFn.WindowParam: (
                    element["event_id"],
                    window.start.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
                    window.end.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat()
                )
            )
        )

        assert_that(
            events,
            equal_to([
                (
                    "event-001",
                    "2026-08-30T12:00:00+00:00",
                    "2026-08-30T12:01:00+00:00"
                ),
                (
                    "event-002",
                    "2026-08-30T12:00:00+00:00",
                    "2026-08-30T12:01:00+00:00"
                ),
                (
                    "event-003",
                    "2026-08-30T12:01:00+00:00",
                    "2026-08-30T12:02:00+00:00"
                ),
            ])
        )