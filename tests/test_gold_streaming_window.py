from datetime import datetime, timezone

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.gold import (
    AggregateTelemetry,
    PrepareGoldRecord,
)
from dataflow.transforms.windowing import ApplyTelemetryWindow


def test_gold_window_emits_after_event_time_window():

    record = {
        "event_id": "phase5-gold-test",
        "device_id": "device-gold-test",
        "event_timestamp": "2026-08-31T12:35:10Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
    }

    with TestPipeline(
    options=PipelineOptions(
        flags=["--allow_unsafe_triggers"])) as pipeline:

        output = (
            pipeline
            | "CreateTestRecord"
            >> beam.Create([
                beam.window.TimestampedValue(
                    record,
                    datetime(
                        2026,
                        8,
                        31,
                        12,
                        35,
                        10,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ])

            | "ApplyTelemetryWindow"
            >> ApplyTelemetryWindow()

            | "KeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    record
                )
            )

            | "AggregateTelemetry"
            >> beam.CombinePerKey(
                AggregateTelemetry()
            )

            | "PrepareGoldRecord"
            >> beam.ParDo(
                PrepareGoldRecord()
            )
        )

        expected = [
            {
                "device_id": "device-gold-test",
                "window_start": datetime(
                    2026,
                    8,
                    31,
                    12,
                    35,
                    0,
                    tzinfo=timezone.utc
                ),
                "window_end": datetime(
                    2026,
                    8,
                    31,
                    12,
                    36,
                    0,
                    tzinfo=timezone.utc
                ),
                "event_count": 1,
                "avg_temperature": 25.5,
                "min_temperature": 25.5,
                "max_temperature": 25.5,
                "avg_humidity": 60.0,
                "avg_pressure": 1012.5,
                "avg_battery_level": 95.0,
            }
        ]

        assert_that(
            output,
            equal_to(expected)
        )