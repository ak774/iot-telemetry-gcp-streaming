from datetime import datetime, timezone

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.event_time import AssignEventTimestamp
from dataflow.transforms.windowing import ApplyTelemetryWindow
from dataflow.transforms.gold import (
    AggregateTelemetry,
    PrepareGoldRecord
)


def test_device_window_gold_aggregation():

    records = [
        {
            "event_id": "event-001",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:00:10+00:00",
            "temperature": 20.0,
            "humidity": 50.0,
            "pressure": 1000.0,
            "battery_level": 90.0
        },
        {
            "event_id": "event-002",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:00:30+00:00",
            "temperature": 30.0,
            "humidity": 60.0,
            "pressure": 1020.0,
            "battery_level": 80.0
        },
        {
            "event_id": "event-003",
            "device_id": "device-001",
            "event_timestamp": "2026-08-30T12:01:10+00:00",
            "temperature": 40.0,
            "humidity": 70.0,
            "pressure": 1040.0,
            "battery_level": 70.0
        },
    ]

    expected = [
        {
            "device_id": "device-001",
            "window_start": datetime(
                2026, 8, 30, 12, 0, 0,
                tzinfo=timezone.utc
            ),
            "window_end": datetime(
                2026, 8, 30, 12, 1, 0,
                tzinfo=timezone.utc
            ),
            "event_count": 2,
            "avg_temperature": 25.0,
            "min_temperature": 20.0,
            "max_temperature": 30.0,
            "avg_humidity": 55.0,
            "avg_pressure": 1010.0,
            "avg_battery_level": 85.0
        },
        {
            "device_id": "device-001",
            "window_start": datetime(
                2026, 8, 30, 12, 1, 0,
                tzinfo=timezone.utc
            ),
            "window_end": datetime(
                2026, 8, 30, 12, 2, 0,
                tzinfo=timezone.utc
            ),
            "event_count": 1,
            "avg_temperature": 40.0,
            "min_temperature": 40.0,
            "max_temperature": 40.0,
            "avg_humidity": 70.0,
            "avg_pressure": 1040.0,
            "avg_battery_level": 70.0
        }
    ]

    with TestPipeline(
    options=PipelineOptions(flags=["--allow_unsafe_triggers"])) as pipeline:
        output = (
            pipeline
            | "CreateRecords"
            >> beam.Create(records)

            | "AssignEventTimestamp"
            >> beam.ParDo(
                AssignEventTimestamp()
            )

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

        assert_that(
            output,
            equal_to(expected)
        )