from datetime import datetime, timezone

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.gold import (
    AggregateTelemetry,
    PrepareGoldRecord,
)


def test_gold_windowed_aggregation():

    records = [
        {
            "device_id": "device-001",
            "temperature": 25.0,
            "humidity": 60.0,
            "pressure": 1010.0,
            "battery_level": 95.0,
        },
        {
            "device_id": "device-001",
            "temperature": 27.0,
            "humidity": 70.0,
            "pressure": 1014.0,
            "battery_level": 93.0,
        },
        {
            "device_id": "device-002",
            "temperature": 30.0,
            "humidity": 50.0,
            "pressure": 1005.0,
            "battery_level": 90.0,
        },
    ]

    timestamps = [
        datetime(
            2026, 8, 30, 12, 0, 10,
            tzinfo=timezone.utc
        ).timestamp(),

        datetime(
            2026, 8, 30, 12, 0, 40,
            tzinfo=timezone.utc
        ).timestamp(),

        datetime(
            2026, 8, 30, 12, 0, 20,
            tzinfo=timezone.utc
        ).timestamp(),
    ]

    with TestPipeline() as pipeline:

        output = (
            pipeline
            | beam.Create([
                beam.window.TimestampedValue(
                    record,
                    timestamp
                )
                for record, timestamp
                in zip(records, timestamps)
            ])
            | beam.WindowInto(
                beam.window.FixedWindows(60)
            )
            | beam.Map(
                lambda record: (
                    record["device_id"],
                    record
                )
            )
            | beam.CombinePerKey(
                AggregateTelemetry()
            )
            | beam.ParDo(
                PrepareGoldRecord()
            )
        )

        expected = [
            {
                "device_id": "device-001",
                "window_start": datetime(
                    2026, 8, 30, 12, 0,
                    tzinfo=timezone.utc
                ),
                "window_end": datetime(
                    2026, 8, 30, 12, 1,
                    tzinfo=timezone.utc
                ),
                "event_count": 2,
                "avg_temperature": 26.0,
                "min_temperature": 25.0,
                "max_temperature": 27.0,
                "avg_humidity": 65.0,
                "avg_pressure": 1012.0,
                "avg_battery_level": 94.0,
            },
            {
                "device_id": "device-002",
                "window_start": datetime(
                    2026, 8, 30, 12, 0,
                    tzinfo=timezone.utc
                ),
                "window_end": datetime(
                    2026, 8, 30, 12, 1,
                    tzinfo=timezone.utc
                ),
                "event_count": 1,
                "avg_temperature": 30.0,
                "min_temperature": 30.0,
                "max_temperature": 30.0,
                "avg_humidity": 50.0,
                "avg_pressure": 1005.0,
                "avg_battery_level": 90.0,
            },
        ]

        assert_that(
            output,
            equal_to(expected)
        )
