from datetime import datetime, timezone

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.silver import PrepareSilverRecord


def test_prepare_silver_record():

    record = {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-30T12:00:10+00:00",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }

    expected = {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-30T12:00:10+00:00",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
        "window_start": datetime(
            2026,
            8,
            30,
            12,
            0,
            0,
            tzinfo=timezone.utc
        ),
        "window_end": datetime(
            2026,
            8,
            30,
            12,
            1,
            0,
            tzinfo=timezone.utc
        )
    }

    with TestPipeline() as pipeline:

        output = (
            pipeline
            | beam.Create([
                beam.window.TimestampedValue(
                    record,
                    datetime(
                        2026,
                        8,
                        30,
                        12,
                        0,
                        10,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ])
            | beam.WindowInto(
                beam.window.FixedWindows(60)
            )
            | beam.ParDo(
                PrepareSilverRecord()
            )
        )

        assert_that(
            output,
            equal_to([expected])
        )