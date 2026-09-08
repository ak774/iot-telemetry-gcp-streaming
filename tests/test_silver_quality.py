import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import FixedWindows

from dataflow.transforms.event_time import AssignEventTimestamp
from dataflow.transforms.silver import PrepareSilverRecord

def test_silver_preserves_all_telemetry_fields():
    """All telemetry fields should be preserved in the Silver record."""


    record = {
        "event_id": "silver-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-31T12:30:15Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
    }

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecord" >> beam.Create([record])
            | "AssignEventTimestamp" >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "ApplyWindow" >> beam.WindowInto(
                FixedWindows(60)
            )
            | "PrepareSilver" >> beam.ParDo(
                PrepareSilverRecord()
            )
        )

        def check_record(actual):
            assert len(actual) == 1

            result = actual[0]

            assert result["event_id"] == "silver-001"
            assert result["device_id"] == "device-001"
            assert result["event_timestamp"] == "2026-08-31T12:30:15Z"
            assert result["temperature"] == 25.5
            assert result["humidity"] == 60.0
            assert result["pressure"] == 1012.5
            assert result["battery_level"] == 95.0

            assert result["window_start"] is not None
            assert result["window_end"] is not None

        assert_that(result, check_record)


def test_silver_preserves_null_sensor_values():
    """NULL sensor values should remain NULL in Silver."""


    record = {
        "event_id": "silver-002",
        "device_id": "device-002",
        "event_timestamp": "2026-08-31T12:31:10Z",
        "temperature": None,
        "humidity": None,
        "pressure": None,
        "battery_level": None,
    }

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecord" >> beam.Create([record])
            | "AssignEventTimestamp" >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "ApplyWindow" >> beam.WindowInto(
                FixedWindows(60)
            )
            | "PrepareSilver" >> beam.ParDo(
                PrepareSilverRecord()
            )
        )

        def check_record(actual):
            assert len(actual) == 1

            result = actual[0]

            assert result["event_id"] == "silver-002"
            assert result["device_id"] == "device-002"

            assert result["temperature"] is None
            assert result["humidity"] is None
            assert result["pressure"] is None
            assert result["battery_level"] is None

            assert result["window_start"] is not None
            assert result["window_end"] is not None

        assert_that(result, check_record)


def test_silver_window_is_one_minute():
    """Silver window_start and window_end should represent a 60-second window."""


    record = {
        "event_id": "silver-003",
        "device_id": "device-003",
        "event_timestamp": "2026-08-31T12:30:15Z",
        "temperature": 20.0,
        "humidity": 50.0,
        "pressure": 1000.0,
        "battery_level": 90.0,
    }

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecord" >> beam.Create([record])
            | "AssignEventTimestamp" >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "ApplyWindow" >> beam.WindowInto(
                FixedWindows(60)
            )
            | "PrepareSilver" >> beam.ParDo(
                PrepareSilverRecord()
            )
        )

        def check_window(actual):
            assert len(actual) == 1

            result = actual[0]

            window_start = result["window_start"]
            window_end = result["window_end"]

            assert window_start is not None
            assert window_end is not None

            assert (
                (window_end - window_start).total_seconds()
                == 60
            )

        assert_that(result, check_window)


def test_silver_multiple_records_remain_independent():
    """Multiple telemetry events should produce independent Silver records."""


    records = [
        {
            "event_id": "silver-004",
            "device_id": "device-A",
            "event_timestamp": "2026-08-31T12:32:10Z",
            "temperature": 20.0,
            "humidity": 40.0,
            "pressure": 1000.0,
            "battery_level": 90.0,
        },
        {
            "event_id": "silver-005",
            "device_id": "device-B",
            "event_timestamp": "2026-08-31T12:32:20Z",
            "temperature": 30.0,
            "humidity": 60.0,
            "pressure": 1020.0,
            "battery_level": 80.0,
        },
    ]

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecords" >> beam.Create(records)
            | "AssignEventTimestamp" >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "ApplyWindow" >> beam.WindowInto(
                FixedWindows(60)
            )
            | "PrepareSilver" >> beam.ParDo(
                PrepareSilverRecord()
            )
        )

        def check_records(actual):
            assert len(actual) == 2

            by_event_id = {
                record["event_id"]: record
                for record in actual
            }

            assert by_event_id["silver-004"]["device_id"] == "device-A"
            assert by_event_id["silver-004"]["temperature"] == 20.0

            assert by_event_id["silver-005"]["device_id"] == "device-B"
            assert by_event_id["silver-005"]["temperature"] == 30.0

            for record in actual:
                assert record["window_start"] is not None
                assert record["window_end"] is not None

        assert_that(result, check_records)
