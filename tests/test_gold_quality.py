
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.gold import AggregateTelemetry


def test_gold_aggregation_multiple_events():
    """Multiple telemetry events for one device/window are aggregated correctly."""

    records = [
        {
            "device_id": "device-001",
            "temperature": 20.0,
            "humidity": 50.0,
            "pressure": 1000.0,
            "battery_level": 90.0,
        },
        {
            "device_id": "device-001",
            "temperature": 30.0,
            "humidity": 70.0,
            "pressure": 1020.0,
            "battery_level": 80.0,
        },
    ]

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecords" >> beam.Create(records)
            | "KeyByDevice" >> beam.Map(
                lambda record: (record["device_id"], record)
            )
            | "Aggregate" >> beam.CombinePerKey(
                AggregateTelemetry()
            )
        )

        expected = [
            (
                "device-001",
                {
                    "event_count": 2,
                    "avg_temperature": 25.0,
                    "min_temperature": 20.0,
                    "max_temperature": 30.0,
                    "avg_humidity": 60.0,
                    "avg_pressure": 1010.0,
                    "avg_battery_level": 85.0,
                },
            )
        ]

        assert_that(
            result,
            equal_to(expected),
        )


def test_gold_aggregation_handles_null_metrics():
    """Null sensor values must not affect averages or event count."""

    records = [
        {
            "device_id": "device-002",
            "temperature": 25.0,
            "humidity": None,
            "pressure": 1000.0,
            "battery_level": None,
        },
        {
            "device_id": "device-002",
            "temperature": None,
            "humidity": 60.0,
            "pressure": None,
            "battery_level": 90.0,
        },
    ]

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecords" >> beam.Create(records)
            | "KeyByDevice" >> beam.Map(
                lambda record: (record["device_id"], record)
            )
            | "Aggregate" >> beam.CombinePerKey(
                AggregateTelemetry()
            )
        )

        expected = [
            (
                "device-002",
                {
                    "event_count": 2,
                    "avg_temperature": 25.0,
                    "min_temperature": 25.0,
                    "max_temperature": 25.0,
                    "avg_humidity": 60.0,
                    "avg_pressure": 1000.0,
                    "avg_battery_level": 90.0,
                },
            )
        ]

        assert_that(
            result,
            equal_to(expected),
        )


def test_gold_aggregation_all_metrics_null():
    """Gold aggregation should safely return null metric values when
    a metric has no valid observations.
    """

    records = [
        {
            "device_id": "device-003",
            "temperature": None,
            "humidity": None,
            "pressure": None,
            "battery_level": None,
        }
    ]

    with TestPipeline() as pipeline:

        result = (
            pipeline
            | "CreateRecords" >> beam.Create(records)
            | "KeyByDevice" >> beam.Map(
                lambda record: (record["device_id"], record)
            )
            | "Aggregate" >> beam.CombinePerKey(
                AggregateTelemetry()
            )
        )

        expected = [
            (
                "device-003",
                {
                    "event_count": 1,
                    "avg_temperature": None,
                    "min_temperature": None,
                    "max_temperature": None,
                    "avg_humidity": None,
                    "avg_pressure": None,
                    "avg_battery_level": None,
                },
            )
        ]

        assert_that(
            result,
            equal_to(expected),
        )


def test_gold_aggregation_separate_devices():
    """Events from different devices must never be combined."""

    records = [
        {
            "device_id": "device-A",
            "temperature": 20.0,
            "humidity": 40.0,
            "pressure": 1000.0,
            "battery_level": 90.0,
        },
        {
            "device_id": "device-B",
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
            | "KeyByDevice" >> beam.Map(
                lambda record: (record["device_id"], record)
            )
            | "Aggregate" >> beam.CombinePerKey(
                AggregateTelemetry()
            )
        )

        expected = [
            (
                "device-A",
                {
                    "event_count": 1,
                    "avg_temperature": 20.0,
                    "min_temperature": 20.0,
                    "max_temperature": 20.0,
                    "avg_humidity": 40.0,
                    "avg_pressure": 1000.0,
                    "avg_battery_level": 90.0,
                },
            ),
            (
                "device-B",
                {
                    "event_count": 1,
                    "avg_temperature": 30.0,
                    "min_temperature": 30.0,
                    "max_temperature": 30.0,
                    "avg_humidity": 60.0,
                    "avg_pressure": 1020.0,
                    "avg_battery_level": 80.0,
                },
            ),
        ]

        assert_that(
            result,
            equal_to(expected),
        )
