import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam import window

from dataflow.transforms.event_time import AssignEventTimestamp


def test_assign_event_timestamp():

    record = {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-30T12:00:05Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }

    with BeamTestPipeline() as pipeline:

        output = (
            pipeline
            | "CreateRecord"
            >> beam.Create([record])
            | "AssignEventTimestamp"
            >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "ExtractTimestamp"
            >> beam.Map(
                lambda x: x
            )
        )

        assert_that(
            output,
            equal_to([record])
        )