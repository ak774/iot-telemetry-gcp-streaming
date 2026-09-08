import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline

from dataflow.transforms.observability import (
    MeasureProcessingLatency,
)


def test_processing_latency_metric():

    record = {
        "event_id": "obs-test-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-05T00:00:00Z",
    }

    pipeline = TestPipeline()

    (
        pipeline
        | "CreateTestRecord"
        >> beam.Create([record])
        | "MeasureProcessingLatency"
        >> beam.ParDo(MeasureProcessingLatency())
    )

    result = pipeline.run()
    result.wait_until_finish()

    metrics = result.metrics().query(
        MetricsFilter().with_name(
            "telemetry_processing_latency_ms"
        )
    )

    distributions = metrics["distributions"]

    assert len(distributions) == 1

    distribution = distributions[0]

    assert distribution.committed.count == 1
    assert distribution.committed.min > 0
    assert distribution.committed.max > 0