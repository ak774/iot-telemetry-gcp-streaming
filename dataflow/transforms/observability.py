import apache_beam as beam
from datetime import datetime, timezone


class MeasureProcessingLatency(beam.DoFn):

    processing_latency = beam.metrics.Metrics.distribution(
        "telemetry",
        "telemetry_processing_latency_ms",
    )

    def process(self, record):

        event_timestamp = record["event_timestamp"]

        timestamp_string = event_timestamp.replace("Z", "+00:00")

        event_time = datetime.fromisoformat(timestamp_string)

        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        processing_time = datetime.now(timezone.utc)

        latency_ms = (
            processing_time - event_time
        ).total_seconds() * 1000

        self.processing_latency.update(
            int(latency_ms)
        )

        yield record