import apache_beam as beam

from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.utils.timestamp import Duration

from config.pipeline_config import (
    DEDUPLICATION_DURATION_SECONDS
)


class DeduplicateTelemetry(beam.PTransform):

    def expand(self, pcoll):

        return (
            pcoll
            | "KeyByEventIdForDeduplication"
            >> beam.Map(
                lambda record: (
                    record["event_id"],
                    record
                )
            )

            | "DeduplicateByEventId"
            >> DeduplicatePerKey(
                event_time_duration=Duration(
                    seconds=DEDUPLICATION_DURATION_SECONDS
                )
            )

            | "RemoveDeduplicationKey"
            >> beam.Values()
        )