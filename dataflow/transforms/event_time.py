from datetime import datetime, timezone

import apache_beam as beam


class AssignEventTimestamp(beam.DoFn):

    def process(self, record):

        event_timestamp = record["event_timestamp"]

        timestamp_string = (
            event_timestamp
            .replace("Z", "+00:00")
        )

        event_time = datetime.fromisoformat(
            timestamp_string
        )

        if event_time.tzinfo is None:
            event_time = event_time.replace(
                tzinfo=timezone.utc
            )

        yield beam.window.TimestampedValue(
            record,
            event_time.timestamp()
        )