from datetime import datetime, timezone

import apache_beam as beam


class PrepareReplayRegistryRecord(beam.DoFn):

    def process(self, record):
        now = datetime.now(timezone.utc)

        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "payload_hash": record["payload_hash"],
            "first_seen_at": now,
            "last_seen_at": now,
        }