from datetime import datetime, timezone

import apache_beam as beam


class PrepareReplayAuditRecord(beam.DoFn):

    def process(self, record):

        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "payload_hash": record.get("payload_hash"),
            "replay_reason": record.get(
                "replay_reason"
            ),
            "original_first_seen_at": record.get(
                "original_first_seen_at"
            ),
            "replay_detected_at": datetime.now(
                timezone.utc
            ),
        }