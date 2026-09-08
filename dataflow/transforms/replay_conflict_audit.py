import apache_beam as beam


class PrepareReplayConflictRecord(beam.DoFn):
    def process(self, record):
        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "conflict_reason": record["conflict_reason"],
            "original_payload_hash": record["original_payload_hash"],
            "conflicting_payload_hash": record["conflicting_payload_hash"],
        }