import apache_beam as beam


class ClassifyReplay(beam.DoFn):

    def process(self, element):
        record, registry_record = element

        if registry_record is None:
            yield beam.pvalue.TaggedOutput(
                "new",
                record
            )
            return

        incoming_hash = record["payload_hash"]
        registered_hash = registry_record["payload_hash"]

        if incoming_hash == registered_hash:
            replay_record = dict(record)

            replay_record["replay_reason"] = (
                "event_id_and_payload_hash_already_seen"
            )

            replay_record["original_first_seen_at"] = (
                registry_record["first_seen_at"]
            )

            yield beam.pvalue.TaggedOutput(
                "replay",
                replay_record
            )

            return

        conflict_record = dict(record)

        conflict_record["conflict_reason"] = (
            "same_event_id_different_payload_hash"
        )

        conflict_record["original_payload_hash"] = (
            registered_hash
        )

        conflict_record["conflicting_payload_hash"] = (
            incoming_hash
        )

        yield beam.pvalue.TaggedOutput(
            "conflict",
            conflict_record
        )