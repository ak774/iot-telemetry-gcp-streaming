import apache_beam as beam

from dataflow.transforms.replay_repository import (
    ReplayRegistryRepository,
)


class DurableReplayDecision(beam.DoFn):

    spanner_new_counter = beam.metrics.Metrics.counter( "spanner", "spanner_new", )
    spanner_replay_counter = beam.metrics.Metrics.counter( "spanner", "spanner_replay", )
    spanner_conflict_counter = beam.metrics.Metrics.counter( "spanner", "spanner_conflict", )

    def __init__(
        self,
        project_id,
        instance_id,
        database_id,
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id

        self.repository = None

    def setup(self):

        self.repository = ReplayRegistryRepository(
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
        )

        self.repository.connect()

    def process(self, record):

        result = self.repository.register_event(
            record
        )

        status = result["status"]

        if status == "new":

            self.spanner_new_counter.inc()

            yield beam.pvalue.TaggedOutput(
                "new",
                record,
            )

            return

        registry_record = result["registry_record"]

        if status == "replay":

            self.spanner_replay_counter.inc()

            replay_record = dict(record)

            replay_record["replay_reason"] = (
                "event_id_and_payload_hash_already_seen"
            )

            replay_record["original_first_seen_at"] = (
                registry_record["first_seen_at"]
            )

            yield beam.pvalue.TaggedOutput(
                "replay",
                replay_record,
            )

            return

        if status == "conflict":

            self.spanner_conflict_counter.inc()

            conflict_record = dict(record)

            conflict_record["conflict_reason"] = (
                "same_event_id_different_payload_hash"
            )

            conflict_record["original_payload_hash"] = (
                registry_record["payload_hash"]
            )

            conflict_record["conflicting_payload_hash"] = (
                record["payload_hash"]
            )

            yield beam.pvalue.TaggedOutput(
                "conflict",
                conflict_record,
            )

            return

        raise ValueError(
            f"Unknown replay status: {status}"
        )
