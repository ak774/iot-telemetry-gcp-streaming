import json
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions
)

from dataflow.transforms.parser import ParseTelemetry
from dataflow.transforms.validator import ValidateTelemetry
from dataflow.transforms.event_time import AssignEventTimestamp
from dataflow.transforms.windowing import ApplyTelemetryWindow
from dataflow.transforms.silver import (
    PrepareSilverRecord,
    PrepareGoldInputDebugRecord
)
from dataflow.transforms.gold import (
    AggregateTelemetry,
    PrepareGoldRecord
)

from config.pipeline_config import (
    PUBSUB_SUBSCRIPTION,
    DLQ_TOPIC,
    RAW_TABLE,
    SILVER_TABLE,
    GOLD_TABLE,
    CONFLICT_TABLE,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
)

from dataflow.transforms.deduplication import DeduplicateTelemetry
from dataflow.transforms.fingerprint import (
    AddPayloadFingerprint
)
from dataflow.transforms.conflict_detection import (
    DetectTelemetryConflicts
)

from dataflow.transforms.replay_registry import (
    PrepareReplayRegistryRecord
)

from dataflow.transforms.durable_replay import (
    DurableReplayDecision
)

from dataflow.transforms.replay_audit import (
    PrepareReplayAuditRecord
)

from dataflow.transforms.replay_conflict_audit import (
    PrepareReplayConflictRecord
)

from datetime import datetime, timezone

from dataflow.transforms.observability import MeasureProcessingLatency

PROJECT_ID = os.environ["GCP_PROJECT_ID"]


class PrepareConflictRecord(beam.DoFn):

    def process(self, record):

        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "temperature": record.get("temperature"),
            "humidity": record.get("humidity"),
            "pressure": record.get("pressure"),
            "battery_level": record.get("battery_level"),
            "payload_hash": record.get("payload_hash"),
            "original_payload_hash": record.get(
                "original_payload_hash"
            ),
            "conflicting_payload_hash": record.get(
                "conflicting_payload_hash"
            ),
            "conflict_reason": record.get(
                "conflict_reason"
            ),
            "conflict_detected_at": datetime.now(
                timezone.utc
            )
        }

# ---------------------------------------------------------
# DLQ formatting
# ---------------------------------------------------------

class FormatDLQ(beam.DoFn):

    def process(self, record):

        yield json.dumps(
            record
        ).encode("utf-8")


# ---------------------------------------------------------
# Pipeline
# ---------------------------------------------------------

def run():

    pipeline_options = PipelineOptions()

    pipeline_options.view_as(
        StandardOptions
    ).streaming = True

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        # ---------------------------------------------
        # Read from Pub/Sub
        # ---------------------------------------------

        messages = (
            pipeline
            | "ReadFromPubSub"
            >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            )
        )


        # ---------------------------------------------
        # Parse JSON
        # ---------------------------------------------

        parsed = (
            messages
            | "ParseJSON"
            >> beam.ParDo(
                ParseTelemetry()
            ).with_outputs(
                ParseTelemetry.VALID,
                ParseTelemetry.INVALID
            )
        )


        # ---------------------------------------------
        # Parser-invalid -> DLQ
        # ---------------------------------------------

        parser_dlq = (
            parsed[ParseTelemetry.INVALID]
            | "FormatParserDLQ"
            >> beam.ParDo(
                FormatDLQ()
            )
        )


        # ---------------------------------------------
        # Validate telemetry
        # ---------------------------------------------

        validated = (
            parsed[ParseTelemetry.VALID]
            | "ValidateTelemetry"
            >> beam.ParDo(
                ValidateTelemetry()
            ).with_outputs(
                ValidateTelemetry.VALID,
                ValidateTelemetry.INVALID
            )
        )


        # ---------------------------------------------
        # Valid -> Raw BigQuery
        # ---------------------------------------------

        (
            validated[ValidateTelemetry.VALID]
            | "WriteValidToBigQuery"
            >> beam.io.WriteToBigQuery(
                RAW_TABLE,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )


        # ---------------------------------------------
        # DEBUG: Event-time window only
        # ---------------------------------------------

        (
            validated[ValidateTelemetry.VALID]

            | "DebugAssignTimestampForWindow"
            >> beam.ParDo(
                AssignEventTimestamp()
            )

            | "DebugApplyEventTimeWindow"
            >> ApplyTelemetryWindow()

            | "DebugEventTimeWindowRecord"
            >> beam.Map(
                lambda record: {
                    "event_id": record["event_id"],
                    "device_id": record["device_id"],
                    "event_timestamp": record["event_timestamp"]
                }
            )

            | "WriteEventTimeWindowDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_event_time_window_debug",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        # ---------------------------------------------
        # DEBUG: Processing-time window + Count.PerKey
        # ---------------------------------------------

        (
            validated[ValidateTelemetry.VALID]

            | "DebugProcessingTimeWindowDirect"
            >> beam.WindowInto(
                beam.window.FixedWindows(60),
                trigger=beam.trigger.AfterProcessingTime(10),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
            )

            | "DebugPTCountKeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    1
                )
            )

            | "DebugPTCountPerDevice"
            >> beam.CombinePerKey(sum)

            | "DebugPTPrepareCountRecord"
            >> beam.Map(
                lambda element: {
                    "device_id": element[0],
                    "record_count": element[1]
                }
            )

            | "WritePTCountPerKeyDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_pt_count_per_key_debug",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        # ---------------------------------------------
        # DEBUG: Event timestamp assignment only
        # ---------------------------------------------

        (
            validated[ValidateTelemetry.VALID]

            | "DebugAssignEventTimestamp"
            >> beam.ParDo(
                AssignEventTimestamp()
            )

            | "DebugEventTimestampRecord"
            >> beam.Map(
                lambda record: {
                    "event_id": record["event_id"],
                    "device_id": record["device_id"],
                    "event_timestamp": record["event_timestamp"]
                }
            )

            | "WriteEventTimestampDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_event_timestamp_debug",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        # ---------------------------------------------
        # DEBUG: Event-time window + Count.PerKey
        # FORCE FIRE AFTER 3 ELEMENTS
        # ---------------------------------------------

        (
            validated[ValidateTelemetry.VALID]
            | "DebugForcedAssignTimestamp"
            >> beam.ParDo(
                AssignEventTimestamp()
            )
            | "DebugForcedEventTimeWindow"
            >> beam.WindowInto(
                beam.window.FixedWindows(60)
            )
            | "DebugForcedCountKeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    1
                )
            )
            | "DebugForcedCountPerDevice"
            >> beam.CombinePerKey(sum)
            | "DebugForcedPrepareCount"
            >> beam.Map(
                lambda element: {
                    "device_id": element[0],
                    "record_count": element[1]
                }
            )
            | "WriteForcedEventTimeCountDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_event_time_count_debug",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        (
            validated[ValidateTelemetry.VALID]
            | "DiagAssignEventTimestamp"
            >> beam.ParDo(AssignEventTimestamp())
            | "DiagEventTimeWindow"
            >> beam.WindowInto(
                beam.window.FixedWindows(60)
            )
            | "DiagKeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    1
                )
            )
            | "DiagGroupByDevice"
            >> beam.GroupByKey()
            | "DiagPrepareGroupCount"
            >> beam.Map(
                lambda element: {
                    "device_id": element[0],
                    "record_count": len(list(element[1]))
                }
            )
            | "WriteEventTimeGroupCountDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_event_time_group_count_debug",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        # ---------------------------------------------
        # Event time + windowing
        # ---------------------------------------------


        fingerprinted_validated = (
            validated[ValidateTelemetry.VALID]
            | "MeasureProcessingLatency"
            >> beam.ParDo(MeasureProcessingLatency())
            | "AddPayloadFingerprint"
            >> beam.ParDo(AddPayloadFingerprint())
        )


        conflict_checked = (
            fingerprinted_validated

            | "DetectTelemetryConflicts"
            >> DetectTelemetryConflicts()
        )

        # ---------------------------------------------
        # Conflicting event_id -> conflict audit table
        # ---------------------------------------------

        conflict_records = (
            conflict_checked.conflict
            | "PrepareConflictRecord"
            >> beam.ParDo(
                PrepareConflictRecord()
            )
        )

        (
            conflict_records
            | "WriteConflictsToBigQuery"
            >> beam.io.WriteToBigQuery(
                CONFLICT_TABLE,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )


        deduplicated_validated = (
            conflict_checked.clean

            | "AssignEventTimestamp"
            >> beam.ParDo(
                AssignEventTimestamp()
            )

            | "DeduplicateTelemetry"
            >> DeduplicateTelemetry()
        )

        durable_replay_checked = (
            deduplicated_validated
            | "DurableReplayDecision"
            >> beam.ParDo(
                DurableReplayDecision(
                    project_id=PROJECT_ID,
                    instance_id=SPANNER_INSTANCE_ID,
                    database_id=SPANNER_DATABASE_ID,
                )
            ).with_outputs(
                "replay",
                "conflict",
                "new",
            )
        )

        # ---------------------------------------------------------
        # DURABLE REPLAY -> REPLAY AUDIT TABLE
        # ---------------------------------------------------------

        replay_audit_records = (
            durable_replay_checked.replay

            | "PrepareReplayAuditRecord"
            >> beam.ParDo(
                PrepareReplayAuditRecord()
            )
        )

        (
            replay_audit_records

            | "WriteReplayAuditToBigQuery"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_silver.telemetry_replays",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        replay_conflict_records = (
            durable_replay_checked.conflict
            | "PrepareReplayConflictRecord"
            >> beam.ParDo(PrepareReplayConflictRecord())
        )

        (
            replay_conflict_records
            | "WriteReplayConflictsToBigQuery"
            >> beam.io.WriteToBigQuery(
                CONFLICT_TABLE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        windowed_validated = (
            durable_replay_checked.new
            | "ApplyTelemetryWindow"
            >> ApplyTelemetryWindow()
        )


        # =================================================
        # SILVER LAYER
        # =================================================

        silver_records = (
            windowed_validated

            | "PrepareSilverRecord"
            >> beam.ParDo(
                PrepareSilverRecord()
            )
        )




        # ---------------------------------------------------------
        # DURABLE REPLAY REGISTRY
        # ---------------------------------------------------------

        replay_registry_records = (
            windowed_validated

            | "PrepareReplayRegistryRecord"
            >> beam.ParDo(
                PrepareReplayRegistryRecord()
            )
        )

        (
            replay_registry_records
            | "WriteReplayRegistryToBigQuery"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_silver.telemetry_event_registry",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        (
            silver_records
            | "WriteSilverToBigQuery"
            >> beam.io.WriteToBigQuery(
                SILVER_TABLE,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        # ---------------------------------------------------------
        # TEMPORARY GOLD INPUT DEBUG
        # ---------------------------------------------------------

        (
            windowed_validated
            | "PrepareGoldInputDebugRecord"
            >> beam.ParDo(
                PrepareGoldInputDebugRecord()
            )
            | "WriteGoldInputDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_input_debug",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        (
            windowed_validated
            | "DebugKeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    record
                )
            )
            | "DebugGroupByDevice"
            >> beam.GroupByKey()
            | "DebugPrepareGroupedRecord"
            >> beam.Map(
                lambda element: {
                    "device_id": element[0],
                    "record_count": len(list(element[1]))
                }
            )
            | "WriteGroupedDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_group_debug",
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )

        processing_time_group_debug = (
            windowed_validated
            | "DebugProcessingTimeWindow"
            >> beam.WindowInto(
                beam.window.FixedWindows(60),
                trigger=beam.trigger.AfterProcessingTime(10),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
            )
            | "DebugPTKeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    record
                )
            )
            | "DebugPTGroupByDevice"
            >> beam.GroupByKey()
            | "DebugPTPrepareGroupedRecord"
            >> beam.Map(
                lambda element: {
                    "device_id": element[0],
                    "record_count": len(list(element[1]))
                }
            )
            | "WriteProcessingTimeGroupedDebug"
            >> beam.io.WriteToBigQuery(
                "iot-gcp-streaming:iot_gold.gold_processing_time_debug",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        # =================================================
        # GOLD LAYER
        # =================================================

        gold_aggregated = (
            windowed_validated

            | "KeyByDevice"
            >> beam.Map(
                lambda record: (
                    record["device_id"],
                    record
                )
            )

            | "AggregateTelemetry"
            >> beam.CombinePerKey(
                AggregateTelemetry()
            )

            | "PrepareGoldRecord"
            >> beam.ParDo(
                PrepareGoldRecord()
            )
        )


        (
            gold_aggregated
            | "WriteGoldToBigQuery"
            >> beam.io.WriteToBigQuery(
                GOLD_TABLE,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )


        # ---------------------------------------------
        # Validator-invalid -> DLQ
        # ---------------------------------------------

        validator_dlq = (
            validated[ValidateTelemetry.INVALID]
            | "FormatValidatorDLQ"
            >> beam.ParDo(
                FormatDLQ()
            )
        )


        # ---------------------------------------------
        # Merge DLQ streams
        # ---------------------------------------------

        (
            [
                parser_dlq,
                validator_dlq
            ]
            | "FlattenDLQ"
            >> beam.Flatten()
            | "WriteToDLQ"
            >> beam.io.WriteToPubSub(
                DLQ_TOPIC
            )
        )


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    run()
