import json

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.transforms.parser import ParseTelemetry
from dataflow.transforms.validator import ValidateTelemetry


def test_pipeline_routes_valid_json_to_validation():

    record = {
        "event_id": "pipeline-valid-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-31T12:30:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
    }

    message = json.dumps(record).encode("utf-8")

    with TestPipeline() as pipeline:

        parsed = (
            pipeline
            | "CreateValidMessage"
            >> beam.Create([message])

            | "ParseJSON"
            >> beam.ParDo(
                ParseTelemetry()
            ).with_outputs(
                ParseTelemetry.VALID,
                ParseTelemetry.INVALID
            )
        )

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

        assert_that(
            validated[ValidateTelemetry.VALID],
            equal_to([record]),
            label="AssertValidRecords"
        )

        assert_that(
            validated[ValidateTelemetry.INVALID],
            equal_to([]),
            label="AssertNoValidationDLQ"
        )

        assert_that(
            parsed[ParseTelemetry.INVALID],
            equal_to([]),
            label="AssertNoParserDLQ"
        )


def test_pipeline_routes_invalid_json_to_parser_dlq():

    message = (
        b'{"event_id": "pipeline-invalid-json-001", '
        b'"temperature": }'
    )

    with TestPipeline() as pipeline:

        parsed = (
            pipeline
            | "CreateInvalidMessage"
            >> beam.Create([message])

            | "ParseJSON"
            >> beam.ParDo(
                ParseTelemetry()
            ).with_outputs(
                ParseTelemetry.VALID,
                ParseTelemetry.INVALID
            )
        )

        assert_that(
            parsed[ParseTelemetry.VALID],
            equal_to([]),
            label="AssertNoValidRecords"
        )

        def check_parser_dlq(actual):

            assert len(actual) == 1

            record = actual[0]

            assert record["error_stage"] == "json_parsing"
            assert record["raw_message"] == message.decode("utf-8")
            assert record["error_message"]

        assert_that(
            parsed[ParseTelemetry.INVALID],
            check_parser_dlq,
            label="AssertParserDLQ"
        )


def test_pipeline_routes_invalid_telemetry_to_validator_dlq():

    record = {
        "event_id": "pipeline-invalid-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-31T12:30:00Z",
        "temperature": 999,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
    }

    message = json.dumps(record).encode("utf-8")

    with TestPipeline() as pipeline:

        parsed = (
            pipeline
            | "CreateInvalidTelemetryMessage"
            >> beam.Create([message])

            | "ParseJSON"
            >> beam.ParDo(
                ParseTelemetry()
            ).with_outputs(
                ParseTelemetry.VALID,
                ParseTelemetry.INVALID
            )
        )

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

        assert_that(
            validated[ValidateTelemetry.VALID],
            equal_to([]),
            label="AssertNoValidTelemetry"
        )

        def check_validator_dlq(actual):

            assert len(actual) == 1

            dlq_record = actual[0]

            assert dlq_record["error_stage"] == "validation"
            assert dlq_record["record"] == record

            assert any(
                "temperature above maximum" in error
                for error in dlq_record["errors"]
            )

        assert_that(
            validated[ValidateTelemetry.INVALID],
            check_validator_dlq,
            label="AssertValidatorDLQ"
        )