
import json

from apache_beam.pvalue import TaggedOutput

from dataflow.transforms.parser import ParseTelemetry
from dataflow.transforms.validator import ValidateTelemetry


def parse_message(message):

    parser = ParseTelemetry()

    outputs = list(
        parser.process(message)
    )

    return outputs


def validate_record(record):

    validator = ValidateTelemetry()

    outputs = list(
        validator.process(record)
    )

    return outputs


def valid_record():

    return {
        "event_id": "event-integration-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-28T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }


def test_valid_json_and_valid_telemetry():

    message = json.dumps(
        valid_record()
    ).encode("utf-8")

    parser_outputs = parse_message(message)

    assert len(parser_outputs) == 1

    parser_output = parser_outputs[0]

    assert isinstance(
        parser_output,
        TaggedOutput
    )

    assert parser_output.tag == "valid"

    validator_outputs = validate_record(
        parser_output.value
    )

    assert len(validator_outputs) == 1

    validator_output = validator_outputs[0]

    assert isinstance(
        validator_output,
        TaggedOutput
    )

    assert validator_output.tag == "valid"

    assert (
        validator_output.value["event_id"]
        == "event-integration-001"
    )


def test_valid_json_but_invalid_telemetry():

    record = valid_record()

    record["temperature"] = 999

    message = json.dumps(
        record
    ).encode("utf-8")

    parser_outputs = parse_message(message)

    assert len(parser_outputs) == 1

    parser_output = parser_outputs[0]

    assert parser_output.tag == "valid"

    validator_outputs = validate_record(
        parser_output.value
    )

    assert len(validator_outputs) == 1

    validator_output = validator_outputs[0]

    assert isinstance(
        validator_output,
        TaggedOutput
    )

    assert validator_output.tag == "invalid"

    assert (
        validator_output.value["error_stage"]
        == "validation"
    )

    assert any(
        "temperature above maximum" in error
        for error in validator_output.value["errors"]
    )


def test_invalid_json_does_not_reach_validator():

    message = (
        b'{"event_id": "event-invalid-json", '
        b'"temperature": }'
    )

    parser_outputs = parse_message(message)

    assert len(parser_outputs) == 1

    parser_output = parser_outputs[0]

    assert isinstance(
        parser_output,
        TaggedOutput
    )

    assert parser_output.tag == "invalid"

    assert (
        parser_output.value["error_stage"]
        == "json_parsing"
    )

    # The invalid parser branch is not passed
    # to ValidateTelemetry.
    assert "record" not in parser_output.value
