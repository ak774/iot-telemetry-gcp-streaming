
import json

from apache_beam.pvalue import TaggedOutput

from dataflow.transforms.parser import ParseTelemetry
from dataflow.transforms.validator import ValidateTelemetry


def valid_record():
    return {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-28T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0,
    }


def run_parser(message):
    parser = ParseTelemetry()
    return list(parser.process(message))


def run_validator(record):
    validator = ValidateTelemetry()
    return list(validator.process(record))


# ---------------------------------------------------------
# Parser DLQ contract
# ---------------------------------------------------------

def test_parser_dlq_contract():

    message = b'{"event_id": "event-001", "temperature": }'

    outputs = run_parser(message)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)
    assert output.tag == "invalid"

    dlq_record = output.value

    # Required DLQ fields
    assert "raw_message" in dlq_record
    assert "error_stage" in dlq_record
    assert "error_message" in dlq_record

    # Contract values
    assert dlq_record["raw_message"] == message.decode("utf-8")
    assert dlq_record["error_stage"] == "json_parsing"

    assert isinstance(
        dlq_record["error_message"],
        str
    )

    assert dlq_record["error_message"]


# ---------------------------------------------------------
# Validator DLQ contract
# ---------------------------------------------------------

def test_validator_dlq_contract():

    record = valid_record()

    record["temperature"] = 999

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)
    assert output.tag == "invalid"

    dlq_record = output.value

    # Required DLQ fields
    assert "record" in dlq_record
    assert "error_stage" in dlq_record
    assert "errors" in dlq_record

    # Contract values
    assert dlq_record["record"] == record
    assert dlq_record["error_stage"] == "validation"

    assert isinstance(
        dlq_record["errors"],
        list
    )

    assert len(dlq_record["errors"]) > 0


# ---------------------------------------------------------
# Validator DLQ must preserve all validation errors
# ---------------------------------------------------------

def test_validator_dlq_preserves_multiple_errors():

    record = valid_record()

    record["temperature"] = 999
    record["humidity"] = -10
    record["battery_level"] = 150

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    dlq_record = output.value

    errors = dlq_record["errors"]

    assert len(errors) >= 3

    assert any(
        "temperature above maximum" in error
        for error in errors
    )

    assert any(
        "humidity below minimum" in error
        for error in errors
    )

    assert any(
        "battery_level above maximum" in error
        for error in errors
    )


# ---------------------------------------------------------
# Parser DLQ raw message must remain recoverable
# ---------------------------------------------------------

def test_parser_dlq_preserves_raw_message():

    original_message = (
        b'{"event_id":"bad-001","device_id":"device-001",'
        b'"temperature":999,}'
    )

    outputs = run_parser(original_message)

    assert len(outputs) == 1

    dlq_record = outputs[0].value

    assert (
        dlq_record["raw_message"]
        == original_message.decode("utf-8")
    )


# ---------------------------------------------------------
# Validator DLQ record must remain structured
# ---------------------------------------------------------

def test_validator_dlq_preserves_original_record():

    record = valid_record()

    record["pressure"] = 1500

    outputs = run_validator(record)

    assert len(outputs) == 1

    dlq_record = outputs[0].value

    preserved_record = dlq_record["record"]

    assert isinstance(preserved_record, dict)

    assert preserved_record["event_id"] == "event-001"
    assert preserved_record["device_id"] == "device-001"
    assert preserved_record["pressure"] == 1500


# ---------------------------------------------------------
# Valid records must never enter DLQ
# ---------------------------------------------------------

def test_valid_record_does_not_enter_dlq():

    record = valid_record()

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "valid"

    assert output.value == record


# ---------------------------------------------------------
# DLQ payload must be JSON serializable
# ---------------------------------------------------------

def test_parser_dlq_payload_is_json_serializable():

    message = b'{"event_id": "event-001", "temperature": }'

    outputs = run_parser(message)

    dlq_record = outputs[0].value

    serialized = json.dumps(dlq_record)

    assert isinstance(serialized, str)

    restored = json.loads(serialized)

    assert restored["error_stage"] == "json_parsing"


def test_validator_dlq_payload_is_json_serializable():

    record = valid_record()
    record["temperature"] = 999

    outputs = run_validator(record)

    dlq_record = outputs[0].value

    serialized = json.dumps(dlq_record)

    assert isinstance(serialized, str)

    restored = json.loads(serialized)

    assert restored["error_stage"] == "validation"
    assert isinstance(restored["errors"], list)
