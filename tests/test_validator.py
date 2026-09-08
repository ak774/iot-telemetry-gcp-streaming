
from apache_beam.pvalue import TaggedOutput

from dataflow.transforms.validator import ValidateTelemetry

from datetime import datetime, timezone, timedelta


def run_validator(record):

    validator = ValidateTelemetry()

    outputs = list(
        validator.process(record)
    )

    return outputs


def valid_record():

    return {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-28T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }


def test_valid_record():

    record = valid_record()

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "valid"

    assert output.value["device_id"] == "device-001"


def test_missing_required_field():

    record = valid_record()

    del record["device_id"]

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert output.value["error_stage"] == "validation"

    assert any(
        "device_id" in error
        for error in output.value["errors"]
    )


def test_temperature_above_maximum():

    record = valid_record()

    record["temperature"] = 150

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert any(
        "temperature above maximum" in error
        for error in output.value["errors"]
    )


def test_humidity_below_minimum():

    record = valid_record()

    record["humidity"] = -10

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert any(
        "humidity below minimum" in error
        for error in output.value["errors"]
    )


def test_battery_above_maximum():

    record = valid_record()

    record["battery_level"] = 150

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert any(
        "battery_level above maximum" in error
        for error in output.value["errors"]
    )


def test_invalid_numeric_value():

    record = valid_record()

    record["temperature"] = "not-a-number"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert any(
        "Invalid numeric value" in error
        for error in output.value["errors"]
    )


def test_missing_pressure():

    record = valid_record()

    del record["pressure"]

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "pressure" in error
        for error in output.value["errors"]
    )


def test_invalid_timestamp():

    record = valid_record()

    record["event_timestamp"] = "not-a-timestamp"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "valid ISO-8601 timestamp" in error
        for error in output.value["errors"]
    )


def test_timestamp_without_timezone():

    record = valid_record()

    record["event_timestamp"] = "2026-08-30T12:00:00"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "include timezone" in error
        for error in output.value["errors"]
    )


def test_future_timestamp():

    record = valid_record()

    future_timestamp = (
        datetime.now(timezone.utc)
        + timedelta(hours=1)
    ).isoformat()

    record["event_timestamp"] = future_timestamp

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "too far in the future" in error
        for error in output.value["errors"]
    )


def test_empty_event_id():

    record = valid_record()

    record["event_id"] = ""

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "event_id cannot be empty" in error
        for error in output.value["errors"]
    )


def test_invalid_device_id():

    record = valid_record()

    record["device_id"] = "device 001"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "device_id contains invalid characters"
        in error
        for error in output.value["errors"]
    )


def test_pressure_below_minimum():

    record = valid_record()

    record["pressure"] = 700

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "pressure below minimum" in error
        for error in output.value["errors"]
    )


def test_non_numeric_humidity():

    record = valid_record()

    record["humidity"] = "invalid"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "Invalid numeric value" in error
        for error in output.value["errors"]
    )


def test_event_id_must_be_string():

    record = valid_record()

    record["event_id"] = 12345

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "event_id must be a string" in error
        for error in output.value["errors"]
    )


def test_device_id_must_be_string():

    record = valid_record()

    record["device_id"] = 12345

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "device_id must be a string" in error
        for error in output.value["errors"]
    )


def test_event_timestamp_must_be_string():

    record = valid_record()

    record["event_timestamp"] = 12345

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "event_timestamp must be a string" in error
        for error in output.value["errors"]
    )


def test_numeric_string_is_accepted():

    record = valid_record()

    record["temperature"] = "25.5"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "valid"


def test_non_numeric_pressure_is_rejected():

    record = valid_record()

    record["pressure"] = "abc"

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "Invalid numeric value for pressure" in error
        for error in output.value["errors"]
    )


def test_nullable_sensor_field_can_be_none():

    record = valid_record()

    record["temperature"] = None

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "valid"


def test_required_field_cannot_be_none():

    record = valid_record()

    record["event_id"] = None

    outputs = run_validator(record)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "invalid"

    assert any(
        "Null required field: event_id" in error
        for error in output.value["errors"]
    )