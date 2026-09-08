
import json

from apache_beam.pvalue import TaggedOutput

from dataflow.transforms.parser import ParseTelemetry


def run_parser(message):

    parser = ParseTelemetry()

    outputs = list(
        parser.process(message)
    )

    return outputs


def test_valid_json():

    record = {
        "event_id": "event-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-28T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }

    message = json.dumps(record).encode("utf-8")

    outputs = run_parser(message)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "valid"

    assert output.value["device_id"] == "device-001"
    assert output.value["temperature"] == 25.5


def test_invalid_json():

    message = b'{"device_id": "device-001", "temperature": }'

    outputs = run_parser(message)

    assert len(outputs) == 1

    output = outputs[0]

    assert isinstance(output, TaggedOutput)

    assert output.tag == "invalid"

    assert output.value["error_stage"] == "json_parsing"

    assert "raw_message" in output.value

    assert "error_message" in output.value

def test_valid_json_preserves_record():

    record = {
        "event_id": "event-preserve-001",
        "device_id": "device-001",
        "event_timestamp": "2026-08-28T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1012.5,
        "battery_level": 95.0
    }

    message = json.dumps(record).encode("utf-8")

    outputs = run_parser(message)

    assert len(outputs) == 1

    output = outputs[0]

    assert output.tag == "valid"
    assert output.value == record
