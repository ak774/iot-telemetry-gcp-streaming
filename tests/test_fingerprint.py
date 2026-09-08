import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from dataflow.transforms.fingerprint import (
    AddPayloadFingerprint
)


def test_same_payload_produces_same_hash():

    record = {
        "event_id": "fingerprint-001",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T10:00:00Z",
        "temperature": 25,
        "humidity": 50,
        "pressure": 1000,
        "battery_level": 90,
    }

    with TestPipeline() as pipeline:

        output = (
            pipeline
            | "CreateRecords"
            >> beam.Create([
                record,
                dict(record)
            ])
            | "AddFingerprint"
            >> beam.ParDo(
                AddPayloadFingerprint()
            )
        )

        assert_that(
            output,
            lambda records: (
                len(records) == 2
                and records[0]["payload_hash"]
                == records[1]["payload_hash"]
            )
        )


def test_different_payload_produces_different_hash():

    record_1 = {
        "event_id": "fingerprint-002",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T10:00:00Z",
        "temperature": 25,
        "humidity": 50,
        "pressure": 1000,
        "battery_level": 90,
    }

    record_2 = {
        "event_id": "fingerprint-002",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T10:00:00Z",
        "temperature": 100,
        "humidity": 90,
        "pressure": 1100,
        "battery_level": 50,
    }

    with TestPipeline() as pipeline:

        output = (
            pipeline
            | "CreateRecords"
            >> beam.Create([
                record_1,
                record_2
            ])
            | "AddFingerprint"
            >> beam.ParDo(
                AddPayloadFingerprint()
            )
        )

        assert_that(
            output,
            lambda records: (
                len(records) == 2
                and records[0]["payload_hash"]
                != records[1]["payload_hash"]
            )
        )


def test_equivalent_timestamp_and_numeric_formats_produce_same_hash():

    record_1 = {
        "event_id": "fingerprint-003",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T10:00:00Z",
        "temperature": 25,
        "humidity": 50,
        "pressure": 1000,
        "battery_level": 90,
    }

    record_2 = {
        "event_id": "fingerprint-003",
        "device_id": "device-001",
        "event_timestamp": "2026-09-04T10:00:00+00:00",
        "temperature": 25.0,
        "humidity": 50.0,
        "pressure": 1000.0,
        "battery_level": 90.0,
    }

    with TestPipeline() as pipeline:

        output = (
            pipeline
            | "CreateRecords"
            >> beam.Create([
                record_1,
                record_2
            ])
            | "AddFingerprint"
            >> beam.ParDo(
                AddPayloadFingerprint()
            )
        )

        assert_that(
            output,
            lambda records: (
                len(records) == 2
                and records[0]["payload_hash"]
                == records[1]["payload_hash"]
            )
        )