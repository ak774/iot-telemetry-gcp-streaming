import hashlib
import json
from datetime import datetime, timezone

import apache_beam as beam


FINGERPRINT_FIELDS = (
    "event_id",
    "device_id",
    "event_timestamp",
    "temperature",
    "humidity",
    "pressure",
    "battery_level",
)


class AddPayloadFingerprint(beam.DoFn):

    def _normalize_timestamp(self, value):

        if value is None:
            return None

        timestamp_string = value.replace(
            "Z",
            "+00:00"
        )

        parsed = datetime.fromisoformat(
            timestamp_string
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        parsed = parsed.astimezone(
            timezone.utc
        )

        return parsed.isoformat()

    def _normalize_record(self, record):

        normalized = {}

        for field in FINGERPRINT_FIELDS:

            value = record.get(field)

            if field == "event_timestamp":

                value = self._normalize_timestamp(
                    value
                )

            elif field in {
                "temperature",
                "humidity",
                "pressure",
                "battery_level",
            }:

                if value is not None:
                    value = float(value)

            elif field in {
                "event_id",
                "device_id",
            }:

                if value is not None:
                    value = value.strip()

            normalized[field] = value

        return normalized

    def process(self, record):

        fingerprint_record = (
            self._normalize_record(record)
        )

        canonical_payload = json.dumps(
            fingerprint_record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True
        )

        payload_hash = hashlib.sha256(
            canonical_payload.encode("utf-8")
        ).hexdigest()

        output = dict(record)

        output["payload_hash"] = payload_hash

        yield output