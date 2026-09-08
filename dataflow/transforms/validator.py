import re
from datetime import datetime, timezone

import apache_beam as beam

from config.validation_config import (
    REQUIRED_FIELDS,
    SENSOR_RANGES,
    MAX_FUTURE_SECONDS,
    DEVICE_ID_MIN_LENGTH,
    DEVICE_ID_MAX_LENGTH,
    EVENT_ID_MIN_LENGTH,
    EVENT_ID_MAX_LENGTH
)

NULLABLE_FIELDS = {
    "temperature",
    "humidity",
    "pressure",
    "battery_level"
}

class ValidateTelemetry(beam.DoFn):

    VALID = "valid"
    INVALID = "invalid"

    valid_counter = beam.metrics.Metrics.counter( "telemetry", "telemetry_valid", )
    invalid_counter = beam.metrics.Metrics.counter( "telemetry", "telemetry_invalid", )

    def process(self, record):

        errors = []

        # -------------------------------------------------
        # Required fields
        # -------------------------------------------------

        for field in REQUIRED_FIELDS:

            if field not in record:

                errors.append(
                    f"Missing required field: {field}"
                )

            elif (
                record[field] is None
                and field not in NULLABLE_FIELDS
            ):

                errors.append(
                    f"Null required field: {field}"
                )
        # -------------------------------------------------
        # Event ID validation
        # -------------------------------------------------

        event_id = record.get("event_id")

        if event_id is not None:

            if not isinstance(event_id, str):

                errors.append(
                    "event_id must be a string"
                )

            else:

                event_id = event_id.strip()

                if not event_id:

                    errors.append(
                        "event_id cannot be empty"
                    )

                elif not (
                    EVENT_ID_MIN_LENGTH
                    <= len(event_id)
                    <= EVENT_ID_MAX_LENGTH
                ):

                    errors.append(
                        "event_id length is outside "
                        "allowed range"
                    )

        # -------------------------------------------------
        # Device ID validation
        # -------------------------------------------------

        device_id = record.get("device_id")

        if device_id is not None:

            if not isinstance(device_id, str):

                errors.append(
                    "device_id must be a string"
                )

            else:

                device_id = device_id.strip()

                if not device_id:

                    errors.append(
                        "device_id cannot be empty"
                    )

                elif not (
                    DEVICE_ID_MIN_LENGTH
                    <= len(device_id)
                    <= DEVICE_ID_MAX_LENGTH
                ):

                    errors.append(
                        "device_id length is outside "
                        "allowed range"
                    )

                elif not re.match(
                    r"^[A-Za-z0-9_-]+$",
                    device_id
                ):

                    errors.append(
                        "device_id contains invalid characters"
                    )

        # -------------------------------------------------
        # Timestamp validation
        # -------------------------------------------------

        event_timestamp = record.get(
            "event_timestamp"
        )

        parsed_timestamp = None

        if event_timestamp is not None:

            if not isinstance(
                event_timestamp,
                str
            ):

                errors.append(
                    "event_timestamp must be a string"
                )

            else:

                try:

                    timestamp_string = (
                        event_timestamp
                        .replace("Z", "+00:00")
                    )

                    parsed_timestamp = (
                        datetime.fromisoformat(
                            timestamp_string
                        )
                    )

                    if parsed_timestamp.tzinfo is None:

                        errors.append(
                            "event_timestamp must "
                            "include timezone"
                        )

                    else:

                        now = datetime.now(
                            timezone.utc
                        )

                        future_limit = (
                            now.timestamp()
                            + MAX_FUTURE_SECONDS
                        )

                        if (
                            parsed_timestamp.timestamp()
                            > future_limit
                        ):

                            errors.append(
                                "event_timestamp is "
                                "too far in the future"
                            )

                except (
                    ValueError,
                    TypeError
                ):

                    errors.append(
                        "event_timestamp is not "
                        "a valid ISO-8601 timestamp"
                    )

        # -------------------------------------------------
        # Range and numeric validation
        # -------------------------------------------------

        for field, limits in SENSOR_RANGES.items():

            if (
                field in record
                and record[field] is not None
            ):

                try:

                    value = float(
                        record[field]
                    )

                    if value < limits["min"]:

                        errors.append(
                            f"{field} below minimum: "
                            f"{value} < "
                            f"{limits['min']}"
                        )

                    if value > limits["max"]:

                        errors.append(
                            f"{field} above maximum: "
                            f"{value} > "
                            f"{limits['max']}"
                        )

                except (
                    ValueError,
                    TypeError
                ):

                    errors.append(
                        f"Invalid numeric value "
                        f"for {field}: "
                        f"{record[field]}"
                    )

        # -------------------------------------------------
        # Final routing
        # -------------------------------------------------

        if errors:

            self.invalid_counter.inc()

            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                {
                    "record": record,
                    "error_stage": "validation",
                    "errors": errors
                }
            )

        else:

            self.valid_counter.inc()

            yield beam.pvalue.TaggedOutput(
                self.VALID,
                record
            )
