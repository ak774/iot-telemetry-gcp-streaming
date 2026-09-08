import json

import apache_beam as beam


class ParseTelemetry(beam.DoFn):

    VALID = "valid"
    INVALID = "invalid"

    def process(self, message):

        raw_message = message.decode(
            "utf-8",
            errors="replace"
        )

        try:

            record = json.loads(raw_message)

            yield beam.pvalue.TaggedOutput(
                self.VALID,
                record
            )

        except json.JSONDecodeError as error:

            error_record = {
                "raw_message": raw_message,
                "error_stage": "json_parsing",
                "error_message": str(error)
            }

            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                error_record
            )