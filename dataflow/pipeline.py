import json
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions
)


PROJECT_ID = os.environ["GCP_PROJECT_ID"]

PUBSUB_SUBSCRIPTION = (
    f"projects/{PROJECT_ID}"
    f"/subscriptions/dataflow-telemetry-sub"
)

BIGQUERY_TABLE = (
    f"{PROJECT_ID}:iot_raw.raw_telemetry"
)


class ParseTelemetry(beam.DoFn):
    """Parse Pub/Sub JSON bytes into a Python dictionary."""

    def process(self, message):

        record = json.loads(
            message.decode("utf-8")
        )

        yield record


def run():

    pipeline_options = PipelineOptions()

    pipeline_options.view_as(
        StandardOptions
    ).streaming = True

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        (
            pipeline

            | "ReadFromPubSub"
            >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            )

            | "ParseJSON"
            >> beam.ParDo(
                ParseTelemetry()
            )

            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                BIGQUERY_TABLE,
                create_disposition=(
                    beam.io.BigQueryDisposition
                    .CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition
                    .WRITE_APPEND
                )
            )
        )


if __name__ == "__main__":
    run()