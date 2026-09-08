import apache_beam as beam

from config.pipeline_config import (
    WINDOW_SIZE_SECONDS,
    ALLOWED_LATENESS_SECONDS
)


class ApplyTelemetryWindow(beam.PTransform):

    def expand(self, pcoll):

        return (
            pcoll
            | "ApplyFixedEventTimeWindow"
            >> beam.WindowInto(
                beam.window.FixedWindows(
                    WINDOW_SIZE_SECONDS
                ),
                trigger=beam.trigger.AfterWatermark(
                    late=beam.trigger.AfterCount(1)
                ),
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
            )
        )