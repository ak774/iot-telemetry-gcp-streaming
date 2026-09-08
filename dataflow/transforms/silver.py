import apache_beam as beam
from datetime import timezone


class PrepareSilverRecord(beam.DoFn):

    def process(
        self,
        record,
        window=beam.DoFn.WindowParam
    ):

        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()

        if window_start.tzinfo is None:
            window_start = window_start.replace(
                tzinfo=timezone.utc
            )

        if window_end.tzinfo is None:
            window_end = window_end.replace(
                tzinfo=timezone.utc
            )

        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "temperature": record.get("temperature"),
            "humidity": record.get("humidity"),
            "pressure": record.get("pressure"),
            "battery_level": record.get("battery_level"),
            "window_start": window_start,
            "window_end": window_end
        }


class PrepareGoldInputDebugRecord(beam.DoFn):

    def process(
        self,
        record,
        window=beam.DoFn.WindowParam
    ):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()

        if window_start.tzinfo is None:
            window_start = window_start.replace(
                tzinfo=timezone.utc
            )

        if window_end.tzinfo is None:
            window_end = window_end.replace(
                tzinfo=timezone.utc
            )

        yield {
            "event_id": record["event_id"],
            "device_id": record["device_id"],
            "event_timestamp": record["event_timestamp"],
            "window_start": window_start,
            "window_end": window_end,
            "temperature": record.get("temperature"),
            "humidity": record.get("humidity"),
            "pressure": record.get("pressure"),
            "battery_level": record.get("battery_level"),
        }