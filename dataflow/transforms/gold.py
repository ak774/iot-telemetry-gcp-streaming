import apache_beam as beam
from datetime import timezone


class AggregateTelemetry(beam.CombineFn):

    def create_accumulator(self):
        return {
            "event_count": 0,
            "temperature_sum": 0.0,
            "temperature_count": 0,
            "temperature_min": None,
            "temperature_max": None,
            "humidity_sum": 0.0,
            "humidity_count": 0,
            "pressure_sum": 0.0,
            "pressure_count": 0,
            "battery_sum": 0.0,
            "battery_count": 0,
        }

    def add_input(self, accumulator, record):

        accumulator["event_count"] += 1

        temperature = record.get("temperature")
        if temperature is not None:
            value = float(temperature)

            accumulator["temperature_sum"] += value
            accumulator["temperature_count"] += 1

            if (
                accumulator["temperature_min"] is None
                or value < accumulator["temperature_min"]
            ):
                accumulator["temperature_min"] = value

            if (
                accumulator["temperature_max"] is None
                or value > accumulator["temperature_max"]
            ):
                accumulator["temperature_max"] = value

        humidity = record.get("humidity")
        if humidity is not None:
            accumulator["humidity_sum"] += float(humidity)
            accumulator["humidity_count"] += 1

        pressure = record.get("pressure")
        if pressure is not None:
            accumulator["pressure_sum"] += float(pressure)
            accumulator["pressure_count"] += 1

        battery = record.get("battery_level")
        if battery is not None:
            accumulator["battery_sum"] += float(battery)
            accumulator["battery_count"] += 1

        return accumulator

    def merge_accumulators(self, accumulators):

        merged = self.create_accumulator()

        for accumulator in accumulators:

            merged["event_count"] += accumulator["event_count"]

            merged["temperature_sum"] += accumulator["temperature_sum"]
            merged["temperature_count"] += accumulator["temperature_count"]

            if accumulator["temperature_min"] is not None:
                if (
                    merged["temperature_min"] is None
                    or accumulator["temperature_min"]
                    < merged["temperature_min"]
                ):
                    merged["temperature_min"] = accumulator["temperature_min"]

            if accumulator["temperature_max"] is not None:
                if (
                    merged["temperature_max"] is None
                    or accumulator["temperature_max"]
                    > merged["temperature_max"]
                ):
                    merged["temperature_max"] = accumulator["temperature_max"]

            merged["humidity_sum"] += accumulator["humidity_sum"]
            merged["humidity_count"] += accumulator["humidity_count"]

            merged["pressure_sum"] += accumulator["pressure_sum"]
            merged["pressure_count"] += accumulator["pressure_count"]

            merged["battery_sum"] += accumulator["battery_sum"]
            merged["battery_count"] += accumulator["battery_count"]

        return merged

    def extract_output(self, accumulator):

        def average(total, count):
            if count == 0:
                return None
            return total / count

        return {
            "event_count": accumulator["event_count"],
            "avg_temperature": average(
                accumulator["temperature_sum"],
                accumulator["temperature_count"]
            ),
            "min_temperature": accumulator["temperature_min"],
            "max_temperature": accumulator["temperature_max"],
            "avg_humidity": average(
                accumulator["humidity_sum"],
                accumulator["humidity_count"]
            ),
            "avg_pressure": average(
                accumulator["pressure_sum"],
                accumulator["pressure_count"]
            ),
            "avg_battery_level": average(
                accumulator["battery_sum"],
                accumulator["battery_count"]
            ),
        }


class PrepareGoldRecord(beam.DoFn):

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam
    ):

        device_id, metrics = element

        window_start = window.start.to_utc_datetime().replace(
            tzinfo=timezone.utc
        )

        window_end = window.end.to_utc_datetime().replace(
            tzinfo=timezone.utc
        )

        yield {
            "device_id": device_id,
            "window_start": window_start,
            "window_end": window_end,
            **metrics
        }
