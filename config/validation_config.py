REQUIRED_FIELDS = [
    "event_id",
    "device_id",
    "event_timestamp",
    "temperature",
    "humidity",
    "pressure",
    "battery_level"
]


SENSOR_RANGES = {
    "temperature": {
        "min": -50.0,
        "max": 100.0
    },
    "humidity": {
        "min": 0.0,
        "max": 100.0
    },
    "pressure": {
        "min": 800.0,
        "max": 1200.0
    },
    "battery_level": {
        "min": 0.0,
        "max": 100.0
    }
}


MAX_FUTURE_SECONDS = 300


DEVICE_ID_MIN_LENGTH = 1
DEVICE_ID_MAX_LENGTH = 100


EVENT_ID_MIN_LENGTH = 1
EVENT_ID_MAX_LENGTH = 100