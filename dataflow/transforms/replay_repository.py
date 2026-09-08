import time
from datetime import datetime, timezone
import random

import apache_beam as beam

from google.api_core import exceptions as core_exceptions
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.api_core.exceptions import AlreadyExists


MAX_RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 10.0


class ReplayRegistryRepository:
    TABLE_NAME = "telemetry_event_registry"

    RETRYABLE_EXCEPTIONS = (
        core_exceptions.Aborted,
        core_exceptions.Cancelled,
        core_exceptions.DeadlineExceeded,
        core_exceptions.ResourceExhausted,
        core_exceptions.ServiceUnavailable,
    )

    retry_counter = beam.metrics.Metrics.counter(
    "spanner",
    "spanner_retry_count",
    )

    failure_counter = beam.metrics.Metrics.counter(
        "spanner",
        "spanner_failure_count",
    )

    def __init__(self, project_id, instance_id, database_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.client = None
        self.instance = None
        self.database = None

    def connect(self):
        self.client = spanner.Client(project=self.project_id)
        self.instance = self.client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)

    def _is_retryable_exception(self, exc):
        return isinstance(exc, self.RETRYABLE_EXCEPTIONS)

    def _run_with_retry(self, operation, operation_name):
        delay = INITIAL_RETRY_DELAY_SECONDS

        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                return operation()

            except AlreadyExists:
                # AlreadyExists is part of the expected event-registration
                # race/replay flow and must not be retried here.
                raise

            except Exception as exc:
                if not self._is_retryable_exception(exc):
                    raise

                if attempt == MAX_RETRY_ATTEMPTS:
                    raise

                self.retry_counter.inc()
                jitter = random.uniform(0, 0.1 * delay)

                retry_delay = min(
                    delay + jitter,
                    MAX_RETRY_DELAY_SECONDS,
                )

                print(
                    f"Spanner transient failure: "
                    f"operation={operation_name}, "
                    f"attempt={attempt}, "
                    f"retry_delay={retry_delay:.2f}s, "
                    f"exception={type(exc).__name__}"
                )

                time.sleep(retry_delay)

                delay = min(
                    delay * 2,
                    MAX_RETRY_DELAY_SECONDS,
                )

    def get_event(self, event_id):
        def operation():
            with self.database.snapshot() as snapshot:
                rows = snapshot.execute_sql(
                    """
                    SELECT
                        event_id,
                        device_id,
                        event_timestamp,
                        payload_hash,
                        first_seen_at,
                        last_seen_at
                    FROM telemetry_event_registry
                    WHERE event_id = @event_id
                    """,
                    params={"event_id": event_id},
                    param_types={"event_id": param_types.STRING},
                )

                rows = list(rows)

                if not rows:
                    return None

                row = rows[0]

                return {
                    "event_id": row[0],
                    "device_id": row[1],
                    "event_timestamp": row[2],
                    "payload_hash": row[3],
                    "first_seen_at": row[4],
                    "last_seen_at": row[5],
                }

        return self._run_with_retry(
            operation,
            "get_event",
        )

    def insert_event(self, record):
        now = datetime.now(timezone.utc)

        def operation():
            def insert(transaction):
                transaction.insert(
                    table=self.TABLE_NAME,
                    columns=[
                        "event_id",
                        "device_id",
                        "event_timestamp",
                        "payload_hash",
                        "first_seen_at",
                        "last_seen_at",
                    ],
                    values=[[
                        record["event_id"],
                        record["device_id"],
                        record["event_timestamp"],
                        record["payload_hash"],
                        now,
                        now,
                    ]],
                )

            return self.database.run_in_transaction(insert)

        return self._run_with_retry(
            operation,
            "insert_event",
        )

    def register_event(self, record):
        event_id = record["event_id"]
        incoming_hash = record["payload_hash"]

        existing = self.get_event(event_id)

        if existing is not None:
            if existing["payload_hash"] == incoming_hash:
                return {
                    "status": "replay",
                    "registry_record": existing,
                }

            return {
                "status": "conflict",
                "registry_record": existing,
            }

        try:
            self.insert_event(record)

            return {
                "status": "new",
                "record": record,
            }

        except AlreadyExists:
            existing = self.get_event(event_id)

            if existing is None:
                raise

            if existing["payload_hash"] == incoming_hash:
                return {
                    "status": "replay",
                    "registry_record": existing,
                }

            return {
                "status": "conflict",
                "registry_record": existing,
            }