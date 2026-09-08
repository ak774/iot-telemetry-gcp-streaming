## Phase 9H.3B — Rate Semantics and Operational Thresholds

### Observability Metric Semantics

The pipeline exposes event-level counters through Apache Beam metrics. Rates are derived from these counters in Cloud Monitoring rather than maintained as separate application counters.

#### Validation Metrics

| Metric              | Type    | Definition                                       |
| ------------------- | ------- | ------------------------------------------------ |
| `telemetry_valid`   | Counter | Number of telemetry records that pass validation |
| `telemetry_invalid` | Counter | Number of telemetry records that fail validation |

Invalid telemetry rate:

```text
telemetry_invalid
-------------------------------
telemetry_valid + telemetry_invalid
```

This represents the percentage of telemetry records that fail application-level validation.

#### Durable Replay Metrics

| Metric             | Type    | Definition                                                                            |
| ------------------ | ------- | ------------------------------------------------------------------------------------- |
| `spanner_new`      | Counter | Events successfully registered as new canonical events                                |
| `spanner_replay`   | Counter | Events whose event ID and payload hash were already registered                        |
| `spanner_conflict` | Counter | Events whose event ID exists but whose payload hash differs from the canonical record |

Replay rate:

```text
spanner_replay
------------------------------------------------
spanner_new + spanner_replay + spanner_conflict
```

Conflict rate:

```text
spanner_conflict
------------------------------------------------
spanner_new + spanner_replay + spanner_conflict
```

Replay events are not automatically failures because duplicate delivery can occur in an at-least-once streaming system.

Conflicts are treated as data-integrity anomalies because the same immutable event ID is associated with different payload content.

#### Spanner Reliability Metrics

| Metric                  | Type    | Definition                                                              |
| ----------------------- | ------- | ----------------------------------------------------------------------- |
| `spanner_retry_count`   | Counter | Number of transient Spanner failures for which another attempt was made |
| `spanner_failure_count` | Counter | Number of Spanner operations that exhausted the bounded retry policy    |

A retry does not necessarily indicate an unsuccessful event. A transient failure followed by a successful retry increments the retry counter but does not increment the failure counter.

`AlreadyExists` is excluded from infrastructure failure metrics because it is an expected event-registration race handled by the replay/conflict logic.

#### Processing Latency

| Metric                            | Type         | Definition                                                                   |
| --------------------------------- | ------------ | ---------------------------------------------------------------------------- |
| `telemetry_processing_latency_ms` | Distribution | Difference between telemetry event timestamp and application processing time |

The distribution metric is used instead of a counter because processing latency is a numeric measurement. It allows analysis of count, minimum, maximum, mean, and percentile latency.

### Operational Thresholds

The following thresholds are initial operational thresholds for this portfolio implementation. Production thresholds should be established from historical baselines, expected traffic patterns, and defined SLOs.

| Signal                 |      Initial Threshold | Severity         | Operational Meaning                                          |
| ---------------------- | ---------------------: | ---------------- | ------------------------------------------------------------ |
| Invalid telemetry rate |                   > 5% | Warning          | Possible producer or telemetry-quality problem               |
| Replay rate            |                   > 2% | Warning          | Possible increase in duplicate delivery                      |
| Conflict count         |                    > 0 | Critical         | Potential event identity/data-integrity problem              |
| Spanner failure count  |                    > 0 | Critical         | Durable replay protection experienced an unrecovered failure |
| Spanner retry count    |     Sustained increase | Warning          | Possible dependency instability                              |
| Processing latency P95 |           > 30 seconds | Warning          | Application processing degradation                           |
| Dataflow system lag    | > 60 seconds sustained | Critical/Warning | Possible processing backlog or resource constraint           |

### Rate Calculation Strategy

Rates are intentionally not implemented as separate Beam counters.

Counters represent discrete events, while rates are derived from those counters over a selected monitoring interval.

This provides flexibility to calculate the same underlying event rate over different windows such as:

* 5 minutes
* 1 hour
* 6 hours
* 24 hours

without changing or redeploying the Dataflow pipeline.

### Dataflow-Native vs Application Metrics

Infrastructure-level health should use Dataflow's native monitoring capabilities, while custom Beam metrics are reserved for application-specific behavior.

**Dataflow-native monitoring:**

* Job state
* Worker health
* Throughput
* System lag
* Backlog
* Autoscaling
* Resource utilization

**Custom application metrics:**

* Validation outcomes
* Replay decisions
* Payload conflicts
* Spanner retry attempts
* Spanner failures
* Event processing latency

This separation avoids duplicating platform metrics and keeps custom instrumentation focused on signals that Dataflow cannot infer from the pipeline itself.
