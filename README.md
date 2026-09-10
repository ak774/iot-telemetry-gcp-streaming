# IoT Telemetry GCP Streaming Pipeline

A production-style IoT telemetry streaming platform built on **Google Cloud Platform**, using **Pub/Sub, Apache Beam/Dataflow, BigQuery, Spanner, Cloud Monitoring, Terraform, and GitHub Actions**.

The project is designed to demonstrate the engineering problems that appear in real-time data platforms beyond simple ingestion: schema and data validation, dead-letter handling, event-time processing, duplicate delivery, payload conflicts, durable replay protection, observability, infrastructure as code, CI/CD, deployment promotion, and rollback readiness.

---

## Table of Contents

* [Overview](#overview)
* [Problem Statement](#problem-statement)
* [Architecture](#architecture)
* [Technology Stack](#technology-stack)
* [End-to-End Data Flow](#end-to-end-data-flow)
* [Data Processing Design](#data-processing-design)
* [Data Model](#data-model)
* [Replay and Deduplication Design](#replay-and-deduplication-design)
* [Event-Time Processing](#event-time-processing)
* [Dead-Letter Queue](#dead-letter-queue)
* [BigQuery Layers](#bigquery-layers)
* [System Design Decisions](#system-design-decisions)
* [Observability](#observability)
* [Infrastructure as Code](#infrastructure-as-code)
* [CI/CD Architecture](#cicd-architecture)
* [Deployment and Promotion](#deployment-and-promotion)
* [Rollback Strategy](#rollback-strategy)
* [Repository Structure](#repository-structure)
* [Local Development](#local-development)
* [Testing](#testing)
* [Production Validation](#production-validation)
* [Key Engineering Trade-offs](#key-engineering-trade-offs)
* [Interview Discussion Points](#interview-discussion-points)
* [Limitations and Future Improvements](#limitations-and-future-improvements)

---

# Overview

This project implements an end-to-end streaming telemetry platform for IoT devices.

Telemetry events are published to **Google Cloud Pub/Sub**, processed by an **Apache Beam streaming pipeline running on Google Cloud Dataflow**, and written into **BigQuery** for analytical consumption.

The pipeline supports:

* JSON parsing
* required-field validation
* type and range validation
* event timestamp validation
* invalid-event routing to a DLQ
* payload fingerprinting
* event-ID conflict detection
* short-window duplicate detection
* durable replay protection using Spanner
* event-time windowing
* late-event handling
* Silver-layer storage
* Gold-layer aggregations
* application-level metrics
* Cloud Monitoring dashboards and alert policies
* Terraform-managed infrastructure
* GitHub Actions CI/CD
* Workload Identity Federation
* isolated candidate deployments
* explicit deployment promotion
* deployment identity tracking
* rollback/recovery planning

The repository is structured around the principle:

> **Separate data-plane processing from deployment/control-plane state.**

The streaming pipeline processes telemetry, while the deployment registry tracks which Dataflow execution is considered the active deployment.

---

# Problem Statement

IoT telemetry systems typically have to deal with several problems simultaneously:

1. Events can arrive continuously.
2. Delivery is not necessarily exactly-once at the transport layer.
3. Producers can send malformed JSON.
4. Producers can send structurally valid but invalid sensor values.
5. The same event can be delivered more than once.
6. The same `event_id` can potentially appear with different payloads.
7. Events can arrive out of order.
8. Event time and processing time can differ significantly.
9. Streaming jobs must be deployed without unnecessarily interfering with production traffic.
10. Operational failures need to be observable.
11. Deployment identity needs to be traceable from Git commit to the actual Dataflow job.

This project addresses those concerns as separate stages instead of attempting to solve everything with one deduplication mechanism.

---

# Architecture

## High-Level Architecture

```text
                         IoT Devices / Producers
                                  |
                                  v
                         +------------------+
                         | Google Pub/Sub   |
                         | iot-telemetry    |
                         +---------+--------+
                                   |
                                   v
                     dataflow-telemetry-sub
                                   |
                                   v
                     +-----------------------+
                     | Google Dataflow       |
                     | Apache Beam           |
                     |                       |
                     | 1. Parse              |
                     | 2. Validate           |
                     | 3. Fingerprint        |
                     | 4. Conflict detection |
                     | 5. Deduplicate        |
                     | 6. Spanner replay     |
                     | 7. Event-time window  |
                     +-----------+-----------+
                                 |
                +----------------+----------------+
                |                                 |
                v                                 v
        +---------------+                 +---------------+
        | BigQuery      |                 | Pub/Sub DLQ   |
        | RAW / Silver  |                 |               |
        | / Gold        |                 | invalid data  |
        +---------------+                 +---------------+
                |
                v
        +---------------+
        | Analytics /   |
        | dashboards    |
        +---------------+

                Durable replay state
                         ^
                         |
                +--------+---------+
                | Google Spanner   |
                | iot_registry     |
                +------------------+

                Operational observability
                         ^
                         |
                +--------+---------+
                | Cloud Monitoring |
                | Dashboard/Alerts |
                +------------------+
```

---

# Technology Stack

| Layer                 | Technology                   | Purpose                                       |
| --------------------- | ---------------------------- | --------------------------------------------- |
| Event transport       | Google Pub/Sub               | Durable streaming ingestion                   |
| Stream processing     | Apache Beam                  | Portable stream-processing model              |
| Execution engine      | Google Dataflow              | Managed distributed streaming execution       |
| Raw/analytics storage | BigQuery                     | Analytical storage                            |
| Durable replay state  | Cloud Spanner                | Hot-path event registration/replay protection |
| Monitoring            | Cloud Monitoring             | Pipeline and application observability        |
| Infrastructure        | Terraform                    | Infrastructure as Code                        |
| CI/CD                 | GitHub Actions               | Automated validation and deployment           |
| Authentication        | Workload Identity Federation | Keyless GitHub → GCP authentication           |
| Language              | Python                       | Beam/Dataflow implementation                  |
| Testing               | pytest                       | Unit/integration testing                      |

---

# End-to-End Data Flow

A telemetry message follows this logical path:

```text
Pub/Sub
   |
   v
Parse JSON
   |
   +---- invalid JSON ----------> DLQ
   |
   v
Validate telemetry
   |
   +---- invalid record --------> DLQ
   |
   v
Raw BigQuery
   |
   v
Processing latency metric
   |
   v
Payload fingerprint
   |
   v
Short-term conflict detection
   |
   +---- conflicting payload --> Conflict audit
   |
   v
Beam deduplication
   |
   v
Durable Spanner replay check
   |
   +---- replay ---------------> Replay audit
   |
   +---- conflict -------------> Conflict audit
   |
   v
Event-time window
   |
   +----> Silver
   |
   +----> Gold aggregation
```

The current pipeline explicitly implements these stages in the Dataflow pipeline.

---

# Data Processing Design

## 1. JSON Parsing

The first processing stage converts the Pub/Sub message bytes into a JSON object.

Malformed JSON is not allowed into the normal processing path.

Instead, the parser emits an invalid side output containing:

* original message
* error stage
* parsing error

The parser implementation explicitly records `error_stage = "json_parsing"` for JSON decoding failures.

---

## 2. Telemetry Validation

Structurally valid JSON still needs application-level validation.

Validation includes:

### Required fields

The pipeline validates required fields such as:

* `event_id`
* `device_id`
* `event_timestamp`

Sensor measurements are treated as nullable fields where appropriate.

### Event ID

`event_id` is validated as a string with configured length constraints.

### Device ID

`device_id` is validated for:

* string type
* non-empty value
* length
* allowed characters

The implementation allows alphanumeric characters, `_`, and `-`.

### Event timestamp

The timestamp must:

* be a string
* be ISO-8601 compatible
* include timezone information
* not be too far in the future

### Sensor values

Configured sensor fields are checked for:

* numeric conversion
* minimum values
* maximum values

Invalid records are routed to the invalid side output rather than being allowed into the analytical processing path.

---

# Data Model

The core telemetry event has the following logical structure:

```text
TelemetryEvent
-------------------------------
event_id
device_id
event_timestamp
temperature
humidity
pressure
battery_level
```

The pipeline then adds processing metadata as it moves through the stages.

---

# Raw Layer

The valid validated telemetry event is appended to the Raw BigQuery table.

Conceptually:

```text
RAW_TELEMETRY
--------------------------------
event_id
device_id
event_timestamp
temperature
humidity
pressure
battery_level
```

The Raw layer preserves the validated event stream before the downstream event-time/deduplication processing.

The current Dataflow implementation writes validated records to the configured `RAW_TABLE` using append semantics.

---

# Fingerprinting

Each valid event receives a deterministic SHA-256 payload fingerprint.

The fingerprint is based on normalized event content.

Conceptually:

```text
payload_hash =
SHA256(
    canonicalized event payload
)
```

The purpose is to distinguish:

```text
same event_id + same payload
```

from:

```text
same event_id + different payload
```

This distinction is critical.

A repeated event with identical content is a duplicate/replay.

A repeated `event_id` with different content is a data-integrity conflict.

---

# Conflict Detection

The pipeline performs short-lived stateful conflict detection.

For a given `event_id`:

### First occurrence

```text
event_id = E1
payload_hash = H1

→ store H1
→ normal processing
```

### Same event and same payload

```text
event_id = E1
payload_hash = H1

→ legitimate duplicate
→ continue through the normal path
```

### Same event and different payload

```text
event_id = E1
payload_hash = H2

→ conflict
→ conflict side output
→ conflict audit
```

The state is configured to expire after 600 seconds.

This logic is implemented using Beam state and a real-time timer.

---

# Why Fingerprinting and Deduplication Are Separate

These solve different problems.

## Fingerprinting

Answers:

> "Is this payload identical to the previously observed payload for this event ID?"

## Deduplication

Answers:

> "Should this repeated event be allowed through the processing path?"

## Durable replay registry

Answers:

> "Has this event identity already been accepted as canonical, even if the in-memory/window state no longer exists?"

This layered design is intentional.

---

# Deduplication

After conflict detection, the clean stream is assigned an event timestamp and passed through Beam deduplication.

The pipeline uses event-ID based deduplication with a bounded duration.

This handles duplicate delivery that occurs within the Beam deduplication horizon.

However, Beam state is not treated as the long-term system of record for replay protection.

That responsibility belongs to Spanner.

---

# Durable Replay Protection

## Why Spanner?

Pub/Sub and distributed streaming systems can produce duplicate deliveries.

A short-lived Beam state store is useful for efficient streaming deduplication, but it should not be the only long-lived record of event identity.

The project therefore uses:

```text
Google Spanner
    |
    +-- telemetry_event_registry
```

as the durable event registration layer.

The Dataflow worker uses the Spanner database with the `roles/spanner.databaseUser` role. Terraform provisions the Spanner instance/database and the IAM binding.

---

# Durable Replay Decision Model

For each event:

```text
                    +----------------+
                    | event_id       |
                    | payload_hash   |
                    +-------+--------+
                            |
                            v
                    +---------------+
                    | Spanner       |
                    | registry      |
                    +-------+-------+
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
             NEW          REPLAY       CONFLICT
              |             |             |
              v             v             v
          process       audit only    conflict audit
```

The current `DurableReplayDecision` implementation produces explicit `new`, `replay`, and `conflict` outputs.

---

# Spanner Data Model

The durable replay registry is logically:

```text
telemetry_event_registry
------------------------------------------------
event_id          STRING   PRIMARY KEY
device_id         STRING
event_timestamp   TIMESTAMP
payload_hash      STRING
first_seen_at     TIMESTAMP
last_seen_at      TIMESTAMP
```

The key design decision is:

```text
PRIMARY KEY(event_id)
```

because event identity is defined by `event_id`.

The payload hash is then used to distinguish:

```text
same event identity + same payload
```

from:

```text
same event identity + different payload
```

---

# Replay Semantics

## New event

```text
event_id not present

→ register event
→ spanner_new
→ continue
```

## Replay

```text
event_id exists
AND
payload_hash matches

→ spanner_replay
→ audit replay
→ do not process as a new canonical event
```

## Conflict

```text
event_id exists
AND
payload_hash differs

→ spanner_conflict
→ write conflict audit
→ do not treat as a new canonical event
```

A replay is not automatically an error because duplicate delivery is expected in distributed streaming systems.

A conflict is treated as a data-integrity anomaly.

---

# BigQuery Silver Layer

Silver represents the event after the stream has passed the canonical processing controls.

The logical Silver grain is:

> **One accepted telemetry event associated with its event-time window.**

Typical fields include:

```text
event_id
device_id
event_timestamp
temperature
humidity
pressure
battery_level
window_start
window_end
```

The pipeline creates Silver records after durable replay acceptance and event-time window assignment.

---

# BigQuery Gold Layer

Gold provides analytical aggregates.

## Grain

```text
(device_id, window_start, window_end)
```

The production aggregation uses 60-second event-time windows.

The Gold record contains:

```text
device_id
window_start
window_end
event_count
avg_temperature
min_temperature
max_temperature
avg_humidity
avg_pressure
avg_battery_level
```

The Beam `AggregateTelemetry` combine function maintains:

* event count
* temperature sum/count/min/max
* humidity sum/count
* pressure sum/count
* battery sum/count

and derives the corresponding averages.

---

# Event-Time Processing

The pipeline uses the timestamp generated by the telemetry event rather than Dataflow processing time.

The event timestamp is parsed and converted into a Beam `TimestampedValue`.

This matters because:

```text
event_time != processing_time
```

For IoT devices, a device may generate an event at:

```text
10:00:05
```

but network delays could cause Dataflow to receive it at:

```text
10:00:17
```

Analytics should generally represent the event according to when it happened, not simply when it reached the processing system.

---

# Windowing

The Gold layer uses fixed 60-second event-time windows.

Conceptually:

```text
10:00:00 ─────────────── 10:01:00
             Window 1

10:01:00 ─────────────── 10:02:00
             Window 2
```

The pipeline also configures allowed lateness and a late-data trigger.

This provides a balance between:

* low-latency aggregation
* out-of-order event handling
* bounded state retention

---

# DLQ Design

There are two major invalid-data paths:

```text
Malformed JSON
      |
      v
Parser invalid
      |
      +------+
             |
             v
            DLQ

Valid JSON
      |
      v
Validation
      |
      +---- invalid
             |
             v
            DLQ
```

The two invalid streams are flattened and written to the configured DLQ topic.

The production infrastructure uses:

```text
iot-telemetry-dlq
        |
        v
telemetry-dlq-sub
```

Terraform manages these Pub/Sub resources.

---

# Why a DLQ?

Invalid data should not block valid data.

Without a DLQ:

```text
bad message
    ↓
pipeline error
    ↓
reprocessing / operational instability
```

With a DLQ:

```text
bad message
    ↓
DLQ
    ↓
investigation / correction / replay
```

This keeps the primary processing path focused on valid telemetry.

---

# Observability

The project separates:

1. **Dataflow-native infrastructure metrics**
2. **Application-specific Beam metrics**

This avoids rebuilding platform-level monitoring inside application code.

---

# Application Metrics

The pipeline exposes metrics including:

### Validation

```text
telemetry_valid
telemetry_invalid
```

### Durable replay

```text
spanner_new
spanner_replay
spanner_conflict
```

### Spanner reliability

```text
spanner_retry_count
spanner_failure_count
```

### Processing latency

```text
telemetry_processing_latency_ms
```

The repository derives rates from counters rather than maintaining separate rate counters.

---

# Monitoring Thresholds

The repository defines initial operational thresholds:

| Signal                 |          Threshold | Severity         |
| ---------------------- | -----------------: | ---------------- |
| Invalid telemetry rate |               > 5% | Warning          |
| Replay rate            |               > 2% | Warning          |
| Payload conflict       |                > 0 | Critical         |
| Spanner failure        |                > 0 | Critical         |
| Spanner retry increase |          Sustained | Warning          |
| Processing latency P95 |           > 30 sec | Warning          |
| Dataflow system lag    | > 60 sec sustained | Critical/Warning |

These are explicitly documented as **initial portfolio thresholds**, not universal production SLOs. Real production values should be established from historical traffic and business requirements.

---

# Dataflow-Native Monitoring

Infrastructure-level signals are intentionally delegated to Dataflow/Cloud Monitoring:

* job state
* worker health
* throughput
* system lag
* backlog
* autoscaling
* resource utilization

Application metrics remain focused on behavior Dataflow cannot infer directly:

* validation outcomes
* replay decisions
* payload conflicts
* Spanner retry/failure behavior
* event processing latency

This separation is documented in the repository's observability design.

---

# Infrastructure as Code

Terraform manages core GCP infrastructure.

Repository:

```text
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── pubsub.tf
├── bigquery.tf
├── spanner.tf
├── storage.tf
├── iam.tf
└── .terraform.lock.hcl
```

The repository intentionally keeps `.terraform.lock.hcl` under version control while excluding local Terraform state artifacts.

---

# Terraform State

Terraform uses a remote GCS backend rather than keeping the authoritative state only on a developer workstation.

The state bucket is:

```text
gs://iot-gcp-streaming-tf-state
```

with the Terraform state stored under:

```text
terraform/state
```

This provides a shared state location for infrastructure operations.

---

# Terraform-Managed Resources

The Terraform configuration covers resources including:

### Pub/Sub

* production telemetry topic
* production Dataflow subscription
* production DLQ topic
* DLQ subscription
* IAM bindings

### BigQuery

* datasets/tables required by the platform
* deployment metadata table

### Spanner

* `iot-streaming` instance
* `iot_registry` database
* Dataflow database IAM

### Cloud Storage

* Dataflow staging/temp bucket
* lifecycle/retention configuration
* worker permissions

### IAM

Dedicated service identities and role bindings are defined as infrastructure rather than relying entirely on manually configured console permissions.

---

# Deployment Metadata Model

The project maintains a BigQuery deployment registry:

```text
iot_silver.pipeline_deployments
```

The Terraform-managed schema contains:

```text
application
environment
deployment_id
job_name
job_id
commit_sha
deployed_at
status
previous_job_name
```

The table is partitioned by `deployed_at` and clustered by:

```text
application
environment
status
```

This table acts as deployment control-plane metadata rather than event-processing state.

---

# Deployment State Machine

The deployment lifecycle is:

```text
                    +-------------+
                    |  CANDIDATE  |
                    +------+------+
                           |
                      promotion
                           |
                           v
                    +-------------+
                    |    ACTIVE   |
                    +------+------+
                           |
                     new promotion
                           |
                           v
                    +-------------+
                    | SUPERSEDED |
                    +-------------+
```

A failed candidate can be marked:

```text
CANDIDATE → FAILED
```

The deployment identity tooling validates the invariant that there should be only one ACTIVE deployment.

---

# CI/CD Architecture

The project separates:

```text
CI
```

from:

```text
Candidate deployment
```

from:

```text
Production promotion
```

This is especially important for a continuously running streaming pipeline.

---

# CI

The GitHub Actions CI workflow validates:

* Python code
* tests
* Terraform
* monitoring configuration
* Spanner integration behavior

The objective is to catch application and infrastructure problems before candidate deployment.

---

# Candidate Deployment — `cd.yml`

The candidate deployment workflow is manually triggered.

The workflow:

1. Checks out the repository.
2. Authenticates to GCP using Workload Identity Federation.
3. Installs Python 3.11 and Dataflow dependencies.
4. Generates a unique candidate Dataflow job name.
5. Deploys the Beam pipeline to Dataflow.
6. Uses a dedicated candidate Pub/Sub subscription.
7. Uses a candidate DLQ.
8. Captures the immutable Dataflow job ID.
9. Verifies the candidate reaches `JOB_STATE_RUNNING`.
10. Runs candidate validation.
11. Records the deployment as `CANDIDATE`.

The actual repository configuration uses:

```text
iot-telemetry-stream-candidate-${GITHUB_RUN_ID}
```

for candidate job naming and:

```text
dataflow-cd-test-sub
```

for candidate ingestion.

---

# Why Candidate Uses a Separate Subscription

The candidate does **not** consume the production Pub/Sub subscription.

Candidate:

```text
iot-telemetry-cd-test
        |
        v
dataflow-cd-test-sub
        |
        v
Candidate Dataflow
```

Production:

```text
iot-telemetry
        |
        v
dataflow-telemetry-sub
        |
        v
Production Dataflow
```

This isolation prevents candidate deployment from competing with the production pipeline for the same subscription.

---

# Why `WAIT_FOR_PIPELINE=false` Is Used in CD

A streaming Dataflow job is expected to run indefinitely.

The pipeline therefore supports:

```text
WAIT_FOR_PIPELINE
```

When running locally, the default behavior waits for the Dataflow result.

During CI/CD candidate deployment:

```text
WAIT_FOR_PIPELINE=false
```

allows the GitHub Actions process to return after submitting the streaming job.

The workflow can then obtain and validate the Dataflow job independently. The pipeline currently calls `pipeline.run()` and only calls `wait_until_finish()` when the environment variable is enabled.

---

# Production Promotion — `promote.yml`

Promotion is intentionally separate from candidate deployment.

`promote.yml` is manually triggered and receives:

```text
deployment_id
job_name
job_id
commit_sha
```

It then calls:

```text
scripts/deployment_identity.py --promote
```

The deployment identity logic verifies the supplied Dataflow identity before promoting the deployment.

The workflow transitions:

```text
CANDIDATE → ACTIVE
```

and the previous ACTIVE deployment becomes:

```text
ACTIVE → SUPERSEDED
```

The actual workflow inputs and promotion command are defined in `.github/workflows/promote.yml`.

---

# Why Separate `cd.yml` and `promote.yml`?

The project deliberately implements:

```text
Deploy
  ↓
Validate
  ↓
Approve
  ↓
Promote
```

instead of:

```text
Git push
  ↓
Production
```

For a streaming pipeline, this reduces the blast radius of a bad deployment.

A candidate can run and be validated without immediately becoming the production deployment.

---

# Workload Identity Federation

GitHub Actions authenticates to GCP through:

```text
GitHub OIDC
      |
      v
Workload Identity Federation
      |
      v
iot-cicd-deployer
```

No long-lived GCP service-account JSON key is required by the workflows.

The deployment workflow uses the configured WIF provider and the dedicated:

```text
iot-cicd-deployer@iot-gcp-streaming.iam.gserviceaccount.com
```

identity.

---

# Service Account Separation

The project distinguishes between:

## CI/CD identity

```text
iot-cicd-deployer
```

Responsible for deployment/control-plane operations.

## Dataflow worker identity

```text
iot-dataflow-worker
```

Responsible for runtime access from Dataflow workers.

This follows the principle of separating deployment permissions from runtime permissions.

---

# Production Cutover

The production cutover process follows:

```text
1. Validate candidate
        |
2. Protect recovery path
        |
3. Stop previous production job
        |
4. Start replacement production job
        |
5. Verify Dataflow RUNNING
        |
6. Verify production Pub/Sub subscription
        |
7. Send controlled test event
        |
8. Validate Silver
        |
9. Validate Gold
       |
10. Reconcile deployment metadata
```

The deployment registry should ultimately point to the actual serving production Dataflow job.

---

# Rollback Strategy

Rollback is treated as a streaming-data problem, not simply an application redeployment.

The high-level procedure is:

```text
Current production
       |
       v
Detect critical problem
       |
       v
Protect/recover Pub/Sub data
       |
       v
Stop faulty Dataflow job
       |
       v
Identify previous known-good deployment
       |
       v
Deploy previous pipeline
       |
       v
Validate production data path
       |
       v
Update deployment metadata
```

The deployment registry's `previous_job_name` field provides part of the deployment lineage required for this process.

---

# Why Pub/Sub Recovery Matters During Rollback

A streaming job may have unprocessed or in-flight messages when a deployment is stopped.

Therefore rollback planning must consider:

* subscription backlog
* message retention
* snapshots/replay mechanisms
* event deduplication
* durable replay state

The Spanner registry provides additional protection against treating replayed events as new canonical events.

---

# Repository Structure

```text
iot-telemetry-gcp-streaming/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── cd.yml
│       ├── promote.yml
│       ├── terraform-ci.yml
│       └── test-gcp-wif.yml
│
├── config/
│   ├── pipeline_config.py
│   └── validation_config.py
│
├── dataflow/
│   ├── pipeline.py
│   ├── requirements.txt
│   │
│   └── transforms/
│       ├── parser.py
│       ├── validator.py
│       ├── fingerprint.py
│       ├── conflict_detection.py
│       ├── deduplication.py
│       ├── event_time.py
│       ├── windowing.py
│       ├── durable_replay.py
│       ├── replay_repository.py
│       ├── replay_registry.py
│       ├── replay_audit.py
│       ├── replay_conflict_audit.py
│       ├── replay_detection.py
│       ├── silver.py
│       ├── gold.py
│       └── observability.py
│
├── monitoring/
│   ├── production-dashboard.json
│   ├── alert-dataflow-lag.json
│   ├── alert-invalid-telemetry.json
│   ├── alert-payload-conflict.json
│   ├── alert-processing-latency.json
│   ├── alert-spanner-failure.json
│   ├── scripts/
│   └── templates/
│
├── publisher/
├── simulator/
├── scripts/
│   └── deployment_identity.py
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── iam.tf
│   ├── pubsub.tf
│   ├── bigquery.tf
│   ├── spanner.tf
│   ├── storage.tf
│   └── .terraform.lock.hcl
│
├── tests/
│
├── setup.py
├── .gitignore
└── README.md
```

The current repository structure contains these major application, infrastructure, monitoring, workflow, script, simulator/publisher, and test directories.

---

# Local Development

## Prerequisites

Install:

* Python 3.11
* Google Cloud CLI
* Terraform
* access to the GCP project
* appropriate GCP IAM permissions

Create a Python environment:

```bash
python -m venv .venv
```

Activate it and install dependencies:

```bash
pip install -r dataflow/requirements.txt
```

---

# Running Tests

Run:

```bash
pytest
```

The CI pipeline also performs compilation and integration-oriented validation.

---

# Running the Dataflow Pipeline

The pipeline uses:

```text
DataflowRunner
```

for GCP execution.

A production deployment requires environment configuration for the project and Pub/Sub subscription.

The pipeline's runtime configuration is sourced from the project configuration modules and environment variables.

---

# Production Dataflow Configuration

The production pipeline uses:

```text
Project:
iot-gcp-streaming

Region:
asia-south1

Production subscription:
dataflow-telemetry-sub

Production topic:
iot-telemetry

Production DLQ:
iot-telemetry-dlq

Dataflow worker service account:
iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com
```

Terraform manages the core Pub/Sub infrastructure and worker subscription permissions.

---

# Testing Strategy

The project uses multiple layers of testing.

## Unit Tests

Validate individual components such as:

* parsing
* validation
* fingerprinting
* conflict detection
* replay decisions

## Spanner Integration Tests

Validate the durable replay registry against a test Spanner database rather than relying only on mocks.

## Deployment Validation

Candidate Dataflow jobs are independently checked for:

* expected job identity
* correct Dataflow state
* successful startup

## End-to-End Validation

A controlled telemetry event is sent through:

```text
Pub/Sub
   ↓
Dataflow
   ↓
Silver
   ↓
Gold
```

The expected values are then checked at the analytical layer.

---

# Production Validation

A successful deployment is not defined merely as:

```text
Dataflow job submitted successfully
```

The stronger validation criterion is:

```text
Dataflow RUNNING
       +
production subscription correct
       +
controlled event accepted
       +
Silver record correct
       +
Gold aggregate correct
       +
deployment metadata correct
```

This is the validation model used for the production cutover.

---

# Key Engineering Trade-offs

## Pub/Sub + Dataflow instead of direct ingestion into BigQuery

Pub/Sub decouples producers from stream processing.

Dataflow provides:

* distributed processing
* event-time semantics
* windowing
* stateful processing
* scalable execution

---

## Dataflow instead of Dataproc/Spark

The workload is continuous streaming rather than primarily batch-oriented Spark processing.

Dataflow provides a managed Beam execution environment without requiring a persistent Spark cluster.

---

## Spanner instead of BigQuery for hot-path replay protection

Spanner is used for low-latency transactional event registration.

BigQuery remains the analytical store.

The separation is:

```text
Spanner
→ operational event identity state

BigQuery
→ analytical and audit state
```

---

## Beam state + Spanner instead of Spanner alone

The short-lived Beam state provides efficient local/window-level processing.

Spanner provides durable replay state.

This creates layered protection:

```text
Beam dedup
      +
Spanner durable registry
```

rather than forcing every duplicate decision to depend exclusively on the durable database.

---

## Event-time instead of processing-time windows

Processing-time windows are easier to implement but can produce analytically incorrect results when events arrive late.

IoT data often experiences network and device delays.

Therefore event time is used for the analytical window.

---

## DLQ instead of failing the pipeline

A malformed or invalid telemetry event should not prevent valid events from being processed.

The DLQ provides isolation between bad input and the healthy stream.

---

# Important Invariants

The system is designed around several invariants.

## Deployment invariant

```text
Exactly one ACTIVE deployment
```

## Replay invariant

```text
event_id + same payload hash
→ replay
```

## Conflict invariant

```text
event_id + different payload hash
→ conflict
```

## Analytics invariant

Gold is aggregated by:

```text
device_id + event-time window
```

## Environment isolation invariant

```text
candidate subscription != production subscription
```

---

# Interview Discussion Points

This project is intentionally designed to support deeper Data Engineering interviews.

Important questions include:

### Streaming Architecture

* Why Pub/Sub?
* Why Dataflow?
* Why not Kafka?
* Why not Spark Structured Streaming?
* How would you scale to millions of events per second?

### Data Quality

* What happens with malformed JSON?
* What happens with invalid sensor ranges?
* Why use a DLQ?
* How would you replay corrected DLQ messages?

### Exactly-Once / Duplicates

* Does Pub/Sub guarantee exactly-once processing?
* Why do you need deduplication?
* Why is event ID insufficient?
* Why use payload hashes?
* Why use both Beam state and Spanner?

### Event Time

* Why event time instead of processing time?
* What happens with late events?
* What is allowed lateness?
* What happens if an event arrives after the allowed lateness period?

### Spanner

* Why Spanner?
* What happens if Spanner is temporarily unavailable?
* How are retries handled?
* What happens when two workers register the same event simultaneously?

### Data Modeling

* What is the grain of Silver?
* What is the grain of Gold?
* Why is Gold keyed by device and window?
* Why are replay and conflict tables separate from the analytical fact?

### CI/CD

* Why separate candidate deployment and promotion?
* Why doesn't the candidate consume production Pub/Sub?
* Why use Workload Identity Federation?
* Why capture the immutable Dataflow job ID?
* How would you roll back a streaming deployment?

### Observability

* Which metrics come from Dataflow?
* Which metrics are custom application metrics?
* What does a high replay rate mean?
* Why is a payload conflict critical?
* Why is processing latency a distribution metric?

---

# Production Deployment Model

The intended deployment model is:

```text
Developer
   |
   v
GitHub
   |
   v
CI
   |
   +-- tests
   +-- validation
   +-- Terraform validation
   +-- monitoring validation
   |
   v
Candidate CD
   |
   v
Isolated Dataflow
   |
   v
Candidate Pub/Sub subscription
   |
   v
Candidate validation
   |
   v
CANDIDATE
   |
   v
Manual approval
   |
   v
promote.yml
   |
   v
ACTIVE
   |
   v
Production Dataflow
```

This separates:

> **"Can this release run?"**

from:

> **"Should this release become production?"**

---

# Current Deployment State

The current deployment registry represents:

```text
02d42e8
ACTIVE
iot-telemetry-stream-02d42e8
```

with the previous:

```text
v53
SUPERSEDED
iot-telemetry-stream-v53
```

The ACTIVE deployment metadata was reconciled to the actual production Dataflow job after the production cutover.

This distinction is important:

> The deployment registry identifies the deployment considered ACTIVE; the actual Dataflow job and production Pub/Sub subscription determine the serving data-plane state.

---

# Future Improvements

Potential next-stage improvements include:

1. Automated production smoke tests after promotion.
2. Automated rollback orchestration.
3. Explicit environment-specific Terraform variables.
4. More extensive load testing.
5. Higher-throughput performance benchmarks.
6. Automated DLQ replay tooling.
7. Schema versioning for telemetry events.
8. Pub/Sub message ordering where required by business semantics.
9. More formal SLOs based on historical traffic.
10. Multi-region disaster recovery.
11. Automated canary analysis before promotion.
12. More granular deployment health checks based on application metrics.
13. Separate staging and production GCP projects for stronger environment isolation.
14. Automated reconciliation between deployment metadata and running Dataflow jobs.

---

# Design Philosophy

The central design principle of this project is:

> **A streaming pipeline is not just an ingestion script. It is a distributed system with data-quality, state-management, temporal, operational, deployment, and recovery concerns.**

The architecture therefore separates responsibilities:

```text
Pub/Sub
→ transport

Dataflow / Beam
→ distributed stream processing

Beam state
→ short-lived processing state

Spanner
→ durable event identity / replay state

BigQuery
→ analytical and audit storage

Cloud Monitoring
→ operational observability

Terraform
→ infrastructure lifecycle

GitHub Actions
→ CI/CD

Deployment registry
→ deployment control-plane state
```

This separation makes each component responsible for the problem it is best suited to solve.
