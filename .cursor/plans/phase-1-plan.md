---
name: Phase 1 Tekton Pipeline
overview: Implement the Phase 1 Tekton pipeline that ingests raw epidemiological events from Kafka, validates their schema, enriches them with metadata, and routes them to typed output topics—with dead-letter topics for rejects and routing failures, Tekton retries for transient task failures, and keyed produces for idempotency-aware downstream handling. Also fill in project documentation files.
todos:
  - id: tasks-yaml
    content: Create tekton/tasks.yaml with validate-events and route-and-enrich Tasks (kafka-python-ng), DLQ produces, and keyed messages where event_id exists
    status: pending
  - id: pipeline-yaml
    content: Create tekton/pipeline.yaml chaining validate -> route-and-enrich with runAfter, extra params for DLQ topics, and PipelineTask retries
    status: pending
  - id: pipelinerun-yaml
    content: Create tekton/pipelinerun.yaml with team02 param values (including validate_dlq and routing_dlq topics)
    status: pending
  - id: tekton-readme
    content: Create tekton/README.md documenting pipeline design, deploy commands, verification, and challenges
    status: pending
  - id: architecture-md
    content: Fill in docs/architecture.md with system overview, data flow, and technology choices
    status: pending
  - id: genai-usage-md
    content: Update docs/genai-usage.md to log AI tool usage for Phase 1
    status: pending
isProject: false
---

# Phase 1: Tekton Pipeline Implementation Plan

## Context

The repo is a scaffold with empty `tekton/`, `k8s/`, and `spark/` directories (only `.gitkeep` files). All Tekton manifests and documentation need to be created from scratch. The team number is **02**, so all topic names use `team02`.

**Kafka topics:**

- Input: `ds551-s26.team02.raw`
- Validated: `ds551-s26.team02.validated`
- Symptom reports: `ds551-s26.team02.symptom_reports`
- Clinic visits: `ds551-s26.team02.clinic_visits`
- Environmental: `ds551-s26.team02.environmental`
- **Validate dead letter:** `ds551-s26.team02.validate_dlq` — invalid or unparseable raw events (retained for inspection / replay)
- **Routing dead letter:** `ds551-s26.team02.routing_dlq` — validated events that fail produce, or have an unexpected `event_type` after validation

Ensure these topics exist in the cluster (same convention as other `team02` topics) before relying on them in production runs.

**Bootstrap server:** `my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092`

---

## Data Flow

```mermaid
flowchart LR
    Generator["Event Generator"] -->|JSON events| RawTopic["Kafka: team02.raw"]
    RawTopic --> Validate["Task: validate-events"]
    Validate -->|valid events| ValidatedTopic["Kafka: team02.validated"]
    Validate -->|invalid / unparseable| ValidateDLQ["Kafka: team02.validate_dlq"]
    ValidatedTopic --> RouteEnrich["Task: route-and-enrich"]
    RouteEnrich -->|symptom_report| SymTopic["Kafka: team02.symptom_reports"]
    RouteEnrich -->|clinic_visit| ClinicTopic["Kafka: team02.clinic_visits"]
    RouteEnrich -->|environmental_conditions| EnvTopic["Kafka: team02.environmental"]
    RouteEnrich -->|produce failure / bad type| RoutingDLQ["Kafka: team02.routing_dlq"]
```



---

## Reliability (addresses Bistro-style gaps)

| Risk | Mitigation in this plan |
|------|-------------------------|
| **Retry-missing** | Set **PipelineTask `retries`** (e.g. `2`–`3`) on both `validate` and `route-and-enrich` in `pipeline.yaml` so transient pod/network/Kafka failures re-run the task before failing the whole `PipelineRun`. |
| **Dead-letter-missing (validation)** | **Produce** failed records to **`validate_dlq`** with a small envelope: `original_value` (raw bytes or base64 if needed), `failure_reason`, `failed_at` (ISO 8601). Still log for immediate visibility. |
| **Dead-letter-missing (routing)** | On **producer exception** or **`event_type` not in the three allowed values**, **produce** to **`routing_dlq`** with the same style envelope plus optional `event_id` when present. Do not silently skip. |
| **Idempotency / duplicates** | Use **`event_id` as the Kafka record key** when producing to `validated`, typed topics, and DLQs (when `event_id` exists; for totally broken payloads use a synthetic key or `None` only for `validate_dlq`). **Note:** keys improve **partition affinity** and help **downstream deduplication** if consumers implement it; they do **not** alone guarantee exactly-once across pipeline reruns—document that **replays can still duplicate** until a later phase adds transactional/idempotent semantics or a dedup store. |

---

## File 1: `tekton/tasks.yaml`

Two Tekton Tasks in a single multi-document YAML file (separated by `---`).

### Task 1: `validate-events`

- **Params:** `kafka-bootstrap`, `input-topic`, `output-topic`, `validate-dlq-topic`, `batch-size`
- **Image:** `python:3.11-slim`
- **Logic (Python, using `kafka-python-ng`):**
  1. Create a `KafkaConsumer` on `input-topic` with a consumer group (e.g., `team02-validate-group`), `auto_offset_reset='earliest'`, and a reasonable `consumer_timeout_ms` (e.g., 30000ms so the task eventually exits after idle).
  2. Create a `KafkaProducer` targeting the bootstrap server.
  3. For each consumed message (up to `batch-size`):
    - Parse JSON.
    - **Validate required fields:** `event_id`, `event_type`, `patient_id`, `timestamp` must all be present.
    - **Validate `event_type`:** must be one of `symptom_report`, `clinic_visit`, `environmental_conditions`.
    - **Validate `timestamp`:** must be parseable as ISO 8601 (use `datetime.fromisoformat`).
    - If valid: produce to `output-topic` with **message key** = string `event_id` when publishing.
    - If invalid or JSON parse fails: **log** the reason, then **produce** a DLQ record to `validate-dlq-topic` (envelope with failure metadata and raw payload); use key = `event_id` if it was extractable from partial parse, else a stable placeholder per message (e.g. hash of raw bytes) so ordering is still defined.
  4. Flush producer, close consumer, print summary counts (valid / dlq / errors).

### Task 2: `route-and-enrich`

- **Params:** `kafka-bootstrap`, `input-topic`, `symptom-topic`, `clinic-topic`, `environmental-topic`, `routing-dlq-topic`, `team-id`, `batch-size`
- **Image:** `python:3.11-slim`
- **Logic (Python, using `kafka-python-ng`):**
  1. Create a `KafkaConsumer` on `input-topic` (validated topic) with its own consumer group, `auto_offset_reset='earliest'`, and `consumer_timeout_ms`.
  2. Create a `KafkaProducer`.
  3. For each consumed message (up to `batch-size`):
    - Parse JSON.
    - **Add enrichment fields:**
      - `team_id`: value of the `team-id` param (`"team02"`)
      - `processing_timestamp`: current UTC time in ISO 8601 (`datetime.utcnow().isoformat() + "Z"`)
      - `event_source`: `"ds551-event-generator"`
    - Map `event_type` to a target topic: `symptom_report` → `symptom-topic`, `clinic_visit` → `clinic-topic`, `environmental_conditions` → `environmental-topic`.
    - **If `event_type` is unknown:** produce to `routing-dlq-topic` with an envelope (`unknown_event_type`, payload, `event_id` if present); **continue** to the next message.
    - **If known type:** try to produce the enriched record to the target topic with **key** = string `event_id`. **If produce raises:** log, produce a failure envelope to `routing-dlq-topic`, then **continue** the batch (document this choice in README so operators know offsets still advance).
  4. Flush producer, close consumer, print summary counts.

**Key implementation details:**

- Use `value_serializer=lambda v: json.dumps(v).encode('utf-8')` in the producer and `value_deserializer=lambda m: json.loads(m.decode('utf-8'))` in the consumer for clean JSON handling.
- For keyed produces, pass `key=k.encode('utf-8')` (or the producer’s key serializer) when `event_id` is available.
- Params are accessed in the script via Tekton's `$(params.param-name)` substitution syntax, injected directly into the Python code as string literals.

---

## File 2: `tekton/pipeline.yaml`

- **Name:** `phase1-pipeline`
- **Params:** `kafka-bootstrap`, `input-topic`, `validated-topic`, `validate-dlq-topic`, `symptom-topic`, `clinic-topic`, `environmental-topic`, `routing-dlq-topic`, `team-id`, `batch-size`
- **Tasks:**
  1. `validate` — references `validate-events` task; **`retries: 2`** (or `3`) on the pipeline task; passes `kafka-bootstrap`, `input-topic`, `output-topic` (bound to `validated-topic`), `validate-dlq-topic`, `batch-size`
  2. `route-and-enrich` — references `route-and-enrich` task with `runAfter: [validate]`; **`retries: 2`** (or `3`); passes `kafka-bootstrap`, `input-topic` (bound to `validated-topic`), all three typed topics, `routing-dlq-topic`, `team-id`, `batch-size`

---

## File 3: `tekton/pipelinerun.yaml`

- **Name:** `phase1-run`
- **pipelineRef:** `phase1-pipeline`
- **Param values:**
  - `kafka-bootstrap`: `my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092`
  - `input-topic`: `ds551-s26.team02.raw`
  - `validated-topic`: `ds551-s26.team02.validated`
  - `validate-dlq-topic`: `ds551-s26.team02.validate_dlq`
  - `symptom-topic`: `ds551-s26.team02.symptom_reports`
  - `clinic-topic`: `ds551-s26.team02.clinic_visits`
  - `environmental-topic`: `ds551-s26.team02.environmental`
  - `routing-dlq-topic`: `ds551-s26.team02.routing_dlq`
  - `team-id`: `team02`
  - `batch-size`: `"100"` (string, reasonable default)

---

## File 4: `tekton/README.md`

Document:

1. **Pipeline design** — two-stage pipeline (validate then route-and-enrich), why this decomposition, how params flow, **DLQ topics**, **Tekton retries**, and **Kafka keys** (`event_id`)
2. **Deploy commands** — the exact `oc apply` and monitoring commands
3. **Verification** — how to use `kafka-console-consumer` to confirm events land in typed topics **and** how to spot records in `validate_dlq` / `routing_dlq` (referencing the command from [docs/kafka-tips.md](docs/kafka-tips.md))
4. **Reliability tradeoffs** — at-least-once behavior, what retries do and do not fix, and remaining duplicate risk on full replays
5. **Challenges and solutions** — placeholder section to fill in after deployment

---

## Documentation Updates

- **[docs/architecture.md](docs/architecture.md):** Fill in system overview, data flow, component descriptions, technology choices (Kafka topic design including **DLQs**, Tekton task design with **retries**), **message keys / idempotency posture**, and deployment info for Phase 1. Add a high-level note about planned database and analytics for later phases.
- **[docs/genai-usage.md](docs/genai-usage.md):** Log the AI usage for Phase 1 development.

