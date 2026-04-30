# Team [02] — Epidemic Engine

**DS-551 Data Engineering at Scale — Spring 2026**

---

## Project Overview

A cloud-native data platform for processing synthetic, funny epidemiological events. 

Events are validated, then enriched and routed, so that analytics can be performed (in a later phase).

---

## Team Members

- [Aidan Smith] — [Pipelinerun, design/planning]
- [Arnald Paguio] — [Task definitions]
- [Evan Ferguson-Hull] — [Logging cleanup and README]
- [Jason Nguyen] — [Pipeline development]

---

## Architecture

The Phase 1 Tekton pipeline ingests raw epidemiological events from Kafka, validates their schema, enriches them with metadata, and routes them to typed output topics—with dead-letter topics for rejects and routing failures, Tekton retries for transient task failures, and keyed produces for idempotency-aware downstream handling.

**Change order CO-2026-01:** `vaccination_record` events route to `ds551-s26.team02.vaccination_records`; Spark writes `feature_vaccination_hourly`. See [`docs/change-order-vaccination.md`](docs/change-order-vaccination.md).

See `docs/architecture.md` for detailed architecture documentation.

---

## Quick Start

Use `INSTRUCTIONS.md` as the course-staff requirements document.

### Current Active Scope

- Architecture Plan
- Phase 1 — Tekton Pipeline

Later project phases will be added to this same repository by course staff.

### Phase 1 Deployment

```bash
oc apply -f tekton/tasks.yaml -n <your-namespace>
oc apply -f tekton/pipeline.yaml -n <your-namespace>
oc apply -f tekton/pipelinerun.yaml -n <your-namespace>
oc get pipelinerun -n <your-namespace>
```

### Verification

Pipeline logs are dumped in a summary json that is human-readable, and contains both high-level summary results and detailed success and error records.

---

## Repository Structure

```text
├── README.md
├── INSTRUCTIONS.md
├── docs/
│   ├── architecture.md
│   ├── genai-usage.md
│   ├── kafka-tips.md
│   ├── spark-tips.md
│   └── tekton-tips.md
├── tekton/
│   └── pipeline.yaml
│   └── pipelinerun.yaml
│   └── tasks.yaml
├── spark/
│   └── [used later]
└── k8s/
    └── [used later]
```

Additional folders and files will be added later as the project grows.

---

## Phase Status

### Architecture Plan ⏳
- [x] Database choice documented
- [x] Planned analytics described
- [x] Team roles assigned

### Phase 1: Tekton Pipeline ⏳
- [x] `validate-events` task implemented
- [x] `route-and-enrich` task implemented
- [x] Pipeline runs successfully
- [x] Typed topics receiving events
- [x] `tekton/README.md` complete

---

## Documentation

- **Requirements:** `INSTRUCTIONS.md`
- **Architecture:** `docs/architecture.md`
- **GenAI Usage:** `docs/genai-usage.md`
- **Kafka Tips:** `docs/kafka-tips.md`
- **Spark Tips:** `docs/spark-tips.md`
- **Spark ETL (deploy / redeploy / test):** `spark/README.md` (includes OpenShift Job iteration commands)
- **Hourly Spark orchestration (external runner):** `scripts/run-hourly-spark-external.sh` (`scripts/run-hourly-spark-gated.sh` is a compatibility wrapper)
- **Local DB connection (DBeaver):** `docs/local-db-connection.md`
- **Phase 2 stress-test report (R5b):** `docs/phase2-stress-test.md`
- **Tekton Tips:** `docs/tekton-tips.md`
- **Phase 1 Pipeline Notes:** create `tekton/README.md` as part of your submission
