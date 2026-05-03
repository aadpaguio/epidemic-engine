# Team [02] — Epidemic Engine

**DS-551 Data Engineering at Scale — Spring 2026**

---

## Project overview

This repository implements a multi-phase, cloud-native pipeline for synthetic epidemiological events:

- **Phase 1 (Tekton)**: validate and route **Kafka** events from a **raw topic** to **typed topics**.
- **Raw landing (optional but recommended for stable batching)**: persist typed Kafka messages into TimescaleDB **`raw_typed_events`** for deduped, replay-friendly storage (`scripts/ingest-typed-events-to-db.py`).
- **Phase 2 (Spark ETL)**: compute **hourly fact tables** `feature_*_hourly` in TimescaleDB (`spark/etl.py`).
- **Layer B (SQL in TimescaleDB)**: `analytic_*` views (and ML feature views) built on hourly facts (`infra/database-init.sql`, `infra/database-deployment.yaml`).
- **Phase 3 (consumers)**: Streamlit dashboard (`app/`), ML scoring (`ml/`), alerting (`alert/`).

If you want one “how we run it for hours without CronJobs” guide, start with [`docs/runbook.md`](docs/runbook.md).

---

## Team members

- [Aidan Smith]
- [Arnald Paguio]
- [Evan Ferguson-Hull]
- [Jason Nguyen]

---

## Architecture (quick)

**Consolidated architecture reference:** [`docs/architecture.md`](docs/architecture.md)

**End-to-end Phase 3 flow (short):** [`docs/phase3-dataflow.md`](docs/phase3-dataflow.md)

**Change order CO-2026-01 (vaccinations):** [`docs/change-order-vaccination.md`](docs/change-order-vaccination.md)

At a glance:

```text
Kafka raw -> Tekton validate -> Kafka validated -> Tekton route -> Kafka typed topics
  -> (ingester) raw_typed_events -> Spark -> feature_*_hourly -> analytic_* views -> dashboard / ML / alerts
```

Spark can read **`raw_typed_events`** (`FEATURE_SOURCE_MODE=raw_db`, preferred when JDBC is enabled) or typed Kafka directly (`FEATURE_SOURCE_MODE=kafka`). See [`spark/README.md`](spark/README.md).

---

## Requirements / coursework pointers

- **Course requirements:** [`INSTRUCTIONS.md`](INSTRUCTIONS.md)
- **Phase instructions (as applicable):** [`PHASE1-LAB-GETTING-STARTED.md`](PHASE1-LAB-GETTING-STARTED.md), [`PHASE2-INSTRUCTIONS.md`](PHASE2-INSTRUCTIONS.md), [`PHASE3-INSTRUCTIONS.md`](PHASE3-INSTRUCTIONS.md)

---

## Quick starts

### Phase 1: Tekton pipeline (Kafka → Kafka)

```bash
oc apply -f tekton/tasks.yaml -n <your-namespace>
oc apply -f tekton/pipeline.yaml -n <your-namespace>
oc create -f tekton/pipelinerun.yaml -n <your-namespace>
oc get pipelinerun -n <your-namespace>
```

Notes: see [`tekton/README.md`](tekton/README.md) and [`docs/tekton-tips.md`](docs/tekton-tips.md).

### Phase 2/3: database + Spark ETL + raw landing (recommended sequencing)

This repo’s recommended “stable collection + refresh” mode is documented in [`docs/runbook.md`](docs/runbook.md), including:

- applying DB manifests / DDL (including `raw_typed_events`)
- running Phase 1 in a quota-safe loop
- running the ingester and Spark refresh (`FEATURE_SOURCE_MODE=raw_db`)

Spark job manifest: [`infra/spark-job.yaml`](infra/spark-job.yaml)

### Streamlit dashboard (local)

See [`docs/visualization-guide.md`](docs/visualization-guide.md).

---

## Repository structure (high level)

```text
├── app/                 # Streamlit dashboard + SQL queries
├── alert/               # Phase 3 alert service (Kubernetes manifests + service code)
├── docs/                # Architecture + runbooks + course docs
├── infra/               # OpenShift/Kubernetes manifests (DB, Spark job, loadgen, ...)
├── ml/                  # Training + batch prediction job
├── scripts/             # Orchestration helpers (Phase1+Spark, raw landing, smoke tests, ...)
├── spark/               # Phase 2 Spark ETL (`etl.py`) + Spark README
├── tekton/              # Phase 1 Tekton tasks + pipelines + pipelineruns
├── tests/               # Unit/integration tests (Spark, ingest, optional DB schema checks)
├── loadgen/             # Kafka load generator tooling
└── main.py              # Small repo entrypoint (if used by your environment)
```

---

## Documentation index (common)

- **Architecture (consolidated):** [`docs/architecture.md`](docs/architecture.md)
- **Phase 3 flow (short):** [`docs/phase3-dataflow.md`](docs/phase3-dataflow.md)
- **Timed continuous collection / raw landing runbook:** [`docs/runbook.md`](docs/runbook.md)
- **Testing Suite:** [`docs/testing-suite.md`](docs/testing-suite.md) — run everything with [`scripts/run-test-suite.sh`](scripts/run-test-suite.sh)
- **Spark ETL deploy + env vars:** [`spark/README.md`](spark/README.md)
- **ML guide:** [`docs/ml-model-guide.md`](docs/ml-model-guide.md)
- **Alerting guide:** [`docs/alerting-guide.md`](docs/alerting-guide.md)
- **Alert config / ops runbook:** [`docs/alert-config.md`](docs/alert-config.md)
- **Visualization / dashboard:** [`docs/visualization-guide.md`](docs/visualization-guide.md)
- **Kafka tips:** [`docs/kafka-tips.md`](docs/kafka-tips.md)
- **Spark tips:** [`docs/spark-tips.md`](docs/spark-tips.md)
- **Tekton tips:** [`docs/tekton-tips.md`](docs/tekton-tips.md)
- **Phase 2 stress testing writeup:** [`docs/phase2-stress-test.md`](docs/phase2-stress-test.md)

---

## Phase status (high level)

- **Phase 1:** Tekton ingestion pipeline (`tekton/`)
- **Phase 2:** Spark hourly facts + Timescale Layer A/B DDL (`spark/`, `infra/database-init.sql`, `infra/database-deployment.yaml`)
- **Phase 3:** Dashboard + ML + alerting (`app/`, `ml/`, `alert/`)
