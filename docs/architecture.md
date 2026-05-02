# Architecture (Consolidated)

This document is the team’s **architecture reference** for the repository as it exists today: Kafka ingestion (Phase 1), durable raw landing (`raw_typed_events`), hourly fact generation in Spark, SQL serving views in TimescaleDB, and Phase 3 consumers (dashboard, ML scoring, alerting).

If you want step-by-step cluster operation (sequencing, quotas, long runs), treat [`continuous-collection-plan.md`](continuous-collection-plan.md) as the runbook and this page as the map.

## System overview

The platform is a batch-oriented analytics pipeline built around **OpenShift/Kubernetes**, **Kafka**, **Tekton**, **Apache Spark**, and **TimescaleDB/PostgreSQL**.

At a high level:

1. Events arrive on a **raw Kafka topic**.
2. **Tekton** validates and routes events to **typed Kafka topics**.
3. A small **ingester** process can persist typed events into **`raw_typed_events`** for durable, idempotent storage.
4. **Spark** reads either **`raw_typed_events`** (preferred when JDBC is enabled) or **typed Kafka topics** (alternate), then writes **`feature_*_hourly`** hourly fact tables.
5. **SQL views** implement **Layer B** metrics and ML feature views on top of hourly facts.
6. Applications read Layer B (and a small set of Phase 3 tables) for visualization and automation.

## Data flow diagram

```text
Kafka raw
  -> Tekton validate-events
  -> Kafka validated
  -> Tekton route-and-enrich
  -> Kafka typed topics
        -> (optional) ingest-typed-events-to-db.py
        -> TimescaleDB raw_typed_events
  -> Spark etl.py
        -> TimescaleDB feature_*_hourly  (Layer A)
        -> (SQL) v_latest_feature_* + analytic_* + vw_region_hourly_ml_features*
  -> Consumers:
        - Streamlit dashboard (app/)
        - ML scoring (ml/predict.py -> ml_outbreak_predictions)
        - Alert service (alert/alert_service.py -> alert_events)
```

## Phase 1: Tekton ingestion (Kafka → Kafka)

**Goal:** turn a heterogeneous raw stream into **typed streams** with basic schema validation and enrichment.

**Implementation:** `tekton/tasks.yaml` defines:

- `validate-events`: consumes raw, validates required fields, emits validated stream, can emit validate DLQ envelopes
- `route-and-enrich`: consumes validated, enriches metadata, routes by `event_type` to typed topics, can emit routing DLQ envelopes

**Orchestration:** `tekton/pipeline.yaml` runs validate then route (`runAfter`), with retries.

**Important behavioral note:** unknown `event_type` values are **silent-dropped** in routing (by design in the task script). Invalid payloads may go to DLQ topics when configured.

## Raw landing: typed Kafka → `raw_typed_events`

**Goal:** provide a **durable, deduped** store of typed messages keyed by Kafka `(topic, partition, offset)` so Spark can use **event-time SQL windows** against stable DB state.

**Implementation:** `scripts/ingest-typed-events-to-db.py`

**DDL:** created via `infra/database-deployment.yaml` and/or `scripts/apply-raw-landing-ddl.sh` (see notes in [`continuous-collection-plan.md`](continuous-collection-plan.md) about existing PVCs not re-running init SQL).

This table is primarily an **input buffer / source of truth for replay**, not a dashboard serving layer.

## Phase 2/3 Spark ETL: raw events → `feature_*_hourly` (Layer A)

**Goal:** compute **canonical hourly aggregates** (“features” in the data-engineering sense) for downstream SQL views and ML.

**Implementation:** `spark/etl.py`, typically executed as Kubernetes Job `epidemic-spark-etl` (`infra/spark-job.yaml`).

**Outputs:** append-only fact rows into Timescale hypertables such as:

- `feature_region_hourly`
- `feature_clinic_hourly`
- `feature_hospital_hourly`
- `feature_diagnosis_hourly`
- `feature_symptom_hourly`
- `feature_vaccination_hourly`

**Spark input modes (`FEATURE_SOURCE_MODE`):**

- `raw_db`: reads `raw_typed_events` for the configured UTC window
- `kafka`: reads typed Kafka topics directly

When `FEATURE_SOURCE_MODE` is unset, `spark/etl.py` defaults to `raw_db` for normal JDBC runs, and `kafka` for `SKIP_JDBC_WRITE=true` dry runs (because `raw_db` requires JDBC reads). See [`spark/README.md`](../spark/README.md).

## TimescaleDB Layer B: SQL views and ML feature views

**Goal:** define stable, consumer-friendly relations for dashboards and training/inference jobs without pushing presentation logic into Spark.

**Definitions:** `infra/database-init.sql` (fresh DB init) and `infra/database-deployment.yaml` (cluster manifest DDL; may include additional operational views like batch-run helpers).

**Representative Layer B objects:**

- `v_latest_feature_*`: pick latest batch semantics per bucket key
- `analytic_*`: rubric/dashboard metrics (event volume, regional severity trend, hospital pressure, follow-up rate, diagnosis temperature, vaccination volume, etc.)
- `vw_region_hourly_ml_features` and `vw_region_hourly_ml_features_windowed`: ML feature projections (windowed aggregates / lags)

Layer B is intentionally **SQL**, not Spark, in this project.

## Phase 3 consumers

### Dashboard

**Implementation:** `app/dashboard.py` + `app/sql_queries.py`

**Reads:** primarily `analytic_*` views, plus `ml_outbreak_predictions` and `alert_events` for detector and alerting pages.

### ML scoring

**Implementation:** `ml/predict.py` (batch job)

**Reads:** `vw_region_hourly_ml_features_windowed`

**Writes:** `ml_outbreak_predictions`

### Alerting

**Implementation:** `alert/alert_service.py`

**Reads:** `vw_region_hourly_ml_features_windowed` (and related context)

**Writes:** `alert_events`

## Technology choices (why this split exists)

- **Kafka + Tekton** for ingestion lets the team decouple “event movement” from compute, and keep failure handling (DLQs, retries) at the edge.
- **Spark** for Layer A rollups keeps JSON parsing, multi-topic unioning, and large batch aggregation in a compute engine suited to batch windows.
- **Timescale/Postgres** for Layer B keeps serving queries simple and lets you iterate on metric definitions with SQL views.
- **`raw_typed_events`** adds durability and makes “replay this event-time hour” an SQL predicate instead of a Kafka offset archaeology exercise.

## Deployment (where manifests live)

Common OpenShift manifests and DDL live under `infra/` (database, Spark job, RBAC, optional loadgen). Tekton lives under `tekton/`. Runtime orchestration scripts live under `scripts/`.

## Pointers to deeper docs

- [`phase3-architecture.md`](phase3-architecture.md)
- [`phase3-dataflow.md`](phase3-dataflow.md)
- [`continuous-collection-plan.md`](continuous-collection-plan.md)
- [`testing-plan.md`](testing-plan.md)
- [`spark/README.md`](../spark/README.md)
- [`visualization-guide.md`](visualization-guide.md)
- [`alerting-guide.md`](alerting-guide.md)
- [`ml-model-guide.md`](ml-model-guide.md)
