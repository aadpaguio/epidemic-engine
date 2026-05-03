# Phase 3 Data Flow

This document is the **short, end-to-end mental model** for how data moves through the system in Phase 3. For operational commands, sequencing, and quota-safe runners, see [`runbook.md`](runbook.md). For Spark configuration details, see [`spark/README.md`](../spark/README.md).

## End-to-end system flow (logical)

```text
Event Generator (infra namespace)
  ↓
Kafka raw topic (example): ds551-s26.team02.raw
  ↓
Tekton: validate-events
  ↓
Kafka validated topic (example): ds551-s26.team02.validated
  ↓
Tekton: route-and-enrich
  ↓
Kafka typed topics (examples):
  - ds551-s26.team02.symptom_reports
  - ds551-s26.team02.clinic_visits
  - ds551-s26.team02.hospital_admissions
  - ds551-s26.team02.vaccination_records
  ↓
(Optional but recommended) Raw landing ingester: scripts/ingest-typed-events-to-db.py
  ↓
TimescaleDB/Postgres: raw_typed_events  (durable copy + Kafka offset dedupe)
  ↓
Spark batch job: epidemic-spark-etl (spark/etl.py)
  - Preferred/default path when JDBC is enabled: FEATURE_SOURCE_MODE=raw_db (reads raw_typed_events)
  - Alternate path: FEATURE_SOURCE_MODE=kafka (reads typed Kafka topics directly)
  ↓
TimescaleDB Layer A: feature_*_hourly hypertables (Spark-written hourly facts)
  ↓
TimescaleDB Layer B: SQL views
  - v_latest_feature_* (latest successful batch per natural key; definition differs slightly between init vs deployment DDL)
  - analytic_* (dashboard-facing metrics)
  - vw_region_hourly_ml_features* (ML feature views built on regional analytics)
  ↓
Phase 3 consumers:
  - Streamlit dashboard (app/dashboard.py reads analytic_* + ml_outbreak_predictions + alert_events)
  - Outbreak prediction batch job (ml/predict.py reads vw_region_hourly_ml_features_windowed; writes ml_outbreak_predictions)
  - Alert service (epidemic-alert-service / alert/alert_service.py): reads raw_typed_events (metric rules),
    ml_outbreak_predictions, vw_region_hourly_ml_features_windowed; writes alert_events; emits structured [ALERT] logs;
    optional Slack notifications (#epidemic-alerts via incoming webhook)
```

## Layer A vs Layer B (what runs where)

- **Layer A (`feature_*_hourly`)**: materialized **hourly fact tables** produced by **Spark** from event-time windows. Spark is responsible for parsing, bucketing, and aggregating into hourly rollups.
- **Layer B (`v_latest_feature_*`, `analytic_*`, ML views)**: **SQL serving logic** in Timescale/Postgres. It selects the latest batch semantics and exposes stable “metric views” for apps and ML.

`raw_typed_events` is **not Layer B**. It is an optional **durable staging table** so Spark can read a stable DB snapshot for a window (`raw_db` mode) instead of re-scanning Kafka.

## Two valid Spark input modes (not two different analytics models)

The **downstream analytics model stays the same**: Spark still writes `feature_*_hourly`, and Layer B still reads those hourly facts.

What changes is only the **Spark input**:

- **`raw_db`**: read `raw_typed_events` for `[WINDOW_START_UTC, WINDOW_END_UTC)` and compute the same hourly facts.
- **`kafka`**: read typed topics for the job’s offset range / batch read semantics and compute the same hourly facts.

In `spark/etl.py`, when `FEATURE_SOURCE_MODE` is unset:

- JDBC enabled (normal runs): defaults to **`raw_db`**
- `SKIP_JDBC_WRITE=true` (dry runs): defaults to **`kafka`** (because `raw_db` requires JDBC reads)

## Related docs

- [`runbook.md`](runbook.md): recommended sequenced operation, dirty-window refresh, and raw landing DDL notes
- [`phase3-architecture.md`](phase3-architecture.md): Phase 3 system framing
- [`architecture.md`](architecture.md): consolidated architecture reference for the repo
- [`testing-plan.md`](testing-plan.md): canonical smoke expectations for the raw-db path
