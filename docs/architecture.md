# Architecture

This document describes the **Epidemic Engine** platform as implemented in this repository: OpenShift/Kubernetes, Kafka, Tekton, optional raw landing ingestion, Apache Spark batch ETL, TimescaleDB/PostgreSQL, and Phase 3 consumers (Streamlit dashboard, ML scoring, alerting).

For **operational sequencing** (long runs, quotas, `oc` loops), use `[runbook.md](runbook.md)`. This page is the **system map** and explains how **Change Order CO-2026-01** (`vaccination_record`) is wired at each stage.

---

## 1. End-to-end data flow

Events are **batch-oriented**: Tekton moves windows of messages; Spark runs as **Jobs**, not structured streaming; the database holds **durable** raw landing and **append-only** hourly facts.

```text
Course / generator
  -> Kafka: ds551-s26.team02.raw
       -> Tekton: validate-events
       -> Kafka: ds551-s26.team02.validated
       -> Tekton: route-and-enrich
       -> Kafka typed topics (symptom_reports, clinic_visits, hospital_admissions, vaccination_records)
             -> (optional) Pod: raw-typed-ingester / ingest-typed-events-to-db.py
             -> TimescaleDB: raw_typed_events
       -> Spark: spark/etl.py (Job epidemic-spark-etl)
       -> TimescaleDB: feature_*_hourly (Layer A), batch_window_runs
       -> SQL: v_latest_feature_*, analytic_* , vw_region_hourly_ml_features*
       -> Consumers:
            - Streamlit (app/) — analytic views, ML predictions, alerts
            - ml/predict.py — reads windowed ML features, writes ml_outbreak_predictions
            - alert/alert_service.py — reads ML features, writes alert_events
```

### Technology roles


| Layer           | Technology             | Role                                                                                                          |
| --------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------- |
| Ingestion edge  | Kafka + Tekton         | Validate, enrich metadata, split raw stream into typed topics; DLQs for bad records when configured           |
| Durable buffer  | `raw_typed_events`     | Idempotent landing keyed by `(topic, partition, offset)`; supports event-time replay and dirty-window refresh |
| Layer A compute | Spark (`spark/etl.py`) | Parse JSON, union topics or DB rows, bucket by hour, aggregate into hypertables                               |
| Layer A storage | TimescaleDB tables     | Append-only hourly facts per batch                                                                            |
| Layer B         | PostgreSQL views       | Latest-row semantics per bucket key, rubric analytics, ML feature projections                                 |
| Phase 3 UI      | Streamlit + `psycopg2` | Parameterized SQL against serving views; runs **locally** with DB via port-forward                            |


---

## 2. Phase 1 — Tekton (Kafka → Kafka)

**Goal:** Turn a heterogeneous **raw** topic into **validated** JSON and then **typed** topics with team metadata.

**Orchestration:** `tekton/pipeline.yaml` chains two tasks with `runAfter`: first `validate-events`, then `route-and-enrich`. **PipelineRun** parameters (see `tekton/pipelinerun.yaml`, `tekton/pipelinerun-continuous.yaml`) supply bootstrap address, topic names, batch size, consumer timeouts, and DLQ topic names.

### 2.1 Task: `validate-events` (`tekton/tasks.yaml`)

- **Consumes:** `input-topic` (team raw), e.g. `ds551-s26.team02.raw`.
- **Produces:** `output-topic` (`ds551-s26.team02.validated`) for records that pass validation.
- **Side behavior:** Assigns `event_id` = `evt-<uuid>` when missing. Logs `[PIPELINE]` JSON lines. If `validate-dlq-topic` is set, invalid payloads are wrapped and sent there.

**Base validation:** Every event must have non-empty `event_type`, `timestamp`, `region`; `timestamp` must be ISO-8601. If `schema_version` is present and non-empty, it must parse as integer **2** (legacy payloads may omit it entirely).

### 2.2 Task: `route-and-enrich` (`tekton/tasks.yaml`)

- **Consumes:** validated topic.
- **Produces:** One of four typed topics selected by `event_type`, after enrichment.

Enrichment adds:

- `team_id` (from PipelineRun param),
- `processing_timestamp` (UTC ISO),
- `event_source` = `ds551-event-generator`.

**Routing map:**

```text
symptom_report      -> symptom-topic
clinic_visit        -> clinic-topic
hospital_admission  -> hospital-topic
vaccination_record  -> vaccination-topic
```

Unknown `event_type` values are **not** sent to a DLQ; they are **silent-dropped** (counter increments only). Routing failures (e.g. produce error) can go to `routing-dlq-topic` when configured.

### 2.3 CO-2026-01 in Phase 1


| Step                 | What changed                                                                                                                                                                                                                                                                                                                                                                      |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **validate-events**  | For `event_type == "vaccination_record"`, require `schema_version`, `record_id`, `patient_id`, `vaccine_type`, `dose_number`, `administered_at`; `schema_version` must be **2**; `dose_number` must be integer-coercible; `administered_at` must be valid ISO-8601. Failures use codes such as `MISSING_FIELD:`*, `BAD_SCHEMA_VERSION`, `BAD_FIELD:dose_number`, `BAD_TIMESTAMP`. |
| **route-and-enrich** | New mapping `vaccination_record` → param `**vaccination-topic`** (default `ds551-s26.team02.vaccination_records`).                                                                                                                                                                                                                                                                |


Existing event types keep prior behavior; optional `schema_version` still must be `2` when present.

---

## 3. Ingester phase — typed Kafka → `raw_typed_events`

This phase is **not** part of the Tekton YAML; it is an optional **in-cluster** consumer that lands typed messages into Postgres for **durable** storage and **event-time** Spark windows. It is documented and orchestrated in `[runbook.md](runbook.md)` (canonical mode in §1, component-level procedures in §8).

### 3.1 Why it exists

- Kafka ordering is **not** the same as payload `timestamp` ordering; a poll can return mixed hours.
- `raw_typed_events` stores each message once, keyed by `(topic, kafka_partition, kafka_offset)`, so replays and “dirty hour” refresh are SQL-driven.
- The recommended loop **sequences** Phase 1 runs, then a bounded ingester pass, then Spark feature refresh (`scripts/run-phase1-rawdb-sequenced.sh`) to avoid overlapping pods and memory quota issues.

### 3.2 Implementation


| Artifact                               | Purpose                                                                                                  |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `scripts/ingest-typed-events-to-db.py` | Kafka consumer → INSERT into `raw_typed_events`                                                          |
| `scripts/run-raw-typed-ingester.sh`    | Creates ConfigMap + Pod `raw-typed-ingester` in the namespace; env `TOPICS` lists comma-separated topics |
| `scripts/run-rawdb-feature-refresh.sh` | Invokes Spark refresh for dirty event-time windows (uses `batch_window_runs`)                            |
| `scripts/apply-raw-landing-ddl.sh`     | Applies `raw_typed_events` DDL if the PVC was initialized before that table existed                      |


**Default topics on the ingester** include all four typed streams, including vaccination:

```text
ds551-s26.team02.symptom_reports,
ds551-s26.team02.clinic_visits,
ds551-s26.team02.hospital_admissions,
ds551-s26.team02.vaccination_records
```

Credentials come from Secret `epidemic-db-spark` (`username` / `password`). In-cluster Kafka bootstrap is typically `kafka:9092` (see ingester script).

### 3.3 `raw_typed_events` schema

Defined in `infra/database-init.sql` and `infra/database-deployment.yaml`:


| Column               | Type        | Description                                     |
| -------------------- | ----------- | ----------------------------------------------- |
| `topic`              | TEXT        | Kafka topic name                                |
| `kafka_partition`    | INT         | Partition                                       |
| `kafka_offset`       | BIGINT      | Offset                                          |
| `event_type`         | TEXT        | Parsed from payload when possible               |
| `event_ts`           | TIMESTAMPTZ | Event time extracted from payload for bucketing |
| `payload_json`       | JSONB       | Full enriched message                           |
| `run_id`, `batch_id` | TEXT        | Optional lineage                                |
| `ingested_at`        | TIMESTAMPTZ | Insert time                                     |


**Primary key:** `(topic, kafka_partition, kafka_offset)`.

### 3.4 CO-2026-01 in the ingester

Vaccination messages on `ds551-s26.team02.vaccination_records` are ingested like other typed topics: same table, `event_type` / JSON reflects `vaccination_record` fields (`vaccine_type`, `dose_number`, etc.). No separate table is required.

---

## 4. Spark ETL — Layer A (`spark/etl.py`)

**Goal:** Build canonical **hourly** fact rows (Layer A) for each event family, append by `batch_id`, and record window execution in `batch_window_runs`.

**Deployment:** Kubernetes Job `epidemic-spark-etl` (`infra/spark-job.yaml`), with `etl.py` supplied via ConfigMap `spark-etl-script`.

### 4.1 Input modes (`FEATURE_SOURCE_MODE`)


| Mode                                 | Reads                               | When used                                                                        |
| ------------------------------------ | ----------------------------------- | -------------------------------------------------------------------------------- |
| `**raw_db`** (default for JDBC runs) | `raw_typed_events` for a UTC window | Preferred for sequenced collection: Spark aligns buckets to **event time** in DB |
| `**kafka`**                          | Typed topics directly               | Alternate / dry runs; env such as `SKIP_JDBC_WRITE=true` may imply Kafka path    |


Environment variables set JDBC URL, topics, window bounds (`WINDOW_START_UTC` / `WINDOW_END_UTC` or dirty-window discovery), and topic names including `**TOPIC_VACCINATION`** (default `ds551-s26.team02.vaccination_records`).

### 4.2 Processing outline

1. Build or load a Spark session; register Kafka or JDBC sources per mode.
2. Parse JSON payloads; map event types to preliminary per-stream DataFrames (symptom, clinic, hospital, **vaccination**).
3. Bucket rows by hour (`bucket_start`).
4. Aggregate into DataFrames passed to `write_feature_table` for each target table.
5. Insert into Timescale hypertables and update `batch_window_runs` (`status`: running → succeeded/failed).

### 4.3 CO-2026-01 in Spark

- `**preliminary_vaccination_records`:** Filters vaccination topic / raw rows to `vaccination_record`, derives hourly buckets and keeps `region`, `vaccine_type`, `dose_number`, etc.
- `**build_feature_vaccination_hourly`:** Aggregates **counts** per `(bucket_start, region, vaccine_type, dose_number)` into `**feature_vaccination_hourly`**.
- Downstream SQL exposes `**v_latest_feature_vaccination_hourly`** and `**analytic_vaccination_volume_by_type`** for dashboards.

---

## 5. TimescaleDB schemas

DDL sources: `**infra/database-init.sql`** (fresh init) and `**infra/database-deployment.yaml**` (cluster ConfigMap). Some environments gate “latest” rows off `**batch_window_runs**`; the init file uses **DISTINCT ON** latest batch per natural key for several `v_latest_feature_*` views. Treat deployment YAML as authoritative for **your** cluster if it differs.

### 5.1 Layer A — hourly fact hypertables (Spark-written)

All share the pattern: `**bucket_start`** (UTC hour), `**batch_id`**, `**run_timestamp`**, plus type-specific measures. Primary keys always include `**batch_id**` so multiple Spark runs can coexist; Layer B picks the correct row for serving.


| Table                                             | Grain                                               | Key measures / dimensions                                                                                      |
| ------------------------------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `feature_region_hourly`                           | `(bucket_start, region)`                            | `symptom_count`, `admission_count`, `clinic_visit_count`, severity/temperature/duration aggregates, bed gauges |
| `feature_clinic_hourly`                           | `(bucket_start, clinic_id)`                         | `visit_count`, `follow_up_true_count`, temperature, bed gauges                                                 |
| `feature_hospital_hourly`                         | `(bucket_start, hospital_id)`                       | `admission_count`, severity, oxygen, LOS, bed gauges                                                           |
| `feature_diagnosis_hourly`                        | `(bucket_start, clinic_id, diagnosis_code)`         | `diagnosis_visit_count`, temperature aggregates                                                                |
| `feature_symptom_hourly`                          | `(bucket_start, region, symptom_token)`             | `symptom_token_count`                                                                                          |
| `**feature_vaccination_hourly**` **(CO-2026-01)** | `(bucket_start, region, vaccine_type, dose_number)` | `**vaccination_count`**                                                                                        |


### 5.2 Operational metadata

`**batch_window_runs`**


| Column                         | Purpose                            |
| ------------------------------ | ---------------------------------- |
| `window_start`, `window_end`   | UTC window Spark processed         |
| `run_id`, `batch_id`           | Job identifiers                    |
| `status`                       | `running` / `succeeded` / `failed` |
| `reason_code`, `reason_detail` | Diagnostics                        |
| `created_at`, `completed_at`   | Timing                             |


Used for gated “latest successful” semantics in some deployments and for orchestration logs.

### 5.3 Layer B — views (SQL-only)


| View                                                       | Purpose                                                                         |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `v_latest_feature_*`                                       | One serving row per bucket key (latest `run_timestamp` / batch semantics)       |
| `analytic_regional_disease_severity_trend`                 | Regional severity + case counts                                                 |
| `analytic_hospital_capacity_pressure`                      | Admissions vs beds                                                              |
| `analytic_symptom_to_admission_escalation`                 | Ratio from region facts                                                         |
| `analytic_event_volume_by_type`                            | Hourly volumes by event family                                                  |
| `analytic_avg_temperature_by_diagnosis`                    | Temperature by diagnosis                                                        |
| `analytic_follow_up_rate_by_clinic`                        | Follow-up rate                                                                  |
| `**analytic_vaccination_volume_by_type`** **(CO-2026-01)** | Hourly `**vaccination_count`** by `**vaccine_type`**, `**dose_number`**, region |


**ML:**


| Object                                  | Role                                                          |
| --------------------------------------- | ------------------------------------------------------------- |
| `vw_region_hourly_ml_features`          | Base features from `analytic_regional_disease_severity_trend` |
| `vw_region_hourly_ml_features_windowed` | Rolling sums, lags, multi-hour severity averages              |


**ML output / alerts:**


| Table                     | Writer                   | Role                                              |
| ------------------------- | ------------------------ | ------------------------------------------------- |
| `ml_outbreak_predictions` | `ml/predict.py`          | Scores and labels per region-hour / model version |
| `alert_events`            | `alert/alert_service.py` | Deduplicated alert rows for UI and ops            |


---

## 6. Idempotency and deduplication

Idempotency appears wherever the system must survive **retries, replays, or overlapping runs** without corrupting counts or storing infinite duplicates. Different layers use different strategies.

### 6.1 Raw landing (`raw_typed_events`) — Kafka-offset idempotency

The ingester (`scripts/ingest-typed-events-to-db.py`) inserts each consumed message with:

- **Primary key:** `(topic, kafka_partition, kafka_offset)` — uniquely identifies a single Kafka record.
- **Write semantics:** `INSERT … ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING`.

If the consumer **re-reads** the same offsets (crash restart before commit, replay, or a second ingester pass), the conflicting insert is skipped and `**cur.rowcount`** distinguishes new inserts from no-ops. **Database commit** happens before **Kafka consumer commit**, so a committed offset implies durable landing for those rows.

This is the strongest idempotency guarantee in the pipeline: at most **one row per Kafka record** in `raw_typed_events`.

Contract tests: `tests/unit/test_ingest_idempotency.py`.

### 6.2 Phase 1 Tekton (Kafka → Kafka)

Tekton tasks produce with `**event_id` as the Kafka key** when present, which helps downstream consumers that dedupe by key or stick to partitions. **They do not** implement database-style upserts: re-running Phase 1 or replaying raw topics can still **duplicate messages** on validated/typed topics unless a downstream store dedupes (that store is `raw_typed_events` when you use the ingester).

Invalid payloads routed to DLQs are separate envelopes; handling those is “process once per failure record,” not offset-based idempotency in the happy path.

### 6.3 Spark Layer A (`feature_*_hourly`) — append + logical “latest,” not upsert-by-metric

Spark writes with `**mode("append")`**. Primary keys **include `batch_id`**, so **re-running a job with a new `batch_id` adds another row** for the same `(bucket_start, …)` natural key. That is intentional: it preserves history and supports reconciliation.

**Serving correctness** relies on Layer B:

- Views such as `**v_latest_feature_*`** pick one row per bucket key (e.g. latest `run_timestamp` / `batch_id`), so dashboards and analytics **do not double-count** when multiple batches exist.
- Some deployments also tie “latest” to `**batch_window_runs`** success for a window.

So Layer A is **not** idempotent in the sense of “replace this hour’s aggregate in place”; it is **append-only with deduplicated reads** at the SQL view layer.

### 6.4 Window metadata (`batch_window_runs`)

Spark updates `**batch_window_runs`** with `**ON CONFLICT (window_start, run_id) DO UPDATE`**, refreshing `status`, `batch_id`, `reason_*`, and `completed_at` when a window run is retried or finalized. Retrying the same logical window under the same `**run_id`** overwrites metadata instead of inserting duplicate primary keys.

### 6.5 Alerts (`alert_events`)

The alert service uses `**alert_key` as primary key** and `**ON CONFLICT (alert_key) DO UPDATE`** so each logical alert stays **one row**, refreshed in place (`updated_at`, message, severity). Optional **cooldown** logic reduces log spam without multiplying rows (see `docs/alerting-guide.md`).

### 6.6 ML predictions (`ml_outbreak_predictions`)

Primary key `**(prediction_timestamp, bucket_start, region, model_version)`** allows multiple scoring runs over time; deduplication for “what to show now” is handled in queries (e.g. latest run), not by collapsing all writes to a single row per region-hour.

### 6.7 Summary


| Layer                 | Mechanism                          | Effect                                            |
| --------------------- | ---------------------------------- | ------------------------------------------------- |
| `raw_typed_events`    | PK + `ON CONFLICT DO NOTHING`      | Same Kafka record → one DB row max                |
| Tekton / Kafka topics | Keys + consumer groups             | Movement and ordering; not a DB dedupe            |
| `feature_*_hourly`    | Append + `batch_id` in PK          | History preserved; views choose latest for counts |
| `batch_window_runs`   | Upsert on `(window_start, run_id)` | Retry-safe window status                          |
| `alert_events`        | Upsert on `alert_key`              | One row per logical alert                         |


---

## 7. Change order CO-2026-01 — traceability


| Stage        | Implementation summary                                                                                                        |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| **Upstream** | Generator publishes `vaccination_record` with `schema_version: 2` on the **same raw topic** as other events.                  |
| **Validate** | Extra required fields and integer/timestamp checks for `vaccination_record`; same pass/fail/DLQ path as other types.          |
| **Route**    | Dedicated topic `**ds551-s26.team02.vaccination_records`**.                                                                   |
| **Ingester** | `TOPICS` includes `vaccination_records`; rows land in `**raw_typed_events`**.                                                 |
| **Spark**    | Reads vaccination stream or DB rows; writes `**feature_vaccination_hourly`**.                                                 |
| **Layer B**  | `**analytic_vaccination_volume_by_type`** for dashboards and rubric analytics using `**vaccine_type`** and `**dose_number`**. |
| **Docs**     | See also `[change-order-vaccination.md](change-order-vaccination.md)`, `[phase3-dataflow.md](phase3-dataflow.md)`.            |


---

## 8. Streamlit application (`app/`)

The Phase 3 dashboard is a **local** Streamlit process; it does **not** run inside the team namespace. It connects to **OpenShift-hosted TimescaleDB** via `**oc port-forward svc/timescaledb-service 5432:5432`** and credentials (see `[local-db-connection.md](local-db-connection.md)`).

### 8.1 How pieces connect


| Module               | Responsibility                                                                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `app/dashboard.py`   | Page layout, chart wiring, cache TTL coordination                                                                                                                                                           |
| `app/sql_queries.py` | Parameterized SQL; functions return `pandas.DataFrame` (e.g. `get_vaccination_volume` → `**analytic_vaccination_volume_by_type`**)                                                                          |
| `app/db.py`          | Connection factory, connectivity banner                                                                                                                                                                     |
| `app/components.py`  | Plotly charts: regional trends, event volume, hospital pressure, diagnosis temperature, follow-up, **vaccination volume** (`line_vaccination_volume`, `bar_top_vaccination_regions`), detector / ML, alerts |


**Data paths:**

- **Analytics pages** read `**analytic_*`** views (including vaccination).
- **Outbreak detector** reads `**ml_outbreak_predictions`** (via queries in `sql_queries.py`).
- **Alerts** read `**alert_events`**.

Every chart path ultimately resolves to **SQL against the current DB state**, satisfying Phase 3 “live query” expectations when port-forward and credentials are valid.

### 8.2 CO-2026-01 in the app

Vaccination charts use `**get_vaccination_volume`** which selects from `**analytic_vaccination_volume_by_type`** filtered by a rolling hours window—so `**vaccine_type`** and `**dose_number`** from the change order surface in the UI.

---

## 9. Deployment layout (repository)


| Path            | Contents                                               |
| --------------- | ------------------------------------------------------ |
| `infra/`        | Database deployment, Spark Job, RBAC, optional loadgen |
| `tekton/`       | Tasks, pipeline, pipelinerun templates                 |
| `spark/`        | `etl.py`, local test helpers                           |
| `scripts/`      | Sequenced orchestration, ingester, smoke tests         |
| `ml/`, `alert/` | Batch prediction and alert service                     |
| `app/`          | Streamlit dashboard                                    |


---

## 10. Related documentation

- `[runbook.md](runbook.md)` — Canonical sequenced mode, verify queries, stop/cancel, troubleshooting, legacy alternatives  
- `[phase3-architecture.md](phase3-architecture.md)` — Short Phase 3 overview  
- `[phase3-dataflow.md](phase3-dataflow.md)` — Topic and stage names  
- `[spark/README.md](../spark/README.md)` — Spark env vars and JDBC  
- `[visualization-guide.md](visualization-guide.md)` — Dashboard usage  
- `[testing-plan.md](testing-plan.md)` — pytest and DB tests

