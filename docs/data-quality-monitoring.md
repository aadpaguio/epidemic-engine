# Data quality monitoring

This feature maps to Phase 3 **Category D — Data Quality & Monitoring**: profiling on landed data, dashboard trends, and threshold alerts.

## What is covered

1. **Upstream schema validation (Kafka → Tekton)**  
   `validate-events` in [`tekton/tasks.yaml`](../tekton/tasks.yaml) rejects invalid JSON, missing base fields, bad timestamps, and bad `schema_version` before routing. That satisfies “schema validation on incoming Kafka events” at the pipeline edge.

2. **Profiling on landed typed events**  
   After Tekton, the short ingester writes validated typed payloads to `raw_typed_events` (see [`scripts/ingest-typed-events-to-db.py`](../scripts/ingest-typed-events-to-db.py)). The view **`vw_data_quality_hourly`** (in [`infra/database-init.sql`](../infra/database-init.sql) and the Timescale init ConfigMap in [`infra/database-deployment.yaml`](../infra/database-deployment.yaml)) aggregates per UTC hour:

   - row counts  
   - missing `event_type`, `event_ts`, `region`, `severity`  
   - invalid `severity` strings (not mild/moderate/severe/critical and not numeric)  
   - combined **quality issue %** and **missing severity %**

3. **Dashboard**  
   Streamlit page **Data Quality** in [`app/dashboard.py`](../app/dashboard.py) reads the view via [`app/sql_queries.py`](../app/sql_queries.py) `get_data_quality_hourly()` and charts rates plus hourly volume.

4. **Alerts**  
   [`alert/alert_service.py`](../alert/alert_service.py) evaluates ConfigMap rules for metrics `data_quality_issue_pct` and `missing_severity_pct` over sliding windows on `raw_typed_events` (same definitions as the view, aligned with rubric-style “% bad in window”). Rules live in [`alert/thresholds.json`](../alert/thresholds.json) and the `epidemic-alert-rules` ConfigMap in [`alert/alert-deployment.yaml`](../alert/alert-deployment.yaml).

## Operations

- **Apply DDL / view**: refresh the database from current `infra/database-init.sql`, or for a minimal cluster touch use [`scripts/apply-raw-landing-ddl.sh`](../scripts/apply-raw-landing-ddl.sh) (includes the view).  
- **Dashboard**: same as [`docs/visualization-guide.md`](visualization-guide.md); open the **Data Quality** page after port-forwarding the DB.  
- **Tune thresholds**: edit `thresholds.json` (or the mounted ConfigMap) and restart `epidemic-alert-service` if needed.

## Scope note

Counts reflect **rows that reached `raw_typed_events`**. Records rejected entirely by Tekton (never ingested) are not in this table; they remain visible via Tekton logs / DLQ topics. A future enhancement could persist validate-run counters to the database for a full reject-rate dashboard.
