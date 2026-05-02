# Phase 3 alerting service

Lightweight **dashboard + log–based** notifications: one Python Deployment polls TimescaleDB, evaluates freshness and outbreak rules, **upserts** rows into `alert_events`, and prints structured **`[ALERT]`** JSON lines to stdout. No Slack or outbound webhooks are required for Phase 3.

## Architecture

```text
TimescaleDB (ml_outbreak_predictions, vw_region_hourly_ml_features_windowed)
        │
        ▼
  epidemic-alert-service (Deployment)
        │
        ├──► INSERT/UPSERT alert_events  ──► Streamlit “Alerts” page
        └──► stdout: [ALERT] { ... json ... }   ──► oc logs / runbook evidence
```

## `alert_events` schema

| Column | Type | Notes |
|--------|------|--------|
| `alert_key` | TEXT PK | Stable key for dedupe, e.g. `pipeline:stale_predictions`, `outbreak:Region:ISO8601:composite` |
| `created_at` | TIMESTAMPTZ | First insert |
| `updated_at` | TIMESTAMPTZ | Last upsert / refresh |
| `alert_type` | TEXT | `prediction_freshness`, `feature_freshness`, `outbreak_risk`, … |
| `severity` | TEXT | `critical`, `warning`, `info` |
| `region` | TEXT | Nullable |
| `bucket_start` | TIMESTAMPTZ | Nullable |
| `model_versions` | TEXT[] | Nullable |
| `max_risk_score` | DOUBLE PRECISION | Nullable |
| `run_id` | TEXT | Nullable |
| `status` | TEXT | Default `active` (reserved for acknowledge flows) |
| `message` | TEXT | Human-readable summary |
| `raw_payload` | JSONB | Full structured payload mirrored from logs |

DDL lives in `infra/database-init.sql`, `infra/database-deployment.yaml` (bootstrap SQL), and `ml/ml_schema.sql`. On startup the service also runs `CREATE TABLE IF NOT EXISTS` so an older DB without a redeploy can still create the table.

## Alert rules

1. **Prediction freshness** — `MAX(prediction_timestamp)` on `ml_outbreak_predictions`. Alerts if older than `PREDICTION_STALE_MINUTES` (default `180`). Empty table → `pipeline:no_predictions`.
2. **Feature freshness** — `MAX(bucket_start)` on `vw_region_hourly_ml_features_windowed`. Alerts if older than `FEATURE_STALE_HOURS` (default `12`).
3. **Composite outbreak** — Within `OUTBREAK_LOOKBACK_HOURS`, latest row per `(bucket_start, region, model_version)` where `predicted_label = 'high_risk'` and `model_version` is one of `moving_avg_k6_v1`, `ewma_v1`, `kalman_v1`, `v1`. If **≥ `MIN_CONFIRMING_MODELS`** distinct models (default `2`) agree on the same `(bucket_start, region)`, emit **warning** (2 models) or **critical** (3+ models).
4. **Single-detector outbreak (optional)** — If `OUTBREAK_ALERT_SINGLE_MODEL=1`, same window but only `moving_avg_k6_v1` and `ewma_v1`, **exactly one** model flags `high_risk` → **info** severity (less noisy; off by default).

## Deduplication / cooldown

- **DB:** one row per `alert_key`; `ON CONFLICT DO UPDATE` refreshes content.
- **Cooldown:** before emitting another `[ALERT]` log line and touching the row, the service checks `updated_at` in `alert_events`. If the last update was within `ALERT_COOLDOWN_SECONDS`, the alert is **suppressed** for that cycle (no log spam, no upsert bump).

In-memory state is optional; DB-backed keys survive pod restarts without duplicate dashboard rows every restart.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | **Required.** Same secret pattern as ML (`outbreak-db-url`). Accepts `postgresql+psycopg2://…`; normalized for `psycopg2`. |
| `ALERT_POLL_SECONDS` | `300` | Sleep between poll cycles |
| `ALERT_COOLDOWN_SECONDS` | `1800` | Min seconds between repeated fires per `alert_key` |
| `PREDICTION_STALE_MINUTES` | `180` | Stale prediction threshold |
| `FEATURE_STALE_HOURS` | `12` | Stale ML feature bucket threshold |
| `OUTBREAK_LOOKBACK_HOURS` | `24` | Window for outbreak queries |
| `MIN_CONFIRMING_MODELS` | `2` | Composite outbreak detector agreement |
| `OUTBREAK_ALERT_SINGLE_MODEL` | `0` | Set `1` to enable single-detector info alerts |

ConfigMap reference: `alert/alert-config.yaml` and `alert/alert-deployment.yaml`.

## Sample `[ALERT]` log line

```text
[ALERT] {"component": "alert-service", "alert_type": "outbreak_risk", "severity": "warning", "region": "Boston", "bucket_start": "2026-04-30T18:00:00+00:00", "model_versions": ["ewma_v1", "moving_avg_k6_v1"], "n_models": 2, "max_risk_score": 1.0, "run_id": "eventtime-...", "reason": "Multiple detectors flagged high_risk for the same region bucket.", "alert_key": "outbreak:Boston:2026-04-30T18:00:00+00:00:composite"}
```

## Streamlit

The dashboard **Alerts** page reads `alert_events` via `get_alert_events()` in `app/sql_queries.py`, sorted by `updated_at DESC`, with filters for severity and alert type.

## Build and deploy (OpenShift)

From repository root:

```bash
podman build -f alert/Dockerfile -t image-registry.openshift-image-registry.svc:5000/<project>/epidemic-alert-service:latest .
podman push image-registry.openshift-image-registry.svc:5000/<project>/epidemic-alert-service:latest
oc apply -f alert/alert-deployment.yaml
oc logs deploy/epidemic-alert-service -f
```

Ensure Secret `outbreak-db-url` with key `database_url` exists (same as `ml/ml-model.yaml`).

## Local verification

1. Port-forward TimescaleDB if needed (`docs/local-db-connection.md`).
2. Export `DATABASE_URL` (or use individual `DB_*` vars only for Streamlit; **the alert service requires `DATABASE_URL`**).
3. One-shot run (exits after manual Ctrl+C is still infinite loop — instead shorten poll):

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/epidemic'
export ALERT_POLL_SECONDS=3600   # long sleep after first cycle for a quick test
python -m alert.alert_service
```

4. Query alerts:

```sql
SELECT * FROM alert_events ORDER BY updated_at DESC LIMIT 20;
```

5. If predictions are empty or the ML output window is too narrow, widen `ML_OUTPUT_WINDOW_HOURS` on the predict job or insert test rows into `ml_outbreak_predictions` per `docs/ml-model-guide.md`.

## Troubleshooting

| Symptom | Check |
|---------|--------|
| No rows in `alert_events` | DB reachable? `ml_outbreak_predictions` populated? Rules may simply not fire. |
| Stale prediction alerts | Spark/ML job schedule; compare `MAX(prediction_timestamp)` to `now()`. |
| No composite outbreak alerts | Need ≥2 models with `high_risk` on same `(region, bucket_start)` in lookback window. |
| Streamlit “No alert rows” | Table missing (allow service to create), or service not deployed. |
| `ModuleNotFoundError: alert` | Run from repo root with `PYTHONPATH=.` or use `python -m alert.alert_service`. |

## Future extensions

Slack, email, or webhooks can subscribe to the same rules or tail `[ALERT]` logs; not required for Phase 3.
