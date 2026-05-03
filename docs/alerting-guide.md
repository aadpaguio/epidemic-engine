# Phase 3 alerting service

`epidemic-alert-service` is a Python deployment that polls TimescaleDB,
evaluates pipeline freshness and rubric-driven metric rules, persists each
alert into `alert_events`, prints structured `[ALERT]` JSON to stdout, and
posts to Slack. The Streamlit Alerts page reads the same `alert_events`
rows so dashboard, logs, and Slack stay in sync.

## Architecture

```text
raw_typed_events  ─┐
ml_outbreak_predictions ─┼─► epidemic-alert-service (Deployment)
vw_region_hourly_ml_features_windowed ─┘            │
                                                    ├──► UPSERT alert_events  ──► Streamlit "Alerts" page
                                                    ├──► stdout: [ALERT] {...} ──► oc logs / runbook evidence
                                                    └──► Slack incoming webhook ──► #epidemic-alerts
```

The service runs `run_cycle()` every `ALERT_POLL_SECONDS`. Each cycle it:

1. Ensures the `alert_events` table exists.
2. Checks ML pipeline freshness (predictions, feature view).
3. Checks ML outbreak agreement (composite + optional single-detector).
4. Evaluates ConfigMap-driven metric rules (event volume, avg severity).
5. Resolves any previously active alert key whose condition has cleared.

## Categories of alerts

| Alert family | Source | Severity ladder | Notes |
|--------------|--------|-----------------|-------|
| Pipeline freshness (`prediction_freshness`, `feature_freshness`) | `ml_outbreak_predictions`, `vw_region_hourly_ml_features_windowed` | critical / warning | Empty or stale ML output. |
| ML outbreak (`outbreak_risk`) | `ml_outbreak_predictions` | critical (3+ models), warning (2 models), info (single, opt-in) | One row per `(region, bucket_start)`. |
| Event volume (`event_count`) | `raw_typed_events` over 5m / 1h / 1d windows | rule-defined | Multi-window consistency required. |
| Avg severity (`avg_severity`) | `raw_typed_events` payload severities (mild=1..critical=4) | rule-defined | Multi-window consistency required. |

## `alert_events` schema

| Column | Type | Notes |
|--------|------|-------|
| `alert_key` | TEXT PK | Stable dedup key (`pipeline:stale_predictions`, `metric:high_event_volume`, `outbreak:Region:ISO8601:composite`, etc.). |
| `created_at` | TIMESTAMPTZ | First active emission. Reset to `now()` if an alert resolves and re-fires. |
| `updated_at` | TIMESTAMPTZ | Last upsert. Used by cooldown gating. |
| `alert_type` | TEXT | `prediction_freshness`, `feature_freshness`, `outbreak_risk`, `event_count`, `avg_severity`, … |
| `severity` | TEXT | `critical`, `warning`, `info`. |
| `region` | TEXT | Optional; set on regional outbreak alerts. |
| `bucket_start` | TIMESTAMPTZ | Optional; set when the alert ties to a Spark hour. |
| `model_versions` | TEXT[] | Optional; set on outbreak alerts. |
| `max_risk_score` | DOUBLE PRECISION | Optional; set on outbreak alerts. |
| `run_id` | TEXT | Optional; ML run id. |
| `status` | TEXT | `active` or `resolved` (used by Streamlit filters and resolution logic). |
| `message` | TEXT | Human-readable summary. |
| `raw_payload` | JSONB | Full structured payload mirrored to logs and Slack, including window measurements and escalation markers. |

DDL ships in `infra/database-init.sql`, `infra/database-deployment.yaml`,
and `ml/ml_schema.sql`. The service also re-runs `CREATE TABLE IF NOT
EXISTS` on startup as a safety net.

## Multi-window consistency

Metric rules from `thresholds.json` evaluate **5m / 1h / 1d** windows in a
single cycle and only fire when the rule's `min_windows_breaching` is
satisfied. The full window set (value + threshold + breach flag) is
written into `raw_payload.measurements` and rendered in Slack messages so
operators can see, at a glance, whether the spike is short-lived noise or
a sustained event.

```json
{
  "high_event_volume": {
    "metric": "event_count",
    "windows": [
      {"name": "5m", "minutes": 5,    "threshold": 50},
      {"name": "1h", "minutes": 60,   "threshold": 1000},
      {"name": "1d", "minutes": 1440, "threshold": 20000}
    ],
    "severity": "warning",
    "min_windows_breaching": 2,
    "cooldown_seconds": 1800
  }
}
```

Edit the ConfigMap to retune thresholds; no code change or redeploy is
required.

## Suppression / dedupe / escalation

- **DB dedupe:** one row per `alert_key`, `ON CONFLICT DO UPDATE`.
- **Cooldown:** `cooldown_allows()` uses `updated_at` to suppress repeat
  log + Slack notifications until `ALERT_COOLDOWN_SECONDS` (or the rule's
  `cooldown_seconds`) elapses. Suppressed cycles also skip the upsert so
  cooldown timing keeps moving forward.
- **Escalation:** when an alert has stayed active for at least
  `ESCALATE_AFTER_SECONDS`, every subsequent notification is tagged
  `escalated: true` in the payload, gets an `:arrow_double_up: ESCALATED`
  marker in Slack, and (for `critical`) renders the configured
  `SLACK_CRITICAL_MENTION` so a human is paged.
- **Resolution:** when the underlying condition clears the service flips
  the row to `status='resolved'` so Streamlit and downstream consumers do
  not see ghost alerts. A re-fire after resolution resets `created_at`,
  so escalation timing reflects the current incident only.

## Slack delivery

The notifier uses an **incoming webhook** posted via `urllib.request`, so
the alert image stays minimal. Severity controls who is paged:

- `critical` — sent to Slack with `SLACK_CRITICAL_MENTION` (default
  `<!channel>`).
- `warning` — sent to Slack without mention.
- `info` — only sent if `SLACK_NOTIFY_SEVERITIES` includes `info`.

Failures (timeouts, non-2xx responses, DNS issues) are logged and
swallowed so the alert loop keeps running.

### Configure Slack in OpenShift

```bash
oc create secret generic epidemic-slack-webhook \
  --from-literal=webhook_url='https://hooks.slack.com/services/T000/B000/XXXX'
oc apply -f alert/alert-deployment.yaml
oc rollout restart deploy/epidemic-alert-service
```

Set `SLACK_ENABLED=0` in `epidemic-alert-config` to silence Slack while
keeping DB + log alerts during maintenance.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | **Required.** Same Secret pattern as ML (`outbreak-db-url`). Normalizes `postgresql+psycopg2://` and `postgres://` to `postgresql://`. |
| `ALERT_POLL_SECONDS` | `300` | Sleep between poll cycles. |
| `ALERT_COOLDOWN_SECONDS` | `1800` | Default min seconds between repeat notifications per `alert_key`. |
| `ESCALATE_AFTER_SECONDS` | `7200` | Mark notifications escalated once an alert has been active this long. |
| `ALERT_RULES_PATH` | `/etc/alerting/thresholds.json` | Mounted ConfigMap; falls back to `alert/thresholds.json` then built-in defaults. |
| `PREDICTION_STALE_MINUTES` | `180` | Stale prediction threshold. |
| `FEATURE_STALE_HOURS` | `12` | Stale ML feature bucket threshold. |
| `OUTBREAK_LOOKBACK_HOURS` | `24` | Window for outbreak queries. |
| `MIN_CONFIRMING_MODELS` | `2` | Composite outbreak detector agreement. |
| `OUTBREAK_ALERT_SINGLE_MODEL` | `0` | Set `1` to enable single-detector info alerts. |
| `SLACK_ENABLED` | derived from `SLACK_WEBHOOK_URL` | `1`/`0` master switch. |
| `SLACK_WEBHOOK_URL` | — | From Secret `epidemic-slack-webhook`/`webhook_url`. |
| `SLACK_CHANNEL_NAME` | `#epidemic-alerts` | Display label used in Slack messages. |
| `SLACK_CRITICAL_MENTION` | `<!channel>` | Mention prefix for critical alerts. |
| `SLACK_NOTIFY_SEVERITIES` | `critical,warning` | Comma-separated allowlist. |
| `SLACK_TIMEOUT_SECONDS` | `5` | HTTP timeout for Slack POST. |

ConfigMaps: `alert/alert-deployment.yaml` (canonical, includes the
`epidemic-alert-rules` ConfigMap with `thresholds.json`) and
`alert/alert-config.yaml` (reference comments). Defaults also live in
`alert/thresholds.json` for local runs.

## Sample `[ALERT]` log line

```text
[ALERT] {"component": "alert-service", "alert_type": "event_count", "severity": "warning", "rule_name": "high_event_volume", "metric": "event_count", "measurements": [{"window": "5m", "minutes": 5, "value": 312, "threshold": 50, "breach": true}, {"window": "1h", "minutes": 60, "value": 4120, "threshold": 1000, "breach": true}, {"window": "1d", "minutes": 1440, "value": 12044, "threshold": 20000, "breach": false}], "n_breaches": 2, "min_windows_breaching": 2, "alert_key": "metric:high_event_volume"}
```

## Streamlit

`app/dashboard.py` ("Alerts" page) reads `alert_events` via
`get_alert_events()` in `app/sql_queries.py`, sorted by `updated_at DESC`,
with severity / type / status filters.

## Build and deploy (OpenShift)

**Option A — local container engine (podman/docker)** — same image URL your `Deployment` references:

```bash
podman build -f alert/Dockerfile \
  -t image-registry.openshift-image-registry.svc:5000/<project>/epidemic-alert-service:latest .
podman push image-registry.openshift-image-registry.svc:5000/<project>/epidemic-alert-service:latest
```

**Option B — binary build on the cluster** (matches [`infra/buildconfig-outbreak-predict-binary.yaml`](../infra/buildconfig-outbreak-predict-binary.yaml); no Git credentials needed). Run from **repository root** so `alert/Dockerfile` `COPY` paths work:

```bash
oc apply -f infra/buildconfig-epidemic-alert-service-binary.yaml
oc start-build epidemic-alert-service --from-dir=. --follow
```

Then apply manifests (ConfigMaps + Deployment) or restart to pick up a new image:

```bash
oc apply -f alert/alert-deployment.yaml
oc rollout restart deploy/epidemic-alert-service
oc logs deploy/epidemic-alert-service -f
```

If someone previously ran `oc new-build` with Git source for this app, delete or replace that `BuildConfig` so it stays **Binary-only**—otherwise OpenShift may try to clone GitHub during builds.

Required Secrets:

- `outbreak-db-url` (`database_url`) — TimescaleDB DSN.
- `epidemic-slack-webhook` (`webhook_url`) — Slack incoming webhook.
  Marked `optional: true` in the deployment so the pod still starts
  before the webhook is provisioned.

## CLI smoke modes

```bash
# Single cycle then exit (good for cron / Tekton / local dev).
python -m alert.alert_service --once

# Send a single Slack ping using the configured webhook.
python -m alert.alert_service --test-slack
```

## Local verification

1. Port-forward TimescaleDB if needed (`docs/local-db-connection.md`).
2. Copy `.env.example` to `.env`, fill in `DATABASE_URL`, and (optionally)
   `SLACK_WEBHOOK_URL` / `SLACK_ENABLED=1`.
3. Run a single cycle:

   ```bash
   export $(grep -v '^#' .env | xargs)
   python -m alert.alert_service --once
   ```

4. Inspect alerts:

   ```sql
   SELECT alert_key, severity, status, updated_at, message
   FROM alert_events
   ORDER BY updated_at DESC LIMIT 20;
   ```

5. Run unit tests:

   ```bash
   pytest tests/unit/test_alert_service.py -v
   ```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| No rows in `alert_events` | DB reachable? `ml_outbreak_predictions` populated? Rules may simply not fire. |
| Stale prediction alerts | Spark/ML schedule; compare `MAX(prediction_timestamp)` to `now()`. |
| No composite outbreak alerts | Need ≥2 models with `high_risk` on same `(region, bucket_start)` in lookback window. |
| `metric:*` rules never fire | `raw_typed_events` populated by `scripts/ingest-typed-events-to-db.py` or Spark? Try lowering thresholds via the `epidemic-alert-rules` ConfigMap. |
| Slack quiet but DB shows alerts | `SLACK_ENABLED=1`? Webhook secret populated? `SLACK_NOTIFY_SEVERITIES` includes the severity? Check pod logs for `slack webhook send failed`. |
| Pod crashloops on missing Slack | Ensure the secret reference uses `optional: true` (default in `alert/alert-deployment.yaml`). |
| Streamlit shows "No alert rows" | Table missing — let the service `ensure_tables()` once, or apply infra DDL. |
| `ModuleNotFoundError: alert` | Run from repo root with `PYTHONPATH=.` or use `python -m alert.alert_service`. |
