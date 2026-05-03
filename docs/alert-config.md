# Alert configuration runbook

This is the operational quick-reference for `epidemic-alert-service`.
For architecture and rule semantics see [`docs/alerting-guide.md`](alerting-guide.md).

## Building the container image

The `Deployment` pulls `epidemic-alert-service:latest` from your namespace’s image registry. Build either with **podman** locally or a **binary OpenShift build** (see [`infra/buildconfig-epidemic-alert-service-binary.yaml`](../infra/buildconfig-epidemic-alert-service-binary.yaml) and [`docs/alerting-guide.md`](alerting-guide.md) § Build and deploy).

## Where settings live

| Setting | Source | How to change |
|---------|--------|---------------|
| Numeric thresholds (`event_count`, `avg_severity`, `data_quality_issue_pct`, `missing_severity_pct`) | `epidemic-alert-rules` ConfigMap, key `thresholds.json` (mounted at `/etc/alerting/thresholds.json`). Default copy: `alert/thresholds.json`. | `oc edit configmap epidemic-alert-rules` (or reapply `alert/alert-deployment.yaml`), then `oc rollout restart deploy/epidemic-alert-service`. |
| ML pipeline thresholds (`PREDICTION_STALE_MINUTES`, etc.) | `epidemic-alert-config` ConfigMap. | `oc edit configmap epidemic-alert-config` and restart the deployment. |
| Polling, cooldown, escalation timers | `epidemic-alert-config` ConfigMap. | Same as above. |
| Slack channel and mention behavior | `epidemic-alert-config` ConfigMap. | Same as above. |
| Slack webhook URL | `epidemic-slack-webhook` Secret, key `webhook_url`. | `oc create secret generic epidemic-slack-webhook --from-literal=webhook_url=<URL>` (then restart). |
| Database DSN | `outbreak-db-url` Secret, key `database_url`. | Reuse the ML secret. |

## Common operations

```bash
# Tail alert service logs
oc logs deploy/epidemic-alert-service -f

# Trigger a one-off Slack ping (uses the running pod's secret/config)
oc exec deploy/epidemic-alert-service -- python -m alert.alert_service --test-slack

# Run one cycle locally against port-forwarded DB
PYTHONPATH=. DATABASE_URL=postgresql://user:pass@localhost:5432/epidemic \
  python -m alert.alert_service --once

# Inspect active alerts via psql
SELECT alert_key, severity, status, updated_at, message
FROM alert_events
WHERE status = 'active'
ORDER BY updated_at DESC;
```

## Tuning checklist

1. **Make the rule fire on demand** — temporarily lower a window
   threshold in the `thresholds.json` ConfigMap (for example `5m`
   threshold from 50 to 1) and confirm a `[ALERT]` log line plus a Slack
   message appear. Restore the value and restart the deployment.
2. **Validate cooldown** — fire the same condition twice in quick
   succession; the second firing should be suppressed because
   `cooldown_seconds` has not elapsed (see `updated_at` on the row).
3. **Validate escalation** — set `ESCALATE_AFTER_SECONDS=60` for a quick
   demo, let an alert stay active across two cycles, and confirm Slack
   shows `:arrow_double_up: ESCALATED`. Reset the value when done.
4. **Validate resolution** — make the breaching condition clear, wait one
   poll cycle, and confirm the row's `status` flips to `resolved` and
   the Streamlit Alerts table no longer lists it as active.

## Failure recovery quick-reference

| If… | Then… |
|-----|-------|
| Pod crashloops on startup | Check `oc describe pod` for missing Secret keys. The Slack secret is `optional`; the DB secret is required. |
| Slack quiet but DB rows show alerts | Verify `SLACK_ENABLED=1`, the severity is in `SLACK_NOTIFY_SEVERITIES`, and the webhook is reachable from the cluster (look for `slack webhook send failed` in logs). |
| Threshold edits do not take effect | ConfigMap changes need a restart: `oc rollout restart deploy/epidemic-alert-service`. |
| DB went down | Service exits the cycle with `alert-service cycle failed`, sleeps `ALERT_POLL_SECONDS`, and retries on the next cycle. Streamlit Alerts page will keep the last successful rows. |

## Tests

```bash
pytest tests/unit/test_alert_service.py -v
```

The unit suite covers rule loading, multi-window evaluation, cooldown
suppression, escalation marking, Slack message rendering, network-error
handling, and the `--once` / `--test-slack` CLI modes — all without
requiring Postgres or a live Slack workspace.
