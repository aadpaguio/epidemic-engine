#!/usr/bin/env python3
"""Epidemic alerting service.

Polls TimescaleDB for pipeline freshness, ML outbreak signals, landed
data-quality rates (completeness on ``raw_typed_events``), and
ConfigMap-driven event-volume / severity thresholds. Persists each alert
to the ``alert_events`` table, emits a structured ``[ALERT]`` JSON log
line to stdout, and (when configured) posts to a Slack incoming webhook.

Environment variables:

  Required:
    DATABASE_URL                  TimescaleDB DSN (postgresql://... or postgresql+psycopg2://...)

  Polling and lifecycle:
    ALERT_POLL_SECONDS            Seconds between cycles (default 300).
    ALERT_COOLDOWN_SECONDS        Default per-alert-key cooldown for outbound notifications (default 1800).
    ESCALATE_AFTER_SECONDS        After an alert has stayed active this long, mark notifications as escalated (default 7200).
    ALERT_RULES_PATH              Path to threshold JSON (default /etc/alerting/thresholds.json, falls back to alert/thresholds.json then built-in defaults).

  ML / pipeline rules:
    PREDICTION_STALE_MINUTES      (default 180)
    FEATURE_STALE_HOURS           (default 12)
    OUTBREAK_LOOKBACK_HOURS       (default 24)
    MIN_CONFIRMING_MODELS         (default 2)
    OUTBREAK_ALERT_SINGLE_MODEL   "1"/"true"/"yes" to emit info-level single-detector outbreaks (default 0)

  Slack delivery:
    SLACK_WEBHOOK_URL             Slack incoming webhook URL (Secret).
    SLACK_ENABLED                 "1"/"0" — when 0, never sends to Slack regardless of webhook.
    SLACK_CHANNEL_NAME            Display label (default #epidemic-alerts).
    SLACK_CRITICAL_MENTION        Mention prefix for critical alerts (default <!channel>).
    SLACK_NOTIFY_SEVERITIES       Comma-separated severities to forward (default critical,warning).
    SLACK_TIMEOUT_SECONDS         HTTP timeout (default 5).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json

from alert.notifier import SlackNotifier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return max(minimum, default)
    try:
        return max(minimum, int(raw.strip()))
    except ValueError:
        logger.warning("Invalid %s=%r; using default %s", name, raw, default)
        return max(minimum, default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_csv(name: str, default: Iterable[str]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return tuple(default)
    return tuple(piece.strip() for piece in raw.split(",") if piece.strip())


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

ALERT_POLL_SECONDS = _env_int("ALERT_POLL_SECONDS", 300, minimum=5)
ALERT_COOLDOWN_SECONDS = _env_int("ALERT_COOLDOWN_SECONDS", 1800, minimum=0)
ESCALATE_AFTER_SECONDS = _env_int("ESCALATE_AFTER_SECONDS", 7200, minimum=60)

PREDICTION_STALE_MINUTES = _env_int("PREDICTION_STALE_MINUTES", 180, minimum=1)
FEATURE_STALE_HOURS = _env_int("FEATURE_STALE_HOURS", 12, minimum=1)
OUTBREAK_LOOKBACK_HOURS = _env_int("OUTBREAK_LOOKBACK_HOURS", 24, minimum=1)
PREFERRED_MODELS = ("moving_avg_k6_v1", "ewma_v1", "kalman_v1", "v1")
SINGLE_ALERT_MODELS = ("moving_avg_k6_v1", "ewma_v1")
_raw_min_models = _env_int("MIN_CONFIRMING_MODELS", 2, minimum=1)
MIN_CONFIRMING_MODELS = min(_raw_min_models, len(PREFERRED_MODELS))
OUTBREAK_ALERT_SINGLE_MODEL = _env_bool("OUTBREAK_ALERT_SINGLE_MODEL", False)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
SLACK_ENABLED = _env_bool("SLACK_ENABLED", default=bool(SLACK_WEBHOOK_URL))
SLACK_CHANNEL_NAME = os.getenv("SLACK_CHANNEL_NAME", "#epidemic-alerts").strip() or "#epidemic-alerts"
SLACK_CRITICAL_MENTION = os.getenv("SLACK_CRITICAL_MENTION", "<!channel>")
SLACK_NOTIFY_SEVERITIES = _env_csv(
    "SLACK_NOTIFY_SEVERITIES", default=("critical", "warning")
)
SLACK_TIMEOUT_SECONDS = _env_int("SLACK_TIMEOUT_SECONDS", 5, minimum=1)

ALERT_RULES_PATH = os.getenv("ALERT_RULES_PATH", "/etc/alerting/thresholds.json")

# Built-in defaults. Mirrored in alert/thresholds.json so the ConfigMap and
# the source-tree fallback stay aligned.
DEFAULT_METRIC_RULES: dict[str, dict[str, Any]] = {
    "high_event_volume": {
        "metric": "event_count",
        "windows": [
            {"name": "5m", "minutes": 5, "threshold": 50},
            {"name": "1h", "minutes": 60, "threshold": 1000},
            {"name": "1d", "minutes": 1440, "threshold": 20000},
        ],
        "severity": "warning",
        "min_windows_breaching": 2,
        "cooldown_seconds": 1800,
    },
    "high_avg_severity": {
        "metric": "avg_severity",
        "windows": [
            {"name": "5m", "minutes": 5, "threshold": 3.0},
            {"name": "1h", "minutes": 60, "threshold": 2.8},
            {"name": "1d", "minutes": 1440, "threshold": 2.5},
        ],
        "severity": "critical",
        "min_windows_breaching": 2,
        "cooldown_seconds": 1800,
    },
    "high_landed_data_quality_issue_rate": {
        "metric": "data_quality_issue_pct",
        "windows": [
            {"name": "1h", "minutes": 60, "threshold": 20.0},
            {"name": "6h", "minutes": 360, "threshold": 20.0},
            {"name": "24h", "minutes": 1440, "threshold": 20.0},
        ],
        "severity": "warning",
        "min_windows_breaching": 2,
        "cooldown_seconds": 3600,
    },
    "high_missing_severity_rate": {
        "metric": "missing_severity_pct",
        "windows": [
            {"name": "1h", "minutes": 60, "threshold": 20.0},
            {"name": "6h", "minutes": 360, "threshold": 20.0},
            {"name": "24h", "minutes": 1440, "threshold": 20.0},
        ],
        "severity": "warning",
        "min_windows_breaching": 2,
        "cooldown_seconds": 3600,
    },
}


def _candidate_rule_paths(path: str | None) -> list[Path]:
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))
    repo_default = Path(__file__).resolve().parent / "thresholds.json"
    candidates.append(repo_default)
    return candidates


def load_rules(path: str | None = None) -> dict[str, dict[str, Any]]:
    """Load rule thresholds from JSON, falling back to built-in defaults."""
    for candidate in _candidate_rule_paths(path or ALERT_RULES_PATH):
        try:
            if candidate.exists():
                data = json.loads(candidate.read_text(encoding="utf-8"))
                if isinstance(data, dict) and data:
                    logger.info("loaded %d rule(s) from %s", len(data), candidate)
                    return data
        except (OSError, ValueError) as exc:
            logger.warning("failed to read rules from %s: %s", candidate, exc)
    logger.info("using built-in DEFAULT_METRIC_RULES")
    return {k: dict(v) for k, v in DEFAULT_METRIC_RULES.items()}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def normalize_db_url(url: str) -> str:
    u = url.strip()
    if u.startswith("postgresql+psycopg2://"):
        u = "postgresql://" + u[len("postgresql+psycopg2://") :]
    elif u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    return u


def log_alert(payload: dict[str, Any]) -> None:
    line = "[ALERT] " + json.dumps(payload, default=str)
    print(line, flush=True)


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_events (
                alert_key      TEXT PRIMARY KEY,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                alert_type     TEXT        NOT NULL,
                severity       TEXT        NOT NULL,
                region         TEXT,
                bucket_start   TIMESTAMPTZ,
                model_versions TEXT[],
                max_risk_score DOUBLE PRECISION,
                run_id         TEXT,
                status         TEXT        NOT NULL DEFAULT 'active',
                message        TEXT,
                raw_payload    JSONB
            );
            CREATE INDEX IF NOT EXISTS idx_alert_events_updated
                ON alert_events (updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_alert_events_status
                ON alert_events (status, updated_at DESC);
            """
        )
    conn.commit()


def cooldown_allows(
    conn, alert_key: str, cooldown_seconds: int | None = None
) -> bool:
    """Return True iff at least ``cooldown_seconds`` have elapsed since the
    most recent active update for ``alert_key``.

    Cooldown gates outbound notifications (log + Slack) and the upsert that
    refreshes ``updated_at``; resolution updates use a separate code path so
    they do not interfere with cooldown timing.
    """
    cooldown = (
        cooldown_seconds if cooldown_seconds is not None else ALERT_COOLDOWN_SECONDS
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT updated_at, status FROM alert_events WHERE alert_key = %s",
            (alert_key,),
        )
        row = cur.fetchone()
    if not row:
        return True
    last, status = row
    if status == "resolved":
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age >= cooldown


def get_alert_state(conn, alert_key: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT created_at, updated_at, status
            FROM alert_events
            WHERE alert_key = %s
            """,
            (alert_key,),
        )
        row = cur.fetchone()
    if not row:
        return None
    created_at, updated_at, status = row
    if created_at is not None and created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    if updated_at is not None and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return {
        "created_at": created_at,
        "updated_at": updated_at,
        "status": status,
    }


def upsert_alert(
    conn,
    *,
    alert_key: str,
    alert_type: str,
    severity: str,
    region: str | None,
    bucket_start,
    model_versions: list[str] | None,
    max_risk_score: float | None,
    run_id: str | None,
    message: str,
    raw_payload: dict[str, Any],
) -> None:
    """Insert or update an active alert row.

    On re-activation (existing row in ``status='resolved'``), ``created_at``
    is reset to ``now()`` so escalation timing reflects the current
    incident, not a long-resolved historical one.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_events (
                alert_key, alert_type, severity, region, bucket_start,
                model_versions, max_risk_score, run_id, status, message,
                raw_payload, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, now()
            )
            ON CONFLICT (alert_key) DO UPDATE SET
                updated_at = now(),
                created_at = CASE
                    WHEN alert_events.status = 'resolved' THEN now()
                    ELSE alert_events.created_at
                END,
                alert_type = EXCLUDED.alert_type,
                severity = EXCLUDED.severity,
                region = EXCLUDED.region,
                bucket_start = EXCLUDED.bucket_start,
                model_versions = EXCLUDED.model_versions,
                max_risk_score = EXCLUDED.max_risk_score,
                run_id = EXCLUDED.run_id,
                status = 'active',
                message = EXCLUDED.message,
                raw_payload = EXCLUDED.raw_payload
            """,
            (
                alert_key,
                alert_type,
                severity,
                region,
                bucket_start,
                model_versions,
                max_risk_score,
                run_id,
                message,
                Json(raw_payload),
            ),
        )
    conn.commit()


def resolve_pipeline_alert(conn, alert_key: str) -> None:
    """Mark a pipeline alert resolved when the underlying condition clears."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE alert_events
            SET status = 'resolved', updated_at = now()
            WHERE alert_key = %s AND status = 'active'
            """,
            (alert_key,),
        )
    conn.commit()


def resolve_composite_outbreaks_not_in(conn, active_keys: set[str]) -> None:
    with conn.cursor() as cur:
        if not active_keys:
            cur.execute(
                """
                UPDATE alert_events
                SET status = 'resolved', updated_at = now()
                WHERE status = 'active'
                  AND alert_key ~ '^outbreak:.+:composite$'
                """
            )
        else:
            cur.execute(
                """
                UPDATE alert_events
                SET status = 'resolved', updated_at = now()
                WHERE status = 'active'
                  AND alert_key ~ '^outbreak:.+:composite$'
                  AND NOT (alert_key = ANY(%s))
                """,
                (list(active_keys),),
            )
    conn.commit()


def resolve_single_outbreaks_not_in(conn, active_keys: set[str]) -> None:
    with conn.cursor() as cur:
        if not active_keys:
            cur.execute(
                """
                UPDATE alert_events
                SET status = 'resolved', updated_at = now()
                WHERE status = 'active'
                  AND alert_key ~ '^outbreak_single:.+'
                """
            )
        else:
            cur.execute(
                """
                UPDATE alert_events
                SET status = 'resolved', updated_at = now()
                WHERE status = 'active'
                  AND alert_key ~ '^outbreak_single:.+'
                  AND NOT (alert_key = ANY(%s))
                """,
                (list(active_keys),),
            )
    conn.commit()


# ---------------------------------------------------------------------------
# Centralised alert emission
# ---------------------------------------------------------------------------


def _is_escalated(state: dict[str, Any] | None) -> tuple[bool, float]:
    if not state or state.get("status") != "active":
        return False, 0.0
    created_at = state.get("created_at")
    if created_at is None:
        return False, 0.0
    active_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    return active_seconds >= ESCALATE_AFTER_SECONDS, active_seconds


def emit_alert(
    conn,
    notifier: SlackNotifier | None,
    *,
    alert_key: str,
    alert_type: str,
    severity: str,
    message: str,
    raw_payload: dict[str, Any],
    region: str | None = None,
    bucket_start: Any | None = None,
    model_versions: list[str] | None = None,
    max_risk_score: float | None = None,
    run_id: str | None = None,
    cooldown_seconds: int | None = None,
    notify_severities: tuple[str, ...] | None = None,
) -> bool:
    """Persist, log, and (optionally) Slack-notify a single alert.

    Returns ``True`` when the alert was persisted and notified this cycle,
    ``False`` when it was suppressed by cooldown.
    """
    state = get_alert_state(conn, alert_key)
    if not cooldown_allows(conn, alert_key, cooldown_seconds=cooldown_seconds):
        return False

    escalated, active_seconds = _is_escalated(state)

    payload = dict(raw_payload)
    payload.setdefault("component", "alert-service")
    payload["alert_key"] = alert_key
    payload["alert_type"] = alert_type
    payload["severity"] = severity
    if escalated:
        payload["escalated"] = True
        payload["escalation_active_seconds"] = round(active_seconds, 1)

    upsert_alert(
        conn,
        alert_key=alert_key,
        alert_type=alert_type,
        severity=severity,
        region=region,
        bucket_start=bucket_start,
        model_versions=model_versions,
        max_risk_score=max_risk_score,
        run_id=run_id,
        message=message,
        raw_payload=payload,
    )

    log_alert(payload)

    severities = notify_severities if notify_severities is not None else SLACK_NOTIFY_SEVERITIES
    if notifier is not None and severity in severities:
        try:
            notifier.send_alert(
                alert_key=alert_key,
                alert_type=alert_type,
                severity=severity,
                message=message,
                payload=payload,
                escalated=escalated,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("slack notification failed for %s: %s", alert_key, exc)

    return True


# ---------------------------------------------------------------------------
# ML pipeline rules
# ---------------------------------------------------------------------------


def _model_versions_join(models) -> str:
    if models is None:
        return ""
    if isinstance(models, (list, tuple)):
        return ", ".join(str(m) for m in models)
    return str(models)


def check_stale_predictions(conn, notifier: SlackNotifier | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(prediction_timestamp) FROM ml_outbreak_predictions")
        row = cur.fetchone()
    latest = row[0] if row else None
    if latest is not None:
        resolve_pipeline_alert(conn, "pipeline:no_predictions")

    if latest is None:
        resolve_pipeline_alert(conn, "pipeline:stale_predictions")
        emit_alert(
            conn,
            notifier,
            alert_key="pipeline:no_predictions",
            alert_type="prediction_freshness",
            severity="critical",
            message="No rows in ml_outbreak_predictions",
            raw_payload={
                "reason": "ml_outbreak_predictions is empty",
            },
        )
        return

    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - latest).total_seconds() / 60.0
    if age_min <= PREDICTION_STALE_MINUTES:
        resolve_pipeline_alert(conn, "pipeline:stale_predictions")
        return

    emit_alert(
        conn,
        notifier,
        alert_key="pipeline:stale_predictions",
        alert_type="prediction_freshness",
        severity="critical",
        message=(
            f"No prediction updates in {age_min:.0f} minutes "
            f"(threshold {PREDICTION_STALE_MINUTES})"
        ),
        raw_payload={
            "reason": "predictions older than threshold",
            "latest_prediction_timestamp": latest.isoformat(),
            "age_minutes": round(age_min, 1),
            "threshold_minutes": PREDICTION_STALE_MINUTES,
        },
    )


def check_feature_freshness(conn, notifier: SlackNotifier | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(bucket_start) FROM vw_region_hourly_ml_features_windowed"
        )
        row = cur.fetchone()
    latest = row[0] if row else None
    if latest is None:
        return
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0
    if age_h <= FEATURE_STALE_HOURS:
        resolve_pipeline_alert(conn, "pipeline:stale_features")
        return

    emit_alert(
        conn,
        notifier,
        alert_key="pipeline:stale_features",
        alert_type="feature_freshness",
        severity="warning",
        message=(
            f"Latest feature bucket is {age_h:.1f}h old "
            f"(threshold {FEATURE_STALE_HOURS}h)"
        ),
        raw_payload={
            "reason": "ML feature view buckets are stale",
            "latest_bucket_start": latest.isoformat(),
            "age_hours": round(age_h, 2),
            "threshold_hours": FEATURE_STALE_HOURS,
        },
        bucket_start=latest,
    )


def check_outbreak_composite(conn, notifier: SlackNotifier | None = None) -> None:
    """Alert when ``MIN_CONFIRMING_MODELS`` distinct detectors flag the same
    region/bucket as ``high_risk``."""
    _in = ", ".join(["%s"] * len(PREFERRED_MODELS))
    sql = f"""
        WITH latest_rows AS (
            SELECT DISTINCT ON (bucket_start, region, model_version)
                bucket_start,
                region,
                model_version,
                risk_score,
                run_id,
                predicted_label,
                prediction_timestamp
            FROM ml_outbreak_predictions
            WHERE predicted_label = 'high_risk'
              AND bucket_start >= now() - (%s || ' hours')::interval
              AND model_version IN ({_in})
            ORDER BY bucket_start, region, model_version, prediction_timestamp DESC
        ),
        grouped AS (
            SELECT
                bucket_start,
                region,
                array_agg(DISTINCT model_version ORDER BY model_version) AS models,
                COUNT(DISTINCT model_version)::int AS n_models,
                MAX(risk_score) AS max_risk,
                MAX(run_id) AS sample_run_id
            FROM latest_rows
            GROUP BY bucket_start, region
            HAVING COUNT(DISTINCT model_version) >= %s
        )
        SELECT bucket_start, region, models, n_models, max_risk, sample_run_id
        FROM grouped
        ORDER BY bucket_start DESC, region;
    """
    params = (OUTBREAK_LOOKBACK_HOURS, *PREFERRED_MODELS, MIN_CONFIRMING_MODELS)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    active_composite: set[str] = set()
    for bucket_start, region, *_ in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        active_composite.add(f"outbreak:{region}:{bucket_iso}:composite")

    for bucket_start, region, models, n_models, max_risk, run_id in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        key = f"outbreak:{region}:{bucket_iso}:composite"
        severity = "critical" if n_models >= 3 else "warning"
        mv_list = list(models) if models else []
        emit_alert(
            conn,
            notifier,
            alert_key=key,
            alert_type="outbreak_risk",
            severity=severity,
            message=(
                f"Outbreak signal: {region} @ {bucket_iso} — "
                f"{n_models} models agree: {_model_versions_join(models)}"
            ),
            raw_payload={
                "region": region,
                "bucket_start": bucket_iso,
                "model_versions": mv_list,
                "n_models": n_models,
                "max_risk_score": float(max_risk) if max_risk is not None else None,
                "run_id": run_id,
                "reason": "Multiple detectors flagged high_risk for the same region bucket.",
            },
            region=region,
            bucket_start=bucket_start,
            model_versions=mv_list,
            max_risk_score=float(max_risk) if max_risk is not None else None,
            run_id=str(run_id) if run_id else None,
        )

    resolve_composite_outbreaks_not_in(conn, active_composite)


def check_outbreak_single(conn, notifier: SlackNotifier | None = None) -> None:
    """Single preferred detector (moving_avg / ewma) flagged high_risk."""
    if not OUTBREAK_ALERT_SINGLE_MODEL:
        resolve_single_outbreaks_not_in(conn, set())
        return
    _in = ", ".join(["%s"] * len(SINGLE_ALERT_MODELS))
    sql = f"""
        WITH latest_rows AS (
            SELECT DISTINCT ON (bucket_start, region, model_version)
                bucket_start,
                region,
                model_version,
                risk_score,
                run_id,
                predicted_label,
                prediction_timestamp
            FROM ml_outbreak_predictions
            WHERE predicted_label = 'high_risk'
              AND bucket_start >= now() - (%s || ' hours')::interval
              AND model_version IN ({_in})
            ORDER BY bucket_start, region, model_version, prediction_timestamp DESC
        ),
        grouped AS (
            SELECT
                bucket_start,
                region,
                array_agg(DISTINCT model_version ORDER BY model_version) AS models,
                COUNT(DISTINCT model_version)::int AS n_models,
                MAX(risk_score) AS max_risk,
                MAX(run_id) AS sample_run_id
            FROM latest_rows
            GROUP BY bucket_start, region
            HAVING COUNT(DISTINCT model_version) = 1
        )
        SELECT bucket_start, region, models, n_models, max_risk, sample_run_id
        FROM grouped
        ORDER BY bucket_start DESC, region;
    """
    params = (OUTBREAK_LOOKBACK_HOURS, *SINGLE_ALERT_MODELS)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    active_single: set[str] = set()
    for bucket_start, region, *_ in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        active_single.add(f"outbreak_single:{region}:{bucket_iso}")

    for bucket_start, region, models, _n_models, max_risk, run_id in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        key = f"outbreak_single:{region}:{bucket_iso}"
        mv = list(models) if models else []
        emit_alert(
            conn,
            notifier,
            alert_key=key,
            alert_type="outbreak_risk",
            severity="info",
            message=(
                f"Single-detector signal: {region} @ {bucket_iso} — "
                f"{mv[0] if mv else 'unknown'}"
            ),
            raw_payload={
                "region": region,
                "bucket_start": bucket_iso,
                "model_versions": mv,
                "max_risk_score": float(max_risk) if max_risk is not None else None,
                "run_id": run_id,
                "reason": "Single preferred detector flagged high_risk.",
            },
            region=region,
            bucket_start=bucket_start,
            model_versions=mv,
            max_risk_score=float(max_risk) if max_risk is not None else None,
            run_id=str(run_id) if run_id else None,
        )

    resolve_single_outbreaks_not_in(conn, active_single)


# ---------------------------------------------------------------------------
# DB metric rules (event volume, average severity)
# ---------------------------------------------------------------------------


def query_event_count(conn, minutes: int) -> int:
    """Count rows in ``raw_typed_events`` whose ``event_ts`` is within the
    last ``minutes`` minutes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM raw_typed_events
            WHERE event_ts >= now() - (%s || ' minutes')::interval
            """,
            (minutes,),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def query_avg_severity(conn, minutes: int) -> float | None:
    """Average normalized severity over the last ``minutes`` minutes.

    Severity strings (``mild``/``moderate``/``severe``/``critical``) map to
    1..4; numeric strings are parsed as-is. Rows without a severity field
    are excluded.
    """
    sql = """
        SELECT AVG(numeric_severity) FROM (
            SELECT
                CASE LOWER(payload_json->>'severity')
                    WHEN 'mild' THEN 1.0
                    WHEN 'moderate' THEN 2.0
                    WHEN 'severe' THEN 3.0
                    WHEN 'critical' THEN 4.0
                    ELSE
                        CASE
                            WHEN payload_json->>'severity' ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN (payload_json->>'severity')::numeric
                            ELSE NULL
                        END
                END AS numeric_severity
            FROM raw_typed_events
            WHERE event_ts >= now() - (%s || ' minutes')::interval
              AND payload_json ? 'severity'
        ) s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (minutes,))
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def query_data_quality_issue_pct(conn, minutes: int) -> float | None:
    """Share of landed typed events in the window with completeness or severity-parse issues."""
    sql = """
        SELECT
            CASE
                WHEN COUNT(*) = 0 THEN NULL::double precision
                ELSE 100.0 * COUNT(*) FILTER (
                    WHERE (event_type IS NULL OR trim(event_type) = '')
                       OR event_ts IS NULL
                       OR NOT (payload_json ? 'region')
                       OR trim(COALESCE(payload_json->>'region', '')) = ''
                       OR NOT (payload_json ? 'severity')
                       OR trim(COALESCE(payload_json->>'severity', '')) = ''
                       OR (
                           payload_json ? 'severity'
                           AND trim(COALESCE(payload_json->>'severity', '')) <> ''
                           AND lower(trim(payload_json->>'severity')) NOT IN (
                               'mild', 'moderate', 'severe', 'critical'
                           )
                           AND NOT (payload_json->>'severity' ~ '^[0-9]+(\\.[0-9]+)?$')
                       )
                )::double precision / COUNT(*)::double precision
            END
        FROM raw_typed_events
        WHERE COALESCE(event_ts, ingested_at) >= now() - (%s || ' minutes')::interval
    """
    with conn.cursor() as cur:
        cur.execute(sql, (minutes,))
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def query_missing_severity_pct(conn, minutes: int) -> float | None:
    """Share of landed rows in the window with missing ``severity`` in payload_json."""
    sql = """
        SELECT
            CASE
                WHEN COUNT(*) = 0 THEN NULL::double precision
                ELSE 100.0 * COUNT(*) FILTER (
                    WHERE NOT (payload_json ? 'severity')
                       OR trim(COALESCE(payload_json->>'severity', '')) = ''
                )::double precision / COUNT(*)::double precision
            END
        FROM raw_typed_events
        WHERE COALESCE(event_ts, ingested_at) >= now() - (%s || ' minutes')::interval
    """
    with conn.cursor() as cur:
        cur.execute(sql, (minutes,))
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def evaluate_metric_rule(
    rule_name: str,
    rule: dict[str, Any],
    measurements: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Decide whether a rule fires given precomputed window measurements.

    Returns the result payload for ``emit_alert`` when the rule breaches its
    consistency threshold, otherwise ``None``.
    """
    if not measurements:
        return None
    breaches = [m for m in measurements if m.get("breach")]
    min_breaching = int(rule.get("min_windows_breaching", len(measurements)))
    min_breaching = max(1, min(min_breaching, len(measurements)))
    if len(breaches) < min_breaching:
        return None
    return {
        "rule_name": rule_name,
        "metric": rule["metric"],
        "severity": rule.get("severity", "warning"),
        "measurements": measurements,
        "n_breaches": len(breaches),
        "min_windows_breaching": min_breaching,
    }


def _measure_event_count(conn, windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for window in windows:
        minutes = int(window["minutes"])
        threshold = float(window["threshold"])
        value = query_event_count(conn, minutes)
        measurements.append(
            {
                "window": window.get("name") or f"{minutes}m",
                "minutes": minutes,
                "value": value,
                "threshold": threshold,
                "breach": value > threshold,
            }
        )
    return measurements


def _measure_avg_severity(conn, windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for window in windows:
        minutes = int(window["minutes"])
        threshold = float(window["threshold"])
        value = query_avg_severity(conn, minutes)
        breach = value is not None and value > threshold
        measurements.append(
            {
                "window": window.get("name") or f"{minutes}m",
                "minutes": minutes,
                "value": None if value is None else round(float(value), 3),
                "threshold": threshold,
                "breach": breach,
            }
        )
    return measurements


def _format_metric_message(rule_name: str, result: dict[str, Any]) -> str:
    breaches = [m for m in result["measurements"] if m.get("breach")]
    parts: list[str] = []
    for m in breaches:
        v = m.get("value")
        v_s = "N/A" if v is None else str(v)
        parts.append(f"{m['window']}={v_s}>{m['threshold']}")
    return (
        f"{rule_name}: {result['n_breaches']}/{len(result['measurements'])} "
        f"windows breached ({', '.join(parts)})"
    )


def _measure_data_quality_issue_pct(
    conn, windows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for window in windows:
        minutes = int(window["minutes"])
        threshold = float(window["threshold"])
        value = query_data_quality_issue_pct(conn, minutes)
        measurements.append(
            {
                "window": window.get("name") or f"{minutes}m",
                "minutes": minutes,
                "value": None if value is None else round(float(value), 3),
                "threshold": threshold,
                "breach": value is not None and value > threshold,
            }
        )
    return measurements


def _measure_missing_severity_pct(
    conn, windows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for window in windows:
        minutes = int(window["minutes"])
        threshold = float(window["threshold"])
        value = query_missing_severity_pct(conn, minutes)
        measurements.append(
            {
                "window": window.get("name") or f"{minutes}m",
                "minutes": minutes,
                "value": None if value is None else round(float(value), 3),
                "threshold": threshold,
                "breach": value is not None and value > threshold,
            }
        )
    return measurements


def check_metric_rules(
    conn,
    notifier: SlackNotifier | None,
    rules: dict[str, dict[str, Any]],
) -> None:
    for rule_name, rule in rules.items():
        metric = rule.get("metric")
        windows = rule.get("windows") or []
        if not windows or metric not in (
            "event_count",
            "avg_severity",
            "data_quality_issue_pct",
            "missing_severity_pct",
        ):
            logger.warning("skipping rule %s: invalid metric/windows", rule_name)
            continue

        alert_key = f"metric:{rule_name}"
        try:
            if metric == "event_count":
                measurements = _measure_event_count(conn, windows)
            elif metric == "avg_severity":
                measurements = _measure_avg_severity(conn, windows)
            elif metric == "data_quality_issue_pct":
                measurements = _measure_data_quality_issue_pct(conn, windows)
            elif metric == "missing_severity_pct":
                measurements = _measure_missing_severity_pct(conn, windows)
        except psycopg2.Error as exc:
            # Roll back so the connection stays usable for downstream rules
            # (e.g. when raw_typed_events is missing on a fresh DB).
            conn.rollback()
            logger.warning("rule %s evaluation failed: %s", rule_name, exc)
            continue

        result = evaluate_metric_rule(rule_name, rule, measurements)
        if result is None:
            resolve_pipeline_alert(conn, alert_key)
            continue

        message = _format_metric_message(rule_name, result)
        emit_alert(
            conn,
            notifier,
            alert_key=alert_key,
            alert_type=metric,
            severity=result["severity"],
            message=message,
            raw_payload={
                "rule_name": rule_name,
                "metric": metric,
                "measurements": measurements,
                "n_breaches": result["n_breaches"],
                "min_windows_breaching": result["min_windows_breaching"],
            },
            cooldown_seconds=rule.get("cooldown_seconds"),
        )


# ---------------------------------------------------------------------------
# Cycle / main loop
# ---------------------------------------------------------------------------


def build_notifier() -> SlackNotifier | None:
    if not SLACK_ENABLED:
        return None
    if not SLACK_WEBHOOK_URL:
        logger.info("SLACK_ENABLED=1 but SLACK_WEBHOOK_URL is empty; Slack disabled")
        return None
    try:
        return SlackNotifier(
            webhook_url=SLACK_WEBHOOK_URL,
            channel_name=SLACK_CHANNEL_NAME,
            critical_mention=SLACK_CRITICAL_MENTION,
            timeout_seconds=SLACK_TIMEOUT_SECONDS,
        )
    except ValueError as exc:
        logger.warning("Slack notifier disabled: %s", exc)
        return None


def run_cycle(
    conn,
    notifier: SlackNotifier | None = None,
    rules: dict[str, dict[str, Any]] | None = None,
) -> None:
    ensure_tables(conn)
    check_stale_predictions(conn, notifier)
    check_feature_freshness(conn, notifier)
    check_outbreak_composite(conn, notifier)
    check_outbreak_single(conn, notifier)
    check_metric_rules(conn, notifier, rules or {})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Epidemic alert service")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle and exit (for cron / smoke tests).",
    )
    parser.add_argument(
        "--test-slack",
        action="store_true",
        help="Send a single Slack smoke-test message and exit.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.test_slack:
        notifier = build_notifier()
        if notifier is None:
            print(
                "Slack is not configured (SLACK_ENABLED=0 or SLACK_WEBHOOK_URL empty).",
                file=sys.stderr,
            )
            return 1
        ok = notifier.send_test_message()
        if ok:
            print(f"Slack test message sent to {SLACK_CHANNEL_NAME}.")
            return 0
        print("Slack test message failed; see logs.", file=sys.stderr)
        return 1

    if not DATABASE_URL:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    notifier = build_notifier()
    rules = load_rules()
    dsn = normalize_db_url(DATABASE_URL)

    while True:
        try:
            conn = psycopg2.connect(dsn)
            try:
                run_cycle(conn, notifier, rules)
            finally:
                conn.close()
        except Exception as exc:
            print(f"alert-service cycle failed: {exc}", file=sys.stderr, flush=True)

        if args.once:
            return 0

        time.sleep(ALERT_POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
