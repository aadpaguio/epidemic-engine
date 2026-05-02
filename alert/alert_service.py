#!/usr/bin/env python3
"""
Poll TimescaleDB for pipeline freshness and outbreak signals; write alert_events;
emit structured [ALERT] logs.

Environment variables (defaults match docs/alerting-guide.md):
  DATABASE_URL — required (postgresql+psycopg2://...)
  ALERT_POLL_SECONDS — default 300
  ALERT_COOLDOWN_SECONDS — default 1800
  PREDICTION_STALE_MINUTES — default 180
  FEATURE_STALE_HOURS — default 12
  OUTBREAK_LOOKBACK_HOURS — default 24
  MIN_CONFIRMING_MODELS — default 2
  OUTBREAK_ALERT_SINGLE_MODEL — if "1", emit info-level single-detector outbreak alerts (default 0)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json

DATABASE_URL = os.getenv("DATABASE_URL")
ALERT_POLL_SECONDS = int(os.getenv("ALERT_POLL_SECONDS", "300"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "1800"))
PREDICTION_STALE_MINUTES = int(os.getenv("PREDICTION_STALE_MINUTES", "180"))
FEATURE_STALE_HOURS = int(os.getenv("FEATURE_STALE_HOURS", "12"))
OUTBREAK_LOOKBACK_HOURS = int(os.getenv("OUTBREAK_LOOKBACK_HOURS", "24"))
MIN_CONFIRMING_MODELS = int(os.getenv("MIN_CONFIRMING_MODELS", "2"))
OUTBREAK_ALERT_SINGLE_MODEL = os.getenv(
    "OUTBREAK_ALERT_SINGLE_MODEL", "0"
).lower() in ("1", "true", "yes")

PREFERRED_MODELS = ("moving_avg_k6_v1", "ewma_v1", "kalman_v1", "v1")
SINGLE_ALERT_MODELS = ("moving_avg_k6_v1", "ewma_v1")


def normalize_db_url(url: str) -> str:
    u = url.strip()
    if u.startswith("postgresql+psycopg2://"):
        u = "postgresql://" + u[len("postgresql+psycopg2://") :]
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


def cooldown_allows(conn, alert_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT updated_at FROM alert_events WHERE alert_key = %s",
            (alert_key,),
        )
        row = cur.fetchone()
    if not row:
        return True
    last = row[0]
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age >= ALERT_COOLDOWN_SECONDS


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
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_events (
                alert_key, alert_type, severity, region, bucket_start,
                model_versions, max_risk_score, run_id, status, message, raw_payload, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, now()
            )
            ON CONFLICT (alert_key) DO UPDATE SET
                updated_at = now(),
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


def check_stale_predictions(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(prediction_timestamp) FROM ml_outbreak_predictions"
        )
        row = cur.fetchone()
    latest = row[0] if row else None
    if latest is None:
        key = "pipeline:no_predictions"
        if cooldown_allows(conn, key):
            payload = {
                "component": "alert-service",
                "alert_type": "prediction_freshness",
                "severity": "critical",
                "reason": "ml_outbreak_predictions is empty",
                "alert_key": key,
            }
            upsert_alert(
                conn,
                alert_key=key,
                alert_type="prediction_freshness",
                severity="critical",
                region=None,
                bucket_start=None,
                model_versions=None,
                max_risk_score=None,
                run_id=None,
                message="No rows in ml_outbreak_predictions",
                raw_payload=payload,
            )
            log_alert(payload)
        return

    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - latest).total_seconds() / 60.0
    if age_min > PREDICTION_STALE_MINUTES:
        key = "pipeline:stale_predictions"
        if cooldown_allows(conn, key):
            payload = {
                "component": "alert-service",
                "alert_type": "prediction_freshness",
                "severity": "critical",
                "reason": "predictions older than threshold",
                "latest_prediction_timestamp": latest.isoformat(),
                "age_minutes": round(age_min, 1),
                "threshold_minutes": PREDICTION_STALE_MINUTES,
                "alert_key": key,
            }
            upsert_alert(
                conn,
                alert_key=key,
                alert_type="prediction_freshness",
                severity="critical",
                region=None,
                bucket_start=None,
                model_versions=None,
                max_risk_score=None,
                run_id=None,
                message=(
                    f"No prediction updates in {age_min:.0f} minutes "
                    f"(threshold {PREDICTION_STALE_MINUTES})"
                ),
                raw_payload=payload,
            )
            log_alert(payload)


def check_feature_freshness(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(bucket_start)
            FROM vw_region_hourly_ml_features_windowed
            """
        )
        row = cur.fetchone()
    latest = row[0] if row else None
    if latest is None:
        return
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0
    if age_h > FEATURE_STALE_HOURS:
        key = "pipeline:stale_features"
        if cooldown_allows(conn, key):
            payload = {
                "component": "alert-service",
                "alert_type": "feature_freshness",
                "severity": "warning",
                "reason": "ML feature view buckets are stale",
                "latest_bucket_start": latest.isoformat(),
                "age_hours": round(age_h, 2),
                "threshold_hours": FEATURE_STALE_HOURS,
                "alert_key": key,
            }
            upsert_alert(
                conn,
                alert_key=key,
                alert_type="feature_freshness",
                severity="warning",
                region=None,
                bucket_start=latest,
                model_versions=None,
                max_risk_score=None,
                run_id=None,
                message=(
                    f"Latest feature bucket is {age_h:.1f}h old "
                    f"(threshold {FEATURE_STALE_HOURS}h)"
                ),
                raw_payload=payload,
            )
            log_alert(payload)


def check_outbreak_composite(conn) -> None:
    """Alert when MIN_CONFIRMING_MODELS+ distinct model_versions flag high_risk for same bucket/region."""
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

    for bucket_start, region, models, n_models, max_risk, run_id in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        key = f"outbreak:{region}:{bucket_iso}:composite"
        if not cooldown_allows(conn, key):
            continue
        severity = "critical" if n_models >= 3 else "warning"
        payload = {
            "component": "alert-service",
            "alert_type": "outbreak_risk",
            "severity": severity,
            "region": region,
            "bucket_start": bucket_iso,
            "model_versions": list(models) if models else [],
            "n_models": n_models,
            "max_risk_score": float(max_risk) if max_risk is not None else None,
            "run_id": run_id,
            "reason": "Multiple detectors flagged high_risk for the same region bucket.",
            "alert_key": key,
        }
        upsert_alert(
            conn,
            alert_key=key,
            alert_type="outbreak_risk",
            severity=severity,
            region=region,
            bucket_start=bucket_start,
            model_versions=list(models) if models else [],
            max_risk_score=float(max_risk) if max_risk is not None else None,
            run_id=str(run_id) if run_id else None,
            message=(
                f"Outbreak signal: {region} @ {bucket_iso} — "
                f"{n_models} models agree: {', '.join(models)}"
            ),
            raw_payload=payload,
        )
        log_alert(payload)


def check_outbreak_single(conn) -> None:
    """Single preferred detector (moving_avg / ewma) high_risk — lower severity."""
    if not OUTBREAK_ALERT_SINGLE_MODEL:
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

    for bucket_start, region, models, _n_models, max_risk, run_id in rows:
        bucket_iso = bucket_start.isoformat() if bucket_start else "unknown"
        key = f"outbreak_single:{region}:{bucket_iso}"
        if not cooldown_allows(conn, key):
            continue
        mv = list(models) if models else []
        payload = {
            "component": "alert-service",
            "alert_type": "outbreak_risk",
            "severity": "info",
            "region": region,
            "bucket_start": bucket_iso,
            "model_versions": mv,
            "max_risk_score": float(max_risk) if max_risk is not None else None,
            "run_id": run_id,
            "reason": "Single preferred detector flagged high_risk (enable multi-model for stronger signal).",
            "alert_key": key,
        }
        upsert_alert(
            conn,
            alert_key=key,
            alert_type="outbreak_risk",
            severity="info",
            region=region,
            bucket_start=bucket_start,
            model_versions=mv,
            max_risk_score=float(max_risk) if max_risk is not None else None,
            run_id=str(run_id) if run_id else None,
            message=(
                f"Single-detector signal: {region} @ {bucket_iso} — {mv[0] if mv else 'unknown'}"
            ),
            raw_payload=payload,
        )
        log_alert(payload)


def run_cycle(conn) -> None:
    ensure_tables(conn)
    check_stale_predictions(conn)
    check_feature_freshness(conn)
    check_outbreak_composite(conn)
    check_outbreak_single(conn)


def main() -> None:
    if not DATABASE_URL:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    dsn = normalize_db_url(DATABASE_URL)

    while True:
        try:
            conn = psycopg2.connect(dsn)
            try:
                run_cycle(conn)
            finally:
                conn.close()
        except Exception as exc:
            print(f"alert-service cycle failed: {exc}", file=sys.stderr, flush=True)

        time.sleep(ALERT_POLL_SECONDS)


if __name__ == "__main__":
    main()
