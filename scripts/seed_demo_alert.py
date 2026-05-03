#!/usr/bin/env python3
"""
Insert (or refresh) a demo row in alert_events for the Streamlit Alerts page.

Uses the same DB_* env vars as app/dashboard.py (via .env in repo root).
After seeding, click "Refresh Data" in the sidebar or wait up to ~60s for cache.

Usage (from repo root):
  PYTHONPATH=. python scripts/seed_demo_alert.py
  PYTHONPATH=. python scripts/seed_demo_alert.py --delete   # remove demo row
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

load_dotenv(REPO_ROOT / ".env")

import psycopg2

from alert.alert_service import ensure_tables, upsert_alert

DEMO_ALERT_KEY = "demo:streamlit:sample-outbreak"


def connect():
    host = os.getenv("DB_HOST", "localhost").strip()
    port = int(os.getenv("DB_PORT", "5432").strip() or "5432")
    name = os.getenv("DB_NAME", "epidemic").strip()
    user = os.getenv("DB_USER", "spark_writer").strip()
    password = os.getenv("DB_PASSWORD", "")
    if not password:
        raise SystemExit("DB_PASSWORD is not set (check .env)")
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
        connect_timeout=10,
    )


def seed() -> None:
    bucket = datetime(2026, 4, 30, 18, 0, 0, tzinfo=timezone.utc)
    payload = {
        "component": "alert-service",
        "alert_type": "outbreak_risk",
        "severity": "warning",
        "region": "Boston",
        "bucket_start": bucket.isoformat(),
        "model_versions": ["ewma_v1", "moving_avg_k6_v1"],
        "n_models": 2,
        "max_risk_score": 1.0,
        "run_id": "demo-run-streamlit",
        "reason": "Demo seed row (docs/alerting-guide.md sample shape).",
        "alert_key": DEMO_ALERT_KEY,
    }
    conn = connect()
    try:
        ensure_tables(conn)
        upsert_alert(
            conn,
            alert_key=DEMO_ALERT_KEY,
            alert_type="outbreak_risk",
            severity="warning",
            region="Boston",
            bucket_start=bucket,
            model_versions=["ewma_v1", "moving_avg_k6_v1"],
            max_risk_score=1.0,
            run_id="demo-run-streamlit",
            message=(
                "Demo: multiple detectors flagged high_risk for Boston @ "
                "2026-04-30 18:00 UTC (seed via scripts/seed_demo_alert.py)."
            ),
            raw_payload=payload,
        )
    finally:
        conn.close()
    print(f"Upserted alert_events row alert_key={DEMO_ALERT_KEY!r}")
    print("Open Streamlit → Alerts → click Refresh Data (or wait ~60s).")


def delete_demo() -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM alert_events WHERE alert_key = %s",
                (DEMO_ALERT_KEY,),
            )
        conn.commit()
    finally:
        conn.close()
    print(f"Deleted alert_events row alert_key={DEMO_ALERT_KEY!r}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--delete",
        action="store_true",
        help="Remove the demo alert row",
    )
    args = p.parse_args()
    if args.delete:
        delete_demo()
    else:
        seed()


if __name__ == "__main__":
    main()
