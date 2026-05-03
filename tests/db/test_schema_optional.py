"""Optional live-DB checks for raw landing and batch window metadata."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.db


def test_raw_typed_events_exists(pg_connection):
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'raw_typed_events'
            """
        )
        assert cur.fetchone() is not None, "Apply raw landing DDL for raw_typed_events first"


def test_raw_typed_events_primary_key_columns(pg_connection):
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = 'raw_typed_events'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """
        )
        cols = [r[0] for r in cur.fetchall()]
    assert cols == ["topic", "kafka_partition", "kafka_offset"]


def test_batch_window_runs_primary_key(pg_connection):
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = 'batch_window_runs'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """
        )
        cols = [r[0] for r in cur.fetchall()]
    assert cols == ["window_start", "run_id"]


def test_dirty_window_query_syntax(pg_connection):
    """Exercise the dirty-window SQL shape used for raw_db hourly refresh (read-only)."""
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            WITH latest_success AS (
              SELECT window_start, MAX(completed_at) AS completed_at
              FROM batch_window_runs
              WHERE status = 'succeeded'
              GROUP BY window_start
            ),
            raw_hours AS (
              SELECT
                date_trunc('hour', event_ts) AS window_start,
                MAX(ingested_at) AS last_ingested_at
              FROM raw_typed_events
              WHERE event_ts IS NOT NULL
              GROUP BY 1
            ),
            dirty AS (
              SELECT r.window_start
              FROM raw_hours r
              LEFT JOIN latest_success s USING (window_start)
              WHERE s.completed_at IS NULL OR r.last_ingested_at > s.completed_at
              ORDER BY r.window_start
              LIMIT GREATEST(1::int, 1)
            )
            SELECT COUNT(*) FROM dirty
            """
        )
        cur.fetchone()
