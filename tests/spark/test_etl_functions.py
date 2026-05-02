"""Pure-function tests for `spark/etl.py` (no SparkSession required)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.spark

import etl  # noqa: E402


def test_parse_utc_window_accepts_z():
    dt = etl.parse_utc_window("2026-04-30T12:00:00Z", "WINDOW_START_UTC")
    assert dt.tzinfo is not None
    assert dt.hour == 12


def test_parse_utc_window_rejects_naive():
    with pytest.raises(RuntimeError, match="timezone"):
        etl.parse_utc_window("2026-04-30T12:00:00", "WINDOW_START_UTC")


def test_resolve_window_requires_both_bounds(monkeypatch):
    monkeypatch.setenv("REQUIRE_WINDOW_UTC", "true")
    monkeypatch.setenv("WINDOW_START_UTC", "2026-04-30T12:00:00Z")
    monkeypatch.setenv("WINDOW_END_UTC", "2026-04-30T13:00:00Z")
    ws, we = etl.resolve_window()
    assert ws < we


def test_resolve_window_rejects_end_before_start(monkeypatch):
    monkeypatch.setenv("REQUIRE_WINDOW_UTC", "true")
    monkeypatch.setenv("WINDOW_START_UTC", "2026-04-30T14:00:00Z")
    monkeypatch.setenv("WINDOW_END_UTC", "2026-04-30T13:00:00Z")
    with pytest.raises(RuntimeError, match="WINDOW_END_UTC"):
        etl.resolve_window()


def test_iter_hour_windows_multi_hour():
    start = datetime(2026, 4, 30, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 30, 13, 0, tzinfo=timezone.utc)
    wins = etl.iter_hour_windows(start, end)
    assert len(wins) == 3
    assert wins[0][0] == start.replace(minute=0, second=0, microsecond=0)


def test_parse_jdbc_postgres_url():
    host, port, db = etl.parse_jdbc_postgres_url("jdbc:postgresql://db.example:5432/epidemic")
    assert host == "db.example"
    assert port == 5432
    assert db == "epidemic"


def test_classify_exception_maps_kafka():
    exc = Exception("connection to kafka broker failed")
    assert etl.classify_exception(exc) == "KAFKA_READ_FAILED"


def test_classify_exception_maps_db_connect():
    exc = Exception("jdbc postgresql timeout")
    assert etl.classify_exception(exc) == "DB_CONNECT_FAILED"


def test_classify_exception_maps_db_write():
    exc = Exception("jdbc postgresql duplicate key violates constraint")
    assert etl.classify_exception(exc) == "DB_WRITE_FAILED"


def test_log_analytics_requires_keys(monkeypatch):
    monkeypatch.delenv("K8S_POD_NAME", raising=False)
    monkeypatch.delenv("HOSTNAME", raising=False)
    with pytest.raises(ValueError, match="missing key"):
        etl.log_analytics({"task": "x", "phase": "y"})


def test_record_window_status_executes_insert():
    fake_cur = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_conn.cursor.return_value.__exit__.return_value = False
    fake_conn.__enter__.return_value = fake_conn
    fake_conn.__exit__.return_value = False

    jdbc_url = "jdbc:postgresql://localhost:5432/epidemic"
    ws = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    we = datetime(2026, 4, 30, 13, 0, tzinfo=timezone.utc)

    with patch("psycopg2.connect", return_value=fake_conn):
        etl.record_window_status(
            jdbc_url,
            "u",
            "p",
            "run-1",
            "batch-1",
            ws,
            we,
            "running",
            None,
        )

    fake_cur.execute.assert_called_once()


def test_record_window_status_range_writes_multiple_hours():
    fake_cur = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_conn.cursor.return_value.__exit__.return_value = False
    fake_conn.__enter__.return_value = fake_conn
    fake_conn.__exit__.return_value = False

    ws = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    we = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)

    with patch("psycopg2.connect", return_value=fake_conn):
        etl.record_window_status_range(
            "jdbc:postgresql://localhost:5432/epidemic",
            "u",
            "p",
            "run-1",
            "batch-1",
            ws,
            we,
            "succeeded",
            None,
        )

    assert fake_cur.execute.call_count == 2
