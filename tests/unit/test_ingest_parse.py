"""Unit tests for `scripts/ingest-typed-events-to-db.py` parsing helpers."""

from __future__ import annotations

from datetime import UTC

import pytest


def test_parse_event_ts_z_suffix(ingest_module):
    parse = ingest_module.parse_event_ts
    dt = parse({"timestamp": "2026-04-30T12:00:00Z"})
    assert dt is not None
    assert dt.tzinfo == UTC
    assert dt.hour == 12


def test_parse_event_ts_offset(ingest_module):
    parse = ingest_module.parse_event_ts
    dt = parse({"timestamp": "2026-04-30T12:00:00+00:00"})
    assert dt is not None
    assert dt.astimezone(UTC).hour == 12


def test_parse_event_ts_missing_returns_none(ingest_module):
    parse = ingest_module.parse_event_ts
    assert parse({}) is None
    assert parse({"timestamp": ""}) is None
    assert parse({"timestamp": 123}) is None


def test_parse_event_ts_invalid_returns_none(ingest_module):
    parse = ingest_module.parse_event_ts
    assert parse({"timestamp": "not-a-date"}) is None


def test_parse_event_ts_administered_at_not_used_by_ingester(ingest_module):
    """Raw ingester only reads `timestamp` for DB event_ts; docs flag vaccination edge cases."""
    parse = ingest_module.parse_event_ts
    assert parse({"administered_at": "2026-04-30T12:00:00Z"}) is None


def test_env_missing_raises(ingest_module):
    env = ingest_module.env
    with pytest.raises(RuntimeError, match="Missing required environment variable"):
        env("__MISSING_VAR_XYZ__")


def test_env_default(ingest_module):
    env = ingest_module.env
    assert env("__MISSING_USE_DEFAULT__", "fallback") == "fallback"


def test_env_strips(ingest_module, monkeypatch):
    monkeypatch.setenv("INGEST_TEST_STRIP", "  val  ")
    assert ingest_module.env("INGEST_TEST_STRIP") == "val"
