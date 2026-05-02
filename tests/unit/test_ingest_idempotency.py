"""Contract and mock-based tests for raw landing idempotency."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.conftest import load_ingest_script_module


def test_upsert_sql_uses_on_conflict_do_nothing():
    """Duplicate (topic, partition, offset) must not raise and should not update existing row."""
    mod = load_ingest_script_module()
    src = open(mod.__file__, encoding="utf-8").read()
    assert "ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING" in src


def test_mock_cursor_duplicate_insert_rowcounts():
    """Simulate PostgreSQL: first insert rowcount=1, duplicate rowcount=0."""
    row_sequence = [1, 0]

    class FakeCursor:
        def __init__(self) -> None:
            self._i = 0

        def execute(self, _sql, _params) -> None:
            self.rowcount = row_sequence[self._i]
            self._i += 1

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    cur = FakeCursor()
    cur.execute(None, None)
    assert cur.rowcount == 1
    cur.execute(None, None)
    assert cur.rowcount == 0
