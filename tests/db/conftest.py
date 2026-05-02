"""Gate DB tests on EPIDEMIC_TEST_PG_URL (postgresql://user:pass@host:port/dbname)."""

from __future__ import annotations

import os

import pytest


def _pg_url() -> str | None:
    return os.environ.get("EPIDEMIC_TEST_PG_URL", "").strip() or None


@pytest.fixture(scope="session")
def pg_connection():
    url = _pg_url()
    if not url:
        pytest.skip("Set EPIDEMIC_TEST_PG_URL to run DB integration tests")

    import psycopg2

    conn = psycopg2.connect(url)
    yield conn
    conn.close()
