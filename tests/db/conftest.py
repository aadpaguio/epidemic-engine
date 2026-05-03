"""Gate DB tests on a Postgres URL or localhost port-forward env vars (see docs/local-db-connection.md)."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
try:
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass


def _pg_url() -> str | None:
    direct = os.environ.get("EPIDEMIC_TEST_PG_URL", "").strip()
    if direct:
        return direct

    host = os.environ.get("DB_HOST", "").strip()
    user = os.environ.get("DB_USER", "").strip()
    password = os.environ.get("DB_PASSWORD", "").strip()
    dbname = os.environ.get("DB_NAME", "epidemic").strip()
    port = os.environ.get("DB_PORT", "5432").strip()
    if host and user and password:
        return (
            f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{dbname}"
        )
    return None


@pytest.fixture(scope="session")
def pg_connection():
    url = _pg_url()
    if not url:
        pytest.skip(
            "Set EPIDEMIC_TEST_PG_URL or DB_HOST, DB_USER, DB_PASSWORD, DB_NAME "
            "(port-forward: oc port-forward svc/timescaledb-service 5432:5432)"
        )

    import psycopg2

    conn = psycopg2.connect(url)
    yield conn
    conn.close()
