"""Shared pytest configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_ingest_script_module():
    """Load `scripts/ingest-typed-events-to-db.py` as a module (not on PYTHONPATH)."""
    path = REPO_ROOT / "scripts" / "ingest-typed-events-to-db.py"
    spec = importlib.util.spec_from_file_location("ingest_typed_events_to_db", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load ingest script from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def ingest_module():
    return load_ingest_script_module()
