"""Verify files required by the canonical cluster smoke: run-phase1-rawdb-sequenced.sh (offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[2]

# Canonical smoke: scripts/run-phase1-rawdb-sequenced.sh — plus helpers and infra it invokes or assumes.
REQUIRED_FOR_RAWDB_SEQUENCED_SMOKE = [
    "scripts/run-phase1-rawdb-sequenced.sh",
    "scripts/run-raw-typed-ingester.sh",
    "scripts/run-rawdb-feature-refresh.sh",
    "tekton/pipeline.yaml",
    "infra/spark-job.yaml",
]


@pytest.mark.parametrize("relative", REQUIRED_FOR_RAWDB_SEQUENCED_SMOKE)
def test_rawdb_sequenced_smoke_dependency_exists(relative: str):
    path = REPO_ROOT / relative
    assert path.is_file(), f"Missing {relative} (needed for run-phase1-rawdb-sequenced.sh)"
