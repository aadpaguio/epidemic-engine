"""Offline Phase 1 smoke: Tekton pipeline + raw-DB Spark job contracts (no bash orchestrators)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    path = REPO_ROOT / rel
    assert path.is_file(), f"Missing {rel}"
    return path.read_text(encoding="utf-8")


def test_phase1_pipeline_manifest():
    text = _read("tekton/pipeline.yaml")
    assert "kind: Pipeline" in text
    assert "name: phase1-pipeline" in text


def test_phase1_spark_job_raw_db_contract():
    """Spark Job defaults for landed rows in Timescale (Phase 1 continuous collection path)."""
    text = _read("infra/spark-job.yaml")
    assert "kind: Job" in text
    assert "name: epidemic-spark-etl" in text
    assert "- name: FEATURE_SOURCE_MODE" in text
    assert '"raw_db"' in text
    assert "JDBC_URL" in text
    assert "spark-etl-script" in text
    assert "mountPath: /opt/epidemic" in text


def test_phase1_etl_entrypoint_present():
    assert (REPO_ROOT / "spark" / "etl.py").is_file()
