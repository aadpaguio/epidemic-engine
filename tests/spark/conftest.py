"""Ensure `spark/etl.py` is importable as `etl` and provide a local Spark session."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPARK_SRC = REPO_ROOT / "spark"
if str(SPARK_SRC) not in sys.path:
    sys.path.insert(0, str(SPARK_SRC))


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("epidemic-etl-test")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except (PySparkRuntimeError, Exception) as exc:
        pytest.skip(
            "Local Spark session requires a working Java runtime (install a JDK and ensure "
            f"java is on PATH). Underlying error: {exc!r}"
        )
    yield spark
    spark.stop()
