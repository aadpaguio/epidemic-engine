# Testing plan

This repository uses **pytest** with optional **PySpark** (local mode) and optional **Postgres** integration tests. Install and run everything via **[uv](https://github.com/astral-sh/uv)** so dependencies resolve from [`pyproject.toml`](../pyproject.toml).

## Install (uv)

From the repo root:

```bash
uv sync --extra dev
```

This installs runtime dependencies plus `pytest` and `pyspark` from the `dev` optional group.

## Tier 1 — Fast unit tests (no Spark, no DB)

Run on every commit:

```bash
uv run pytest tests/unit -q
```

Includes ML detector tests and raw-ingester parsing helpers.

## Tier 2 — Spark local tests

Requires PySpark (installed by `uv sync --extra dev`) and a **Java runtime** on `PATH` (Spark starts a JVM). If Java is missing, DataFrame tests in `tests/spark/test_etl_pipeline_local.py` are **skipped**; pure-function tests in `tests/spark/test_etl_functions.py` still run.

```bash
uv run pytest tests/spark -m spark -q
```

Or run the whole spark folder:

```bash
uv run pytest tests/spark -q
```

## Tier 3 — Optional DB integration

Point at a database that already has the epidemic schema (for example after `oc apply` / `scripts/apply-raw-landing-ddl.sh` on your Timescale pod, or a local Postgres with the same DDL):

```bash
export EPIDEMIC_TEST_PG_URL='postgresql://USER:PASSWORD@HOST:5432/epidemic'
uv run pytest tests/db -m db -q
```

If `EPIDEMIC_TEST_PG_URL` is unset, DB tests **skip**.

## Tier 4 — Cluster smoke (one canonical script)

**Canonical end-to-end smoke** for the current pipeline (Phase 1 Tekton → typed topics → `raw_typed_events` via ingester → Spark `FEATURE_SOURCE_MODE=raw_db` refresh) is:

[`scripts/run-phase1-rawdb-sequenced.sh`](../scripts/run-phase1-rawdb-sequenced.sh)

Run it live after `oc login` and `oc project <namespace>`, with env vars as in [`continuous-collection-plan.md`](continuous-collection-plan.md) (for example `DURATION_SEC`, `BATCH_SIZE`, `INGEST_RUN_SECONDS`). One-off prerequisites: DB/RBAC/Spark script ConfigMap, `raw_typed_events` DDL (`scripts/apply-raw-landing-ddl.sh`), and secret `epidemic-db-spark`.

Offline, pytest only checks that this orchestrator and the scripts it calls still exist in the repo:

```bash
uv run pytest tests/cluster -m cluster -q
```

Older scripts such as `smoke-phase1-topics.sh` (topics-only) and `smoke-phase2-db.sh` (typed Kafka → Spark, **no** raw ingest) are optional debugging aids; they do not replace the sequenced raw-landing smoke above.

## Run everything (default pytest selection)

```bash
uv run pytest tests/unit tests/spark tests/cluster -q
```

(DB tier omitted unless `EPIDEMIC_TEST_PG_URL` is set.)

## Legacy unittest

Detector tests remain `unittest`-based under `tests/unit/`; they are collected by pytest and can still be run with:

```bash
uv run python -m unittest discover -s tests/unit
```

## Pre-submission checklist

1. `uv sync --extra dev`
2. `uv run pytest tests/unit tests/spark tests/cluster -q`
3. If you have DB access: set `EPIDEMIC_TEST_PG_URL` and run `uv run pytest tests/db -m db -q`
4. Before demos on cluster: run [`scripts/run-phase1-rawdb-sequenced.sh`](../scripts/run-phase1-rawdb-sequenced.sh) (short `DURATION_SEC` smoke is fine)
