# Testing plan

This repository uses **pytest** with optional **PySpark** (local JVM) and optional **Postgres/Timescale** integration tests. Install and run via **[uv](https://github.com/astral-sh/uv)** so dependencies resolve from `[pyproject.toml](../pyproject.toml)`.

## What runs where


| Layer                                                            | What it is                                               | Needs cluster? |
| ---------------------------------------------------------------- | -------------------------------------------------------- | -------------- |
| `**pytest`** (`tests/`)                                          | Automated checks you can run locally or in CI            | No             |
| **Operational smokes** (`scripts/smoke-*.sh`, `run-phase1-*.sh`) | End-to-end jobs on OpenShift (Kafka, Tekton, Spark pods) | Yes            |


The automated suite **does not** execute bash smokes or `oc`; it validates code, manifests, and (optionally) a DB you reach yourself (for example via port-forward).

---

## Install (uv)

From the repo root:

```bash
uv sync --extra dev
```

This installs runtime dependencies plus `pytest` and `pyspark` from the `dev` optional group.

---

## Single command — full pytest suite

`[scripts/run-test-suite.sh](../scripts/run-test-suite.sh)` runs **all** tests under `tests/` with verbose output suitable for logs or submissions:

```bash
./scripts/run-test-suite.sh
```

Equivalent:

```bash
uv run pytest tests -v --tb=short -ra
```

- `**-ra**` includes *short test summary info* (skipped tests and reasons — useful when DB or Java is missing).
- Exit code **0** means every non-skipped test passed.

---

## Layout (`tests/`)


| Directory                             | Role                                                                                                                                                                                                                   |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `[tests/unit/](../tests/unit/)`       | Ingest parsing, SQL idempotency helpers, ML detector (`unittest` style; collected by pytest).                                                                                                                          |
| `[tests/spark/](../tests/spark/)`     | Pure ETL helpers in `[spark/etl.py](../spark/etl.py)`; optional DataFrame tests if Java is available.                                                                                                                  |
| `[tests/db/](../tests/db/)`           | Live Timescale checks (**skipped** unless credentials are configured — see below).                                                                                                                                     |
| `[tests/cluster/](../tests/cluster/)` | **Offline** Phase 1 contract tests on `[tekton/pipeline.yaml](../tekton/pipeline.yaml)` and `[infra/spark-job.yaml](../infra/spark-job.yaml)` (`FEATURE_SOURCE_MODE=raw_db`, JDBC, ConfigMap mount). No `oc` required. |


---

## Pytest markers

Registered in `[pytest.ini](../pytest.ini)`:


| Marker    | Meaning                                                                        |
| --------- | ------------------------------------------------------------------------------ |
| `spark`   | Uses PySpark (local); subset of `tests/spark/`.                                |
| `db`      | Needs a live Postgres URL / `.env` DB vars (`tests/db/`).                      |
| `cluster` | Offline manifest checks (`tests/cluster/`); **does not** connect to a cluster. |


Filter examples:

```bash
uv run pytest tests -m "not db" -q
uv run pytest tests/db -m db -v
```

---

## Tier 1 — Unit tests (no Spark, no DB)

```bash
uv run pytest tests/unit -q
```

---

## Tier 2 — Spark tests

Requires **PySpark** (`uv sync --extra dev`) and a **Java runtime** on `PATH` for `[tests/spark/test_etl_pipeline_local.py](../tests/spark/test_etl_pipeline_local.py)`. If Java is missing, those tests are **skipped**; `[tests/spark/test_etl_functions.py](../tests/spark/test_etl_functions.py)` still runs (no JVM session).

```bash
uv run pytest tests/spark -q
```

---

## Tier 3 — DB integration (`tests/db/`)

These tests assert tables, primary keys, and the dirty-window SQL shape against a database that already has the epidemic schema.

### When tests skip

If neither `**EPIDEMIC_TEST_PG_URL**` nor a complete `**.env**` DB configuration is present, all four DB tests **skip** with a message pointing here. That is expected for offline runs.

### Option A — explicit URL

```bash
export EPIDEMIC_TEST_PG_URL='postgresql://USER:PASSWORD@HOST:5432/epidemic'
uv run pytest tests/db -m db -v
```

### Option B — repo `.env` (port-forward)

For OpenShift Timescale reached via **localhost** after port-forward, copy `[.env.example](../.env.example)` to `**.env`** (gitignored), set `DB_HOST=localhost`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT`. Tests load `.env` automatically from the repo root via `python-dotenv`.

Password source (same as Spark/DBeaver):

```bash
oc get secret epidemic-db-spark -o jsonpath='{.data.password}' | base64 --decode; echo
```

Port-forward (keep open in another terminal):

```bash
oc port-forward svc/timescaledb-service 5432:5432
```

More detail: `[docs/local-db-connection.md](local-db-connection.md)`.

---

## Tier 4 — Offline Phase 1 “smoke” contracts (`tests/cluster/`)

`[tests/cluster/test_phase1_contract.py](../tests/cluster/test_phase1_contract.py)` checks that:

- Tekton defines Pipeline `**phase1-pipeline**`.
- Spark Job `**infra/spark-job.yaml**` wires `**FEATURE_SOURCE_MODE**` to `**raw_db**`, JDBC env, and `**spark-etl-script**` mounted at `/opt/epidemic`.
- `**[spark/etl.py](../spark/etl.py)**` exists.

This is **not** a substitute for running Spark on the cluster; it prevents accidental drift in committed manifests.

---

## End-to-end cluster operations (not pytest)

For live PipelineRuns, Kafka, and Spark Jobs on OpenShift, use the bash workflows documented in `[runbook.md](runbook.md)`. The **canonical** long-running smoke for Phase 1 → raw ingest → raw_db Spark refresh is `[scripts/run-phase1-rawdb-sequenced.sh](../scripts/run-phase1-rawdb-sequenced.sh)`.

Optional helpers (narrower scope): `[scripts/smoke-phase1-topics.sh](../scripts/smoke-phase1-topics.sh)`, `[scripts/smoke-phase2-db.sh](../scripts/smoke-phase2-db.sh)`.

---

## Legacy unittest

Detector tests under `tests/unit/` can still be run with:

```bash
uv run python -m unittest discover -s tests/unit
```

---

## Pre-submission checklist

1. `uv sync --extra dev`
2. `./scripts/run-test-suite.sh` (or `uv run pytest tests -v --tb=short -ra`)
3. Optional: port-forward + `.env` or `EPIDEMIC_TEST_PG_URL` so `**tests/db/` passes** (not skipped)
4. Optional: confirm Java on `PATH` so `**tests/spark/test_etl_pipeline_local.py`** runs (not skipped)
5. Before cluster demos: run `[scripts/run-phase1-rawdb-sequenced.sh](../scripts/run-phase1-rawdb-sequenced.sh)` with a short `DURATION_SEC` if you need live Tekton + ingest proof

