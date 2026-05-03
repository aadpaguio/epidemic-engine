# Local DB Connection (DBeaver + TimescaleDB)

This guide explains how to connect to the in-cluster TimescaleDB instance from your local machine.

## Why direct host access fails

`timescaledb-service` is a Kubernetes/OpenShift internal DNS name (`ClusterIP` service).  
Desktop tools like DBeaver cannot resolve that host directly from your laptop.

Use a local port-forward, then connect through `localhost`.

## Prerequisites

- Cluster access via `oc` or `kubectl`
- Target namespace where `timescaledb-service` is deployed
- DBeaver installed locally

## 1) Start port-forwarding

Open a terminal and run one of the following:

```bash
# OpenShift
oc port-forward svc/timescaledb-service 5432:5432

# Kubernetes
kubectl port-forward svc/timescaledb-service 5432:5432
```

Keep this terminal session running while connected from DBeaver.

If you are not already in the correct project/namespace context, add `-n <namespace>`.

## 2) Configure DBeaver (PostgreSQL driver)

Create a PostgreSQL connection with:

- Host: `localhost`
- Port: `5432`
- Database: `epidemic`
- Username: `spark_writer`
- Password: `CHANGE_ME_BEFORE_DEPLOY` (or the real current secret value)

Then click **Test Connection**.

## 3) Verify DB state

After connecting, run:

```sql
SELECT version();
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';
```

If `timescaledb` is returned, the extension is enabled and connection is correct.

## pytest integration tests (`tests/db/`)

The same **localhost** connection settings work for automated DB checks.

1. Start port-forward (as above), keep the terminal open.
2. Either export a full URL:

   ```bash
   export EPIDEMIC_TEST_PG_URL='postgresql://spark_writer:YOUR_PASSWORD@127.0.0.1:5432/epidemic'
   ```

   …or create a repo-root **`.env`** from [`.env.example`](../.env.example) with `DB_HOST=localhost`, `DB_USER=spark_writer`, `DB_PASSWORD`, `DB_NAME=epidemic`, `DB_PORT=5432`. The test harness loads `.env` automatically.

3. Run:

   ```bash
   uv run pytest tests/db -v
   ```

   Or the full suite:

   ```bash
   ./scripts/run-test-suite.sh
   ```

Full testing overview: [`docs/testing-plan.md`](testing-plan.md).

## Troubleshooting

- If DBeaver cannot connect, verify DB pod/service:
  - `oc get pods -l app=timescaledb`
  - `oc get svc timescaledb-service`
- If credentials fail, check the secret values currently deployed:
  - `oc get secret epidemic-db-spark -o jsonpath='{.data.username}' | base64 --decode; echo`
  - `oc get secret epidemic-db-spark -o jsonpath='{.data.password}' | base64 --decode; echo`
