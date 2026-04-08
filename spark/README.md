# Phase 2 Spark batch ETL

`etl.py` reads the three typed Kafka topics, builds hourly fact DataFrames, and appends to TimescaleDB/PostgreSQL via JDBC.

## Configuration (environment)

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP` | yes | Kafka bootstrap servers |
| `JDBC_URL` | yes | e.g. `jdbc:postgresql://timescaledb-service:5432/epidemic` |
| `JDBC_USER` | yes | DB user |
| `JDBC_PASSWORD` | yes | DB password |
| `TOPIC_SYMPTOM` / `TOPIC_CLINIC` / `TOPIC_HOSPITAL` | no | Defaults to team02 topic names in code |
| `BATCH_ID` / `RUN_ID` | no | Run metadata stored on fact rows |
| `SPARK_JDBC_JAR` | no | Set in Docker image to PostgreSQL JDBC path |
| `ANALYTICS_SKIP_DEGRADE_PCT` | no | Skip-rate threshold for `degraded` summary (default `25`) |
| `SKIP_JDBC_WRITE` | no | If `1` / `true` / `yes` / `on`, run Kafka → parse → aggregates and logs **without** `JDBC_*` or any database write (`write_mode: dry_run` on fact tables) |

Structured logs use prefix `[ANALYTICS]` (see Phase 2 plan).

## Test without a database

If TimescaleDB is not ready yet, you can still validate reads, parsing, hourly facts, and logging:

```bash
export KAFKA_BOOTSTRAP=<reachable-broker-host:9092>
export SKIP_JDBC_WRITE=true
# Do not set JDBC_* when skipping — they are not required.
python spark/etl.py
```

`rows_written_total` in the final summary will be `0`; `rows_computed_total` shows how many fact rows would have been appended.

## Local machine vs cluster Kafka

**Yes, a local run can talk to the same bootstrap server**—as long as **your laptop can open a TCP route** to `KAFKA_BOOTSTRAP` (DNS resolves and port 9092, or TLS port if used, is reachable).

- **In-cluster DNS** (e.g. `kafka-team02:9092`, `*.svc.cluster.local`) usually **does not resolve** from your Mac unless you use something that joins that network (VPN, `oc port-forward` to a Kafka **Route**/NodePort if exposed, or run the job **inside** OpenShift).
- **From inside the cluster** (Job/CronJob pod), use the bootstrap host your course/team docs give for **in-namespace** access.

So: same *logical* broker, but pick the **hostname that works from where the process runs** (laptop vs pod).

## Docker image (team02 placeholder registry)

From the **repository root** (parent of `spark/`):

```bash
# Log in to OpenShift internal registry first (see course credentials).
export REGISTRY=<your-openshift-internal-registry-host>
export NS=team02
docker build -t "${REGISTRY}/${NS}/epidemic-spark-etl:latest" .

docker push "${REGISTRY}/${NS}/epidemic-spark-etl:latest"
```

On-cluster builds (example):

```bash
oc new-build --name=epidemic-spark-etl --binary --strategy=docker -n "${NS}"
oc start-build epidemic-spark-etl --from-dir=. -n "${NS}" --follow
```

## Kubernetes: one-shot Job (testing)

`infra/spark-job.yaml` defines a **Job** (runs once to completion). Update the image reference, `KAFKA_BOOTSTRAP`, `JDBC_URL`, topic env vars, and Secret name to match your namespace.

```bash
oc apply -n "${NS}" -f infra/spark-rbac.yaml
oc apply -n "${NS}" -f infra/spark-job.yaml
oc logs -f job/epidemic-spark-etl -n "${NS}"
```

To run again, delete the Job (Jobs are immutable):

```bash
oc delete job epidemic-spark-etl -n "${NS}"
```

## Local run (developer laptop)

Requires Spark, Java 17+, and the PostgreSQL JDBC JAR on the driver class path, or set `SPARK_JDBC_JAR` to that JAR path.

```bash
export KAFKA_BOOTSTRAP=...
export JDBC_URL=jdbc:postgresql://localhost:5432/epidemic
export JDBC_USER=...
export JDBC_PASSWORD=...
python spark/etl.py
```
