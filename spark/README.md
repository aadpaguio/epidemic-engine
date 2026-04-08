# Phase 2 Spark batch ETL

`etl.py` reads the three typed Kafka topics, builds hourly fact DataFrames, and appends to TimescaleDB/PostgreSQL via JDBC.

## Configuration (environment)


| Variable                                            | Required | Description                                                                                                                                               |
| --------------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP`                                   | yes      | Kafka bootstrap servers                                                                                                                                   |
| `JDBC_URL`                                          | yes      | e.g. `jdbc:postgresql://timescaledb-service:5432/epidemic`                                                                                                |
| `JDBC_USER`                                         | yes      | DB user                                                                                                                                                   |
| `JDBC_PASSWORD`                                     | yes      | DB password                                                                                                                                               |
| `TOPIC_SYMPTOM` / `TOPIC_CLINIC` / `TOPIC_HOSPITAL` | no       | Defaults to team02 topic names in code                                                                                                                    |
| `BATCH_ID` / `RUN_ID`                               | no       | Run metadata stored on fact rows                                                                                                                          |
| `SPARK_JDBC_JAR`                                    | no       | Set in Docker image to PostgreSQL JDBC path                                                                                                               |
| `ANALYTICS_SKIP_DEGRADE_PCT`                        | no       | Skip-rate threshold for `degraded` summary (default `25`)                                                                                                 |
| `SKIP_JDBC_WRITE`                                   | no       | If `1` / `true` / `yes` / `on`, run Kafka → parse → aggregates and logs **without** `JDBC_`* or any database write (`write_mode: dry_run` on fact tables) |


Structured logs use prefix `[ANALYTICS]` (see Phase 2 plan).

## End-to-end on OpenShift: Phase 1 → Spark ETL

Phase 1 fills the **typed topics**; Phase 2 reads those same topics. Run in this order:

1. `**oc login`** and `**oc project <your-namespace>`** (same project where Kafka and Tekton run).
2. **Raw input** — The generator (or replay) must be writing to `**ds551-s26.team02.raw`** so Phase 1 has something to consume.
3. **Deploy and run Phase 1** (from repo root; see `**tekton/README.md`**):
  ```bash
   export NAMESPACE=<your-namespace>
   oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
   oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
   oc create -f tekton/pipelinerun.yaml -n "$NAMESPACE"
  ```
   Wait until the `PipelineRun` and both `TaskRun`s succeed (`oc get pipelinerun,taskrun -n "$NAMESPACE"`). Each run processes up to `**batch-size**` messages (default **100**) from validated → typed topics.
4. **Kafka bootstrap** — `infra/spark-job.yaml` uses the **same `KAFKA_BOOTSTRAP` as `tekton/pipelinerun.yaml`** (`kafka-bootstrap`). If your team changes the broker in the `PipelineRun`, update the Job env to match.
5. **Build/push the Spark image** (see **Docker image** below), then:
  ```bash
   oc apply -f infra/spark-rbac.yaml -n "$NAMESPACE"
  ```
6. **Database optional for first test** — Uncomment `**SKIP_JDBC_WRITE`** in `infra/spark-job.yaml` (or patch the Job) so you do not need the DB secret yet. When the DB exists, remove `SKIP_JDBC_WRITE` and create the `**epidemic-db-spark`** secret expected by the manifest.
7. **Run the Spark Job** — `oc apply -f infra/spark-job.yaml -n "$NAMESPACE"` then `**oc logs -f job/epidemic-spark-etl -n "$NAMESPACE"`**. You should see `**[ANALYTICS]`** lines (`kafka_read`, `parse`, `db_write`, `summary`). Topic env vars default to `**ds551-s26.team02.***` and should match Phase 1 output topic params.
8. **Re-run** — Delete the Job before re-applying: `**oc delete job epidemic-spark-etl -n "$NAMESPACE"`**. Run another `**oc create -f tekton/pipelinerun.yaml`** to push more batches through Phase 1, then run the Spark Job again.

**Note:** Spark batch reads `**earliest` → `latest`**, so each ETL run scans the full current topic; new Phase 1 runs append messages for the next ETL pass.

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

- **In-cluster DNS** (e.g. `kafka.*.svc.cluster.local:9092`) usually **does not resolve** from your Mac unless you use something that joins that network (VPN, `oc port-forward` to a Kafka **Route**/NodePort if exposed, or run the job **inside** OpenShift).
- **From inside the cluster** (Job/CronJob pod), use the bootstrap host your course/team docs give for **in-namespace** access.

So: same *logical* broker, but pick the **hostname that works from where the process runs** (laptop vs pod).

## Docker image (team02 placeholder registry)

The **`Dockerfile` uses UBI 9** (`registry.access.redhat.com/ubi9/python-311`) and **`microdnf`** for Java so OpenShift builds do not need **`deb.debian.org`** (often blocked from build pods).

From the **repository root** (parent of `spark/`):

```bash
# Log in to OpenShift internal registry first (see course credentials).
export REGISTRY=<your-openshift-internal-registry-host>
export NS=team02
docker build -t "${REGISTRY}/${NS}/epidemic-spark-etl:latest" .

docker push "${REGISTRY}/${NS}/epidemic-spark-etl:latest"
```

If **`ADD` of the PostgreSQL JDBC JAR** fails in-cluster (egress), build the image **on your laptop** (where HTTPS to `jdbc.postgresql.org` works) and **`docker push`** into the registry.

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

