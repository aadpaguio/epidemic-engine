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
| `SPARK_EXTRA_JARS`                                  | no       | Optional comma-separated Spark datasource jars; the image now sets this to the Spark Kafka connector jars                                                 |
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
5. **Apply RBAC + create the ETL script ConfigMap** (no custom image build needed):
  ```bash
   oc apply -f infra/spark-rbac.yaml -n "$NAMESPACE"
   oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
     --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -
  ```
6. **Database optional for first test** — `infra/spark-job.yaml` defaults to `**SKIP_JDBC_WRITE=true`**, so no DB secret is required. When the DB exists, remove/flip `SKIP_JDBC_WRITE` and create the `**epidemic-db-spark`** secret expected by the manifest.
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

## Container image strategy (TA guidance)

For Phase 2, use the pre-built image in the Job spec instead of OpenShift image builds:

```yaml
image: apache/spark:3.5.0
```

This avoids current build-path issues (`oc start-build` / binary upload) and still
satisfies the containerization requirement for this phase.

If additional Python packages are needed, install them inline at job runtime:

```bash
pip install kafka-python-ng psycopg2-binary --target=/tmp/pkgs --quiet
export PYTHONPATH="/tmp/pkgs:${PYTHONPATH:-}"
```

## Kubernetes: one-shot Job (testing)

`infra/spark-job.yaml` defines a **Job** (runs once to completion). It uses `apache/spark:3.5.0` and mounts `spark/etl.py` from a ConfigMap, so apply RBAC and create/update the ConfigMap before running:

```bash
oc apply -n "${NS}" -f infra/spark-rbac.yaml
oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -n "${NS}" -f -
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

