# Phase 2 Spark batch ETL

`etl.py` reads the three typed Kafka topics, builds hourly fact DataFrames, and appends to TimescaleDB/PostgreSQL via JDBC.

## Computed analytics

The job implements **five distinct hourly aggregations** in `spark/etl.py` (functions `build_feature_region_hourly` through `build_feature_symptom_hourly`). Each result set is written with **`mode("append")`** to the matching Timescale table (primary keys include `batch_id`).

1. **`feature_region_hourly`** — Per hour and region, unions symptom, clinic, and hospital streams and aggregates event-type counts, severity and duration sums/counts, temperature sum/count, and `available_beds` gauge rollups (min, avg, last-by-event-time, sample count).

2. **`feature_clinic_hourly`** — Per hour and clinic, counts visits and follow-ups, sums/counts temperatures, and rolls up `available_beds` gauge stats.

3. **`feature_hospital_hourly`** — Per hour and hospital, counts admissions; sums/counts severity, temperature, oxygen, and expected length-of-stay; plus `available_beds` gauge rollups.

4. **`feature_diagnosis_hourly`** — Per hour, clinic, and diagnosis code (clinic visits with non-null `diagnosis_code`), counts visits and aggregates temperature sum/count.

5. **`feature_symptom_hourly`** — Per hour, region, and exploded symptom token, counts how often each token appears in that bucket.

**Layer B (SQL only):** `infra/database-deployment.yaml` defines views such as `analytic_regional_disease_severity_trend` and `analytic_hospital_capacity_pressure` on top of these facts; they are evaluated in PostgreSQL/Timescale, not inside Spark.

**Idempotency:** Re-running the Spark job re-reads Kafka (`earliest` → `latest`) and **appends** new rows with a new `batch_id`; it does not upsert in place.

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

Phase 1 fills the **typed topics**; Phase 2 reads those same topics. To run both with cleanup and waits in one shot, use **`scripts/run-phase1-and-phase2.sh`** (see **Run Phase 1 and Phase 2 (automation)** below). Manual steps:

Run in this order:

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
6. **Database** — `infra/spark-job.yaml` wires JDBC using Secret **`epidemic-db-spark`** (username/password). For a dry run without writes, set env **`SKIP_JDBC_WRITE=true`** on the Job and omit JDBC usage in `etl.py`.
7. **Run the Spark Job** — `oc apply -f infra/spark-job.yaml -n "$NAMESPACE"` then `**oc logs -f job/epidemic-spark-etl -n "$NAMESPACE"`**. You should see `**[ANALYTICS]`** lines (`kafka_read`, `parse`, `db_write`, `summary`). Topic env vars default to `**ds551-s26.team02.***` and should match Phase 1 output topic params.
8. **Re-run** — Delete the Job before re-applying: `**oc delete job epidemic-spark-etl -n "$NAMESPACE"`**. Run another `**oc create -f tekton/pipelinerun.yaml`** to push more batches through Phase 1, then run the Spark Job again.

**Note:** Spark batch reads `**earliest` → `latest`**, so each ETL run scans the full current topic; new Phase 1 runs append messages for the next ETL pass.

## Run Phase 1 and Phase 2 (automation)

From the **repository root**, with `oc login` done and **`oc project <namespace>`** set (same namespace as Kafka/Tekton/DB):

```bash
chmod +x scripts/run-phase1-and-phase2.sh   # once
./scripts/run-phase1-and-phase2.sh
```

The script **`scripts/run-phase1-and-phase2.sh`**:

- **Cleans up** the previous `epidemic-spark-etl` Job and (by default) **Succeeded** Phase 1 Task pods named like `phase1-run-*-validate-pod` / `phase1-run-*-route-and-enrich-pod` (helps quota and clutter).
- **Phase 1:** `oc apply` for `tekton/tasks.yaml` and `tekton/pipeline.yaml`, **`oc create`** `tekton/pipelinerun.yaml`, then **`oc wait`** for the newest `PipelineRun` to succeed.
- **Phase 2:** applies `infra/spark-rbac.yaml`, refreshes the `spark-etl-script` ConfigMap from `spark/etl.py`, applies `infra/spark-job.yaml`, waits for the Job to complete, and prints the last **`[ANALYTICS]`** lines from the job log.

**Useful options:**

| Flag | Effect |
|------|--------|
| `--truncate-db` | `TRUNCATE` the five `feature_*_hourly` tables before Spark (destructive). |
| `--skip-phase1` | Only run Spark (typed topics must already have data). |
| `--skip-phase2` | Only run Tekton. |
| `--no-cleanup` | Skip Job delete and Succeeded-pod delete (still creates a new `PipelineRun` when Phase 1 runs). |
| `--no-delete-phase1-pods` | Keep Succeeded Phase 1 Task pods. |

**Environment overrides:** `SPARK_JOB_NAME`, `PIPELINERUN_MANIFEST`, `DB_POD`, `DB_NAME`, `DB_USER`, `PHASE1_WAIT_TIMEOUT`, `SPARK_WAIT_TIMEOUT`.

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

The Job installs **kafka-python-ng** and **psycopg2-binary** with `pip` (not PySpark — that comes from the image). It runs `**spark-submit`** so the driver sees `**pyspark`**, and passes `**--packages`** for the Kafka SQL and PostgreSQL JDBC jars (downloaded once into `**/tmp/spark-ivy2**`; needs egress to Maven Central unless your cluster mirrors it).

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

## Redeploy after code or manifest changes

Kubernetes **Jobs are immutable**: you cannot change a running Job’s pod template in place. After you edit `**spark/etl.py`**, refresh the ConfigMap and create a new Job. After you edit `**infra/spark-job.yaml`**, re-apply the manifest and again replace the Job.

From the **repository root**, with `NAMESPACE` set to your OpenShift project (same value everywhere):

```bash
export NAMESPACE=<your-openshift-project>

# 1) Push the latest ETL script into the cluster (skip if you only changed infra/spark-job.yaml)
oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -

# 2) Remove the old Job (required: Jobs are immutable; pods keep the old ConfigMap mount until recreated)
oc delete job epidemic-spark-etl -n "$NAMESPACE" --ignore-not-found

# 3) Create a new Job from the manifest on disk (apply whenever spark-job.yaml changed)
oc apply -f infra/spark-job.yaml -n "$NAMESPACE"

# 4) Stream driver logs until completion
oc logs -f job/epidemic-spark-etl -n "$NAMESPACE"
```

**Notes**

- Step 1 alone updates the ConfigMap; you **must** still run steps 2–3 so a new pod mounts the new `etl.py` revision.
- If you only changed `**infra/spark-job.yaml`**, run steps 2–4 (skip step 1).
- First run on a **new** worker may re-download `**--packages`** JARs (Ivy cache under `/tmp` in the pod).

## Local run (developer laptop)

Requires Spark, Java 17+, and the PostgreSQL JDBC JAR on the driver class path, or set `SPARK_JDBC_JAR` to that JAR path.

```bash
export KAFKA_BOOTSTRAP=...
export JDBC_URL=jdbc:postgresql://localhost:5432/epidemic
export JDBC_USER=...
export JDBC_PASSWORD=...
python spark/etl.py
```

