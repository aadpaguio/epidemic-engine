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

## Docker image (OpenShift registry)

The **Dockerfile** uses **UBI 9** (`registry.access.redhat.com/ubi9/python-311`) and **`dnf`** for Java so cluster builds do not need **Debian mirrors** (`deb.debian.org`, often blocked from build pods).

### On-cluster build from Git (usually best)

The cluster **clones** the repo—**no laptop tarball upload**. Requires the build pod to reach **GitHub** (or your Git host). The `Dockerfile` is at the repo root; `.dockerignore` keeps the clone/build context small.

If you **already** have a **binary** `BuildConfig` named `epidemic-spark-etl`, delete it (and optionally the old `ImageStream`) before creating a Git-sourced one:

```bash
oc delete buildconfig epidemic-spark-etl --ignore-not-found
# Optional: only if you want a clean ImageStream — Job will break until a new build completes.
# oc delete imagestream epidemic-spark-etl --ignore-not-found
```

**Branch:** always build from **`main`** — merge feature branches there before you rely on cluster builds.

```bash
REPO=https://github.com/bu-cds-ds551-s26/epidemic-engine-2-smith-fergusonhull-nguyen-paguio.git
```

**Private GitHub repo** (typical for course teams) — **HTTPS + PAT:**

1. Create a GitHub **personal access token** with **read** access to this repo (never commit the token; store only in OpenShift).

2. Create a **basic-auth** secret and allow the **`builder`** account to use it for clones:

```bash
oc create secret generic git-github-epidemic \
  --from-literal=username=git \
  --from-literal=password=<GITHUB_PAT> \
  --type=kubernetes.io/basic-auth

oc secrets link builder git-github-epidemic
```

3. Create the `BuildConfig`:

```bash
oc new-build "${REPO}#main" --name epidemic-spark-etl --strategy docker --source-secret=git-github-epidemic
```

**Public repo:** same `oc new-build` line **without** `--source-secret=...`.

Rebuild after you **push** to **`main`**:

```bash
oc start-build epidemic-spark-etl --follow
```

SSH deploy keys are also supported (`kubernetes.io/ssh-auth` secret + `--source-secret`); see OpenShift **“Creating a Secret for Git Authentication”**.

**If the cluster cannot reach GitHub**, Git-sourced builds will not work—use binary `--from-dir` or build locally.

### Laptop: docker build + push

From the **repository root** (only works if you have a **reachable** registry URL from your machine; many class clusters have **no public registry route**):

```bash
export REGISTRY=<external-registry-host-from-oc-or-course-docs>
docker build -t "${REGISTRY}/$(oc project -q)/epidemic-spark-etl:latest" .
oc registry login   # if supported
docker push "${REGISTRY}/$(oc project -q)/epidemic-spark-etl:latest"
```

If **`ADD`** of the PostgreSQL JDBC JAR fails in-cluster (egress), building **on your laptop** avoids that.

### On-cluster binary build (fallback)

```bash
oc new-build --name=epidemic-spark-etl --binary --strategy=docker
oc start-build epidemic-spark-etl --from-dir=. --follow
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

