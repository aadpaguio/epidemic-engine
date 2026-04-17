# Phase 2 stress-test report (R5b)

This document describes how we stress-test the full path **Kafka (raw) → Phase 1 Tekton → typed topics → Spark batch ETL → TimescaleDB**, what we measure, and how to reproduce runs. Fill in **Results** (below) after each formal test run.

**Related:** raw loadgen details in [`load-test-raw-kafka.md`](load-test-raw-kafka.md), Spark job ops in [`../spark/README.md`](../spark/README.md).

---

## 1. Test setup

### 1.1 Kafka bootstrap configuration

| Item | Value (team: adjust if different) |
|------|-----------------------------------|
| Bootstrap (in-cluster DNS) | `kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092` |
| Raw topic (example) | `ds551-s26.team02.raw` |
| Typed topics (example) | `ds551-s26.team02.symptom_reports`, `…clinic_visits`, `…hospital_admissions` |

**Stress isolation:** each automated run uses a unique suffix (`run_id`) so topics and consumer groups do not reuse committed offsets from prior tests. The Tekton template is [`tekton/pipelinerun-stress-template.yaml`](../tekton/pipelinerun-stress-template.yaml).

### 1.2 Database specs

From [`infra/database-deployment.yaml`](../infra/database-deployment.yaml) (TimescaleDB / PostgreSQL):

| Item | Configuration |
|------|----------------|
| JDBC URL (cluster) | `jdbc:postgresql://timescaledb-service:5432/epidemic` |
| Data volume | PVC `timescaledb-pvc`, **1 Gi** requested (class: cluster default) |
| Pod resources (StatefulSet) | requests: **100m CPU, 512Mi** memory; limits: **500m CPU, 1Gi** memory |

Record any cluster-specific overrides (actual PVC size, node pool, storage class) in **Results**.

### 1.3 Spark job resource limits (CPU, memory)

From [`infra/spark-job.yaml`](../infra/spark-job.yaml), container `spark-etl`:

| | Request | Limit |
|--|---------|--------|
| CPU | 500m | 2 |
| Memory | 1Gi | 2Gi |

Execution mode: `spark-submit --master 'local[*]'` inside the Job pod (driver executes locally; Kafka read uses batch `earliest` → `latest` for subscribed typed topics).

### 1.4 Phase 1 (Tekton) consumer tasks

Tasks [`validate-events` and `route-and-enrich`](../tekton/tasks.yaml) use **1 CPU / 1Gi** limits per step (see manifests). Large backlogs require a high **`batch-size`** PipelineRun parameter so each run can drain enough messages; the stress suite sets this via `STRESS_BATCH_SIZE` (see below).

---

## 2. Load scenarios (rubric)

Per **PHASE2-INSTRUCTIONS.md R5b**, we report two primary scenarios:

| Scenario | Target rate | Duration | Approx. raw events (order of magnitude) |
|----------|-------------|----------|------------------------------------------|
| **A — sustained** | **100 events/sec** | **1 hour** | ~3.6×10⁵ |
| **B — burst** | **1000 events/sec** | **10 minutes** | ~6×10⁵ |

**What we measure**

| Layer | Metric | Where to read it |
|-------|--------|------------------|
| Ingest | Throughput, broker ack latency | Loadgen summary JSON (`tag: LOADGEN`) in `logs/stress/<run_id>/loadgen.log` |
| Phase 1 | Drain batch size, TaskRun duration | `oc get taskrun`, saved `taskruns.yaml` under `logs/stress/<run_id>/` |
| Spark | End-to-end job time, Kafka rows read, parse skips | `[ANALYTICS]` lines in `phase2-spark.log` (`phase: summary`, `kafka_read`, `db_write`) |
| Database | Per-table write duration | `[ANALYTICS]` `phase: db_write` → `duration_ms`, `rows_written` |
| Cluster | CPU/memory | `oc adm top pod` samples during the run (paste snapshots into **Results** if useful) |

### 2.1 How to run (automated suite)

From repo root, with `oc project` set to your namespace:

```bash
# ConfigMap for producer script (idempotent)
oc create configmap loadgen-produce-script \
  --from-file=produce.py=loadgen/produce.py \
  --dry-run=client -o yaml | oc apply -f -

# Ensure Phase 1 batch can drain the backlog (≥ expected raw event count for that run)
export STRESS_BATCH_SIZE=400000          # example: 100/s × 1 h ≈ 3.6e5 raw; add headroom
export STRESS_CONSUMER_TIMEOUT_MS=300000
export PHASE1_WAIT_TIMEOUT=240m
export SPARK_WAIT_TIMEOUT=120m

# Scenario A: extend duration — the default script "moderate" is 100/s × 600s only.
# Option: run the script with a longer loadgen by temporarily editing scripts/run-phase1-stress-suite.sh
# moderate duration to 3600, or run loadgen Job + PipelineRun manually (see load-test-raw-kafka.md).
bash scripts/run-phase1-stress-suite.sh --only moderate --with-phase2
```

```bash
# Scenario B: default "burst" in script is 1000/s × 60s — not full 10 minutes.
export STRESS_BATCH_SIZE=650000
export PHASE1_WAIT_TIMEOUT=240m
export SPARK_WAIT_TIMEOUT=120m
bash scripts/run-phase1-stress-suite.sh --only burst --with-phase2
```

For a **full 10-minute burst at 1000/s**, increase loadgen duration to **600** seconds (custom Job env or a one-line edit to `run_scenario burst` third argument in `scripts/run-phase1-stress-suite.sh`) and keep `STRESS_BATCH_SIZE` above **600000**.

Artifacts land under **`logs/stress/<run_id>/`** (`loadgen.log`, `pipelinerun.yaml`, `taskruns.yaml`, `phase2-spark.log` when `--with-phase2` is used).

---

## 3. Results

This section uses two saved runs under `logs/stress/`. **Important:** the assignment asks for **100/s for 1 hour** and **1000/s for 10 minutes**. What we actually ran here is shorter: **100/s for 10 minutes** (`moderate-20260417-001437`) and **1000/s for 1 minute** (`burst-20260417-004204`). The numbers below are still useful for throughput, Kafka ack behavior, Phase 1 drain time, Spark, and DB writes; repeat with the full durations when you submit the formal rubric runs.

**Log paths**

- Sustained: `logs/stress/moderate-20260417-001437/loadgen.log`, `phase2-spark.log`, `pipelinerun.yaml`
- Burst: `logs/stress/burst-20260417-004204/loadgen.log`, `phase2-spark.log`, `pipelinerun.yaml`

### 3.1 Sustained load — 100 events/sec × 10 minutes (`moderate-20260417-001437`)

| Field | Value |
|-------|--------|
| Run ID / date | `moderate-20260417-001437` (2026-04-17) |
| `events_sent` (loadgen) | 59,821 |
| `actual_rate_per_s` | 99.671 |
| Kafka ack latency p50 / p95 / p99 (ms) | 300,292 / 569,813 / 593,767 |
| Send / ack errors (loadgen) | 0 / 0 |
| Phase 1 PipelineRun duration | ~12 min 38 s (start `04:24:50Z` → completion `04:37:28Z`, `phase1-stress-moderate-20260417-001437-lk98q`) |
| Spark `summary.duration_ms` | 70,016 (~70 s) |
| Spark `total_consumed` (typed Kafka rows) | 56,835 (symptom 25,563 + clinic 16,975 + hospital 14,297) |
| Spark parse | `parse_ok_count` 56,835, `skipped_count` 0, `status` healthy |
| DB write times (ms) | region 5,494; clinic 2,393; hospital 2,094; **diagnosis 13,140**; symptom 2,115 |
| Rows written (feature tables, total) | 16,506 |
| Peak pod memory/CPU | Not saved (use `oc adm top pod` on future runs if you want this in the report) |

**Plain-language readout:** The producer held **~100 events/sec** for **600 s** with **no failed sends or acks**. Typed-topic row counts (~56.8k routable) line up with ~59.8k raw messages minus ~5% unroutable traffic. **Kafka acknowledgements were very slow** (median ~5 minutes, tail ~10 minutes) even though the rate target was met, which usually means messages are piling up inside the broker or the cluster is under heavy queueing. Phase 1 took **~13 minutes** to drain this batch. Spark finished in **~70 seconds**, parsed everything, and wrote to the database; the **diagnosis** hourly table took the longest single write because it has the most distinct groups (hour × clinic × diagnosis code).

### 3.2 Burst load — 1000 events/sec × 1 minute (`burst-20260417-004204`)

| Field | Value |
|-------|--------|
| Run ID / date | `burst-20260417-004204` (2026-04-17) |
| `events_sent` (loadgen) | 46,348 |
| `actual_rate_per_s` | **771.7** (target was 1000/s) |
| Kafka ack latency p50 / p95 / p99 (ms) | 29,775 / 56,693 / 59,136 |
| Send / ack errors (loadgen) | 0 / 0 |
| Phase 1 PipelineRun duration | ~12 min 12 s (start `04:43:19Z` → completion `04:55:31Z`, `phase1-stress-burst-20260417-004204-bmzkp`) |
| Spark `summary.duration_ms` | 60,231 (~60 s) |
| Spark `total_consumed` | 44,083 (symptom 20,005 + clinic 13,133 + hospital 10,945) |
| Spark parse | `parse_ok_count` 44,083, `skipped_count` 0, `status` healthy |
| DB write times (ms) | region 4,963; clinic 3,160; hospital 2,034; **diagnosis 11,216**; symptom 1,757 |
| Rows written (feature tables, total) | 12,993 |
| Peak pod memory/CPU | Not saved |

**Plain-language readout:** We asked for **1000 events/sec** but only achieved about **772/sec** on average, so the **ingest path could not keep up** with the requested burst (still with zero errors). Ack latencies were **tens of seconds to about a minute**, better than the sustained test in absolute milliseconds, but the **shortfall in rate** is the main signal of pressure. Phase 1 again took on the order of **12 minutes** for this batch. Spark ran in **~60 s** and stayed healthy; the largest DB cost was again **diagnosis** writes.

### 3.3 Summary table — target rate vs. achieved rate vs. Kafka ack latency (ms)

Latency here is **producer ack latency** from the loadgen `LOADGEN` summary (time until Kafka confirms each batch of sends), not end-to-end event latency through Spark.

| Run | Target events/sec | Achieved events/sec (avg) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) |
|-----|-------------------|---------------------------|------------------|------------------|------------------|
| Moderate (100/s × 10 min) | 100 | 99.7 | 300,292 | 569,813 | 593,767 |
| Burst (1000/s × 1 min) | 1000 | 771.7 | 29,775 | 56,693 | 59,136 |

### 3.4 Bottlenecks identified (from these two runs)

- **Kafka:** Under **steady 100/s**, ack times were **extremely high** (many minutes), which points to **broker-side queueing or capacity limits** even though the client kept publishing at the target rate. Under **1000/s burst**, the system **did not reach the target rate**, so **Kafka (or the shared cluster)** is likely the first limit we hit for ingest.
- **Phase 1:** Each run needed **~12+ minutes** of Tekton time to move tens of thousands of messages. The tasks are **single-threaded Python consumers** with a cap on messages per run (`batch-size`), so Phase 1 is a **throughput and wall-clock** bottleneck relative to raw loadgen speed.
- **Spark:** For these batch sizes, Spark stayed **healthy** and finished in **about one minute**. It does a **full earliest→latest read** of the three typed topics each job, so **very large topics** would show up as longer runs and more driver memory use (not stress-tested to failure here).
- **Database:** **`feature_diagnosis_hourly`** dominated JDBC time because it writes **one row per (hour, clinic, diagnosis)** combination; with random diagnosis codes there are many groups. Other feature tables are smaller keyed aggregates.

### 3.5 Back-pressure handling

If **loadgen or Phase 1** writes faster than **Spark** runs, messages **stay on the typed Kafka topics** until the next Spark job. Spark reads from **earliest to latest** for that job, so lag shows up as **more bytes/offsets on topic**, a **longer Spark batch read**, and **larger DB appends**. Operationally you can **run Spark more often**, **reduce load**, **raise Spark resources**, or **process a smaller offset window** (would require a job change). Phase 1 already limits how much moves per `PipelineRun` via `batch-size`; if raw outruns Phase 1, **raw topic lag** grows until you add **more or larger PipelineRuns**.

---

## 4. Failure modes

We did **not** run chaos or fault-injection tests for this write-up, so the bullets below are **risks to watch**, not observed outcomes.

- **Database down during Spark:** JDBC writes would likely fail; the job may **retry** a few times (`backoffLimit` on the Spark Job) then exit non-zero. Logs would show `[ANALYTICS]` errors with database-related `reason_code` values.
- **Kafka broker overload or huge topics:** Producer ack latency can grow (as in the sustained test) or throughput can fall short of target (as in the burst test). Spark may run longer or hit **memory pressure** if a single batch read spans a very large offset range on `local[*]`.
- **Phase 1 slower than loadgen:** **Raw topic lag** grows until you run additional PipelineRuns or increase `batch-size`; unroutable and invalid traffic still follow DLQ / silent-drop rules in the tasks.
- **Recovery (typical):** restore the failing dependency, **delete** the failed Spark Job if immutable, re-apply the Job manifest, re-run Phase 1 drain if topics are ahead of processing, then confirm `[ANALYTICS] summary` and spot-check feature tables in Postgres.

---

## Revision history

| Date | Change |
|------|--------|
| 2026-04-17 | Filled Results from `moderate-20260417-001437` and `burst-20260417-004204` logs |
| 2026-04-17 | Trimmed burst table; failure modes reframed as untested risks |
| (earlier) | Initial template aligned with R5b |
