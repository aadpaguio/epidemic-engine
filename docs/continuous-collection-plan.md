# Timed Continuous Collection Plan

This plan avoids Kubernetes `CronJob` scheduling. Use a sequenced shell orchestrator from your laptop: Phase 1 Tekton runs drain the real Kafka raw topic into typed topics, a short in-cluster ingester lands typed events into TimescaleDB, then Spark refreshes dirty event-time feature windows from the database.

## Goal

Collect epidemic events into TimescaleDB/PostgreSQL for a controlled period (for example 15 minutes smoke or 3 hours) without depending on cluster scheduling.

End-to-end flow:

1. Real raw events arrive at Kafka (course bootstrap).
2. Phase 1: **validate-events** → `validated` → **route-and-enrich** → typed topics (including `vaccination_records` on the vaccination branch).
3. Raw landing: typed topics are persisted to `raw_typed_events` with Kafka topic/partition/offset dedupe.
4. Phase 2 Spark: reads `raw_typed_events` for event-time windows, writes `feature_*_hourly` tables, and records `batch_window_runs`.

Spark remains **batch** jobs, not streaming.

## Recommended operating mode

Use **one orchestrator terminal** for full collection:


| Terminal | Purpose                                                                                     |
| -------- | ------------------------------------------------------------------------------------------- |
| **1**    | `scripts/run-phase1-rawdb-sequenced.sh`: Phase 1 runs, raw ingest catch-up, feature refresh |


The sequenced orchestrator is safest for the namespace quota because it avoids overlapping Phase 1 pods, the ingester pod, and Spark. It also handles out-of-order payload timestamps by aggregating from the raw landing table instead of trying to infer all processing directly from Kafka.

## Prerequisites

```bash
export NAMESPACE=<your-openshift-namespace>
oc project "$NAMESPACE"
```

Apply DB, RBAC, and Spark script ConfigMap:

```bash
oc apply -f infra/database-deployment.yaml -n "$NAMESPACE"
oc apply -f infra/spark-rbac.yaml -n "$NAMESPACE"

oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -
```

After changing `tekton/tasks.yaml` (memory limits), re-apply Tasks and Pipeline:

```bash
oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
```

The Spark job and raw ingester expect Secret `epidemic-db-spark` (`username` / `password`).

## Real raw events (preferred)

Do **not** use `loadgen` for production-style collection unless the raw topic is empty.

```bash
export KAFKA_BOOTSTRAP=kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092
export RAW_TOPIC=ds551-s26.team02.raw
```

Bootstrap = Kafka address; raw topic = what Phase 1 **validate** consumes.

## Optional synthetic loadgen

Only if you need artificial traffic:

```bash
oc create configmap loadgen-produce-script \
  --from-file=produce.py=loadgen/produce.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -

oc delete job loadgen-raw-kafka -n "$NAMESPACE" --ignore-not-found

oc create -f infra/loadgen-job.yaml --dry-run=client -o yaml \
  | oc set env -f - --local -o yaml \
      KAFKA_BOOTSTRAP='kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092' \
      LOADGEN_TOPIC='ds551-s26.team02.raw' \
      LOADGEN_DURATION_SEC='10800' \
      LOADGEN_RATE='10' \
  | oc apply -n "$NAMESPACE" -f -

oc logs -f job/loadgen-raw-kafka -n "$NAMESPACE"
```

## Smoke tests

**Phase 1 (topics):**

```bash
scripts/smoke-phase1-topics.sh
```

**Phase 2 (DB, includes vaccination facts on the CO branch):**

```bash
scripts/smoke-phase2-db.sh
```

## Continuous PipelineRun manifest

**File:** `tekton/pipelinerun-continuous.yaml`

Current intent (check file for exact values):

- `batch-size`: tuned for throughput vs Tekton duration (for example `500` for the current sequenced run).
- `consumer-timeout-ms`: `120000` (2 minutes) — poll idle timeout per batch logic in tasks.
- `route-auto-offset-reset`: `earliest` — route must read records validate already wrote for the same run (fresh route groups must not use `latest` only).

To change batch size permanently, edit that file and `oc apply` is not required for PipelineRun templates — each `oc create` embeds params.

### Batch size vs latency

- **Validate** usually dominates wall time; **route-and-enrich** is often much faster for the same batch.
- Goal for steady operation: **complete validate + route often enough** that typed topics keep moving, not “largest batch possible.”
- If validate routinely exceeds your target (for example 5 minutes), **lower `batch-size`** (for example 250 → 100), not raise it.
- If quota allows and validate is fast, you can **raise** batch size gradually.

### Consumer offsets (short version)

- Repeated runs use the **same consumer group IDs** by default → each run continues from **committed offsets**, not “from the beginning” every time.
- `route-auto-offset-reset: earliest` matters when the route group has **no** committed offset yet; otherwise committed offsets win.
- Course text suggests `auto_offset_reset='earliest'` for raw backlog replay when appropriate; for ongoing operation, committed offsets are normal.

## Phase 1 task order

Inside each `PipelineRun`:

1. **validate-events** — consumes raw, writes **validated**.
2. **route-and-enrich** — runs **after** validate (`runAfter`); consumes **validated**, writes typed topics.

If validate has not finished, **route does not start**. Logs may show validate “done” in stdout before Tekton marks the TaskRun **Succeeded** — trust `oc get taskrun` for completion.

## Shell gotchas (zsh)

- Do **not** put `#` comments on the same line as `end=$(( ... ))` in a pasted block; zsh may error (`command not found: #`). Put comments on their own lines or omit them.
- macOS often has **no `watch`** — use a `while true; do ... sleep 3; done` loop instead.

## Recommended: sequential Phase 1 loop (one run at a time)

This pattern **creates one `PipelineRun`, waits until it finishes**, then creates the next. It avoids overlapping runs, quota pile-ups, and ambiguous “is anything still running?” states.

**15 minutes:**

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

deadline=$(( $(date +%s) + 900 ))

while [ "$(date +%s)" -lt "$deadline" ]; do
  PR=$(oc create -f tekton/pipelinerun-continuous.yaml -n "$NAMESPACE" -o name)
  echo "Started $PR"
  oc wait -n "$NAMESPACE" --for=condition=Succeeded "$PR" --timeout=3600s || true
done
```

**3 hours:** replace `900` with `10800`.

### Anti-pattern: sleep + create without wait

A loop like:

```bash
while ...; do
  oc create -f tekton/pipelinerun-continuous.yaml
  sleep 45
done
```

starts a **new** run every interval **whether or not** the previous run finished. That stacks concurrent `PipelineRun`s, triggers **memory quota** errors, and makes Phase 1 hard to reason about. Prefer `oc wait` (above).

### Optional: guard “only if nothing Running”

If you must use sleep between launches, gate on **Tekton `PipelineRun` status**. For `oc get pipelinerun` **without** `--no-headers`, the `Running` state is in column **3** (`REASON`):

```bash
running=$(oc get pipelinerun -n "$NAMESPACE" --no-headers \
  | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{c++} END{print c+0}')
```

Using the wrong column caused false “no runs” and double launches.

## Namespace memory quota and Tekton tasks

OpenShift project quotas (for example **8Gi** total `limits.memory`) can block new Task pods with `ExceededResourceQuota`. Symptoms:

- `validate` or `route` TaskRuns stay **Pending** / retry.
- Events mention `requested: limits.memory=4Gi` vs `limited: limits.memory=8Gi`.

Mitigations:

1. **Only one Phase 1 run in flight** (sequential loop above).
2. Keep **explicit low `computeResources`** on **validate** and **route** steps in `tekton/tasks.yaml` (for example **512Mi** limit per step after patch) and **re-apply** to the cluster.
3. Avoid launching many `PipelineRun`s in parallel.

After editing tasks, run:

```bash
oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
```

## Monitoring

**Watch PipelineRuns and TaskRuns (no `watch` on Mac):**

```bash
while true; do
  clear
  date
  oc get pipelinerun,taskrun -n "$NAMESPACE" | grep -E 'phase1-continuous-run|NAME'
  sleep 3
done
```

**Logs for a specific run** (`PR=phase1-continuous-run-xxxxx`):

```bash
PR=phase1-continuous-run-xxxxx

oc logs -f -n "$NAMESPACE" "${PR}-validate-pod" --all-containers=true
oc logs -f -n "$NAMESPACE" "${PR}-route-and-enrich-pod" --all-containers=true
```

If pods are gone (run finished), use `--tail=200` without `-f`.

**Validate duration** (wall clock for the validate TaskRun):

```bash
PR=phase1-continuous-run-xxxxx
oc get taskrun "${PR}-validate" -n "$NAMESPACE" \
  -o jsonpath='Start: {.status.startTime}{"\n"}End:   {.status.completionTime}{"\n"}'
```

Same pattern for `${PR}-route-and-enrich` to compare validate vs route time.

## Cancelling runs

**Cancel one run:**

```bash
oc patch pipelinerun <name> -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}'
```

**Cancel all currently Running continuous runs:**

```bash
oc get pipelinerun -n "$NAMESPACE" --no-headers \
  | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{print $1}' \
  | while read -r pr; do
      oc patch pipelinerun "$pr" -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}'
    done
```

Stop any **shell loops** in other terminals (`Ctrl+C`) so they do not create new runs after cancel.

## Raw Landing Sequenced Orchestrator

This is the current recommended operating mode. It separates durable collection from feature computation:

```text
Phase 1 Tekton -> typed Kafka topics -> raw_typed_events -> feature_*_hourly
```

Why this is preferred:

- Kafka messages are not ordered by payload event time; recent typed-topic samples contained mixed event hours in the same offset range.
- `raw_typed_events` gives a durable source of truth keyed by Kafka topic/partition/offset.
- Feature refresh can query dirty event-time hours from the database instead of repeatedly scanning Kafka.
- The orchestrator runs Phase 1, raw ingest, and Spark refresh **sequentially** to avoid namespace memory quota collisions.

### One-Time Setup

Run from the repo root:

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

oc apply -f infra/database-deployment.yaml -n "$NAMESPACE"
oc apply -f infra/spark-rbac.yaml -n "$NAMESPACE"
oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"

oc create configmap spark-etl-script \
  --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -

bash scripts/apply-raw-landing-ddl.sh
```

`oc apply -f infra/database-deployment.yaml` updates manifests, but it does **not** re-run init SQL on an existing database volume. `scripts/apply-raw-landing-ddl.sh` applies the `raw_typed_events` table and indexes to the live DB.

### Recommended Five-Hour Run

Use this for a stable collection run:

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

DURATION_SEC=18000 \
BATCH_SIZE=500 \
PHASE1_RUNS_PER_CYCLE=2 \
INGEST_RUN_SECONDS=30 \
INGEST_MAX_MESSAGES=0 \
MAX_DIRTY_WINDOWS_PER_RUN=8 \
LOG_FILE=logs/runtime/phase1-rawdb-b500-c2-ingest30.log \
bash scripts/run-phase1-rawdb-sequenced.sh
```

What each setting means:

- `BATCH_SIZE=500`: each Phase 1 PipelineRun can consume up to 500 raw records.
- `PHASE1_RUNS_PER_CYCLE=2`: run two Phase 1 batches before raw ingest and feature refresh.
- `INGEST_RUN_SECONDS=30`: run the in-cluster typed-topic ingester long enough to catch up.
- `MAX_DIRTY_WINDOWS_PER_RUN=8`: refresh up to eight dirty event-time hours per Spark cycle.

### Smoke Test

Use this when validating changes:

```bash
DURATION_SEC=900 \
BATCH_SIZE=250 \
PHASE1_RUNS_PER_CYCLE=1 \
INGEST_RUN_SECONDS=45 \
MAX_DIRTY_WINDOWS_PER_RUN=6 \
LOG_FILE=logs/runtime/phase1-rawdb-smoke.log \
bash scripts/run-phase1-rawdb-sequenced.sh
```

### Component Tests

Apply raw landing DDL:

```bash
bash scripts/apply-raw-landing-ddl.sh
```

Run one bounded in-cluster raw ingest:

```bash
RUN_SECONDS=45 MAX_MESSAGES=0 bash scripts/run-raw-typed-ingester.sh once
```

Run one dirty-window feature refresh:

```bash
MAX_DIRTY_WINDOWS_PER_RUN=6 \
REFRESH_SLEEP_SEC=10 \
LOG_FILE=logs/runtime/rawdb-feature-refresh-test.log \
bash scripts/run-rawdb-feature-refresh.sh once
```

Start/stop the ingester manually:

```bash
bash scripts/run-raw-typed-ingester.sh start
bash scripts/run-raw-typed-ingester.sh logs
bash scripts/run-raw-typed-ingester.sh status
bash scripts/run-raw-typed-ingester.sh stop
```

### Verification Queries

Raw landing coverage:

```bash
oc exec -i timescaledb-0 -n "$NAMESPACE" -- psql -U spark_writer -d epidemic -P pager=off -c "
SELECT
  COUNT(*) AS raw_rows,
  COUNT(*) FILTER (WHERE event_ts IS NOT NULL) AS rows_with_event_ts,
  MIN(ingested_at) AS first_ingested_at,
  MAX(ingested_at) AS latest_ingested_at
FROM raw_typed_events;

SELECT
  date_trunc('hour', event_ts) AS event_hour,
  COUNT(*) AS rows,
  MAX(ingested_at) AS latest_ingested_at
FROM raw_typed_events
WHERE event_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;
"
```

Feature/window coverage:

```bash
oc exec -i timescaledb-0 -n "$NAMESPACE" -- psql -U spark_writer -d epidemic -P pager=off -c "
SELECT window_start, window_end, status, batch_id, completed_at
FROM batch_window_runs
ORDER BY completed_at DESC NULLS LAST
LIMIT 20;

WITH raw_hours AS (
  SELECT date_trunc('hour', event_ts) AS bucket_start, COUNT(*) AS raw_rows
  FROM raw_typed_events
  WHERE event_ts IS NOT NULL
  GROUP BY 1
),
success AS (
  SELECT DISTINCT ON (window_start)
    window_start AS bucket_start,
    batch_id,
    completed_at
  FROM batch_window_runs
  WHERE status = 'succeeded'
  ORDER BY window_start, completed_at DESC NULLS LAST
)
SELECT r.bucket_start, r.raw_rows, s.batch_id AS latest_success_batch, s.completed_at
FROM raw_hours r
LEFT JOIN success s USING (bucket_start)
ORDER BY r.bucket_start;
"
```

Event-time disorder check:

```bash
oc exec -i timescaledb-0 -n "$NAMESPACE" -- psql -U spark_writer -d epidemic -P pager=off -c "
SELECT
  date_trunc('hour', ingested_at) AS ingest_hour,
  date_trunc('hour', event_ts) AS event_hour,
  COUNT(*) AS rows
FROM raw_typed_events
WHERE event_ts IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
"
```

### Stop Active Work

```bash
oc get pipelinerun -n "$NAMESPACE" --no-headers \
  | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{print $1}' \
  | while read -r pr; do
      oc patch pipelinerun "$pr" -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}'
    done

oc delete job epidemic-spark-etl -n "$NAMESPACE" --ignore-not-found
oc delete pod raw-typed-ingester -n "$NAMESPACE" --ignore-not-found
```

## Legacy Event-Time Spark Orchestrator

Spark must target **event-time windows**, not necessarily the current wall-clock hour. Live testing showed Phase 1 can route records at the current time while the payload `timestamp` values are historical (for example `2026-04-29T12:54:35Z`). In that case, a Spark run for the current closed hour succeeds but writes **0 rows**, because no event timestamps fall inside that wall-clock window.

This older path reads typed Kafka directly from Spark. Keep it for manual debugging, but prefer the raw landing sequenced orchestrator above for collection runs.

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1

FRESH_CONSUMER_GROUPS=1 \
SPARK_EVERY_PHASE1_RUNS=1 \
EVENTTIME_SAMPLE_TIMEOUT_MS=2000 \
EVENTTIME_SAMPLE_MESSAGES=50 \
EVENTTIME_LOOKBACK_HOURS=2 \
EVENTTIME_MAX_WINDOWS_PER_CHECK=4 \
EVENTTIME_CURSOR_MODE=db \
LOG_FILE=logs/runtime/eventtime-continuous.log \
bash scripts/run-phase1-spark-orchestrated.sh
```

For a quick test:

```bash
DURATION_SEC=600 \
FRESH_CONSUMER_GROUPS=1 \
SPARK_EVERY_PHASE1_RUNS=1 \
EVENTTIME_SAMPLE_TIMEOUT_MS=2000 \
EVENTTIME_SAMPLE_MESSAGES=50 \
EVENTTIME_LOOKBACK_HOURS=2 \
EVENTTIME_MAX_WINDOWS_PER_CHECK=4 \
EVENTTIME_CURSOR_MODE=db \
LOG_FILE=logs/runtime/eventtime-test.log \
bash scripts/run-phase1-spark-orchestrated.sh
```

The script:

- Runs Phase 1 sequentially with configurable consumer groups.
- Samples recent messages from typed topics.
- Samples typed-topic messages only to find the latest event-time upper bound.
- Uses `batch_window_runs` as the default cursor (`EVENTTIME_CURSOR_MODE=db`) to find missing successful hourly windows through that upper bound.
- Refreshes the latest sampled event-time hour when DB cursor mode finds no missing windows (`EVENTTIME_REFRESH_LATEST_WHEN_COVERED=1`, default). This keeps live collection moving when new Kafka messages keep arriving with payload timestamps in the same hour.
- Initializes the cursor from the latest sampled hour minus `EVENTTIME_LOOKBACK_HOURS - 1`; override with `EVENTTIME_CURSOR_START_UTC` to force a starting hour.
- Processes at most `EVENTTIME_MAX_WINDOWS_PER_CHECK` windows each Spark cycle so long backfills do not starve Phase 1.
- Launches Spark for that event-time window with a unique `batch_id`.
- Logs Phase 1 summaries and Spark `[ANALYTICS]` lines to `LOG_FILE`.
- Cancels active PipelineRuns and deletes the Spark job on `Ctrl+C`.

Spark reads Kafka with `startingOffsets` / `endingOffsets` batch semantics but **filters by event time** using `WINDOW_START_UTC` / `WINDOW_END_UTC`. A successful Spark job with `rows_written_total=0` usually means the selected window did not match the payload timestamps.

For a strict one-pass historical backfill, set `EVENTTIME_REFRESH_LATEST_WHEN_COVERED=0` so succeeded windows are treated as final and skipped on later checks.

If you need to force a known event-time window manually:

```bash
WINDOW_START_UTC="2026-04-29T12:00:00Z"
WINDOW_END_UTC="2026-04-29T13:00:00Z"
RUN_ID="eventtime-$(date -u +%Y%m%dT%H%M%SZ)"
BATCH_ID="eventtime-2026-04-29T12-$(date -u +%Y%m%dT%H%M%SZ)"

oc delete job epidemic-spark-etl -n "$NAMESPACE" --ignore-not-found --wait=true

oc create -f infra/spark-job.yaml --dry-run=client -o yaml \
  | oc set env -f - --local -o yaml \
      WINDOW_START_UTC="$WINDOW_START_UTC" \
      WINDOW_END_UTC="$WINDOW_END_UTC" \
      REQUIRE_WINDOW_UTC=true \
      RUN_ID="$RUN_ID" \
      BATCH_ID="$BATCH_ID" \
  | oc apply -n "$NAMESPACE" -f -
```

## Database checks

```bash
oc logs -f job/epidemic-spark-etl -n "$NAMESPACE"
```

```sql
SELECT batch_id, window_start, window_end, status, completed_at
FROM batch_window_runs
ORDER BY completed_at DESC
LIMIT 20;

SELECT COUNT(*) FROM feature_region_hourly;
SELECT COUNT(*) FROM feature_vaccination_hourly;
```

## Clean Run Reset

For a clean performance run, stop active work and truncate only the analytics state. This keeps the database schema, users, secrets, and views intact.

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1

oc get pipelinerun -n "$NAMESPACE" --no-headers \
  | awk '$3=="Running"{print $1}' \
  | while read -r pr; do
      oc patch pipelinerun "$pr" -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}'
    done

oc delete job epidemic-spark-etl -n "$NAMESPACE" --ignore-not-found --wait=false

oc exec -i timescaledb-0 -n "$NAMESPACE" -- psql -U spark_writer -d epidemic -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE
  batch_window_runs,
  feature_region_hourly,
  feature_clinic_hourly,
  feature_hospital_hourly,
  feature_diagnosis_hourly,
  feature_symptom_hourly,
  feature_vaccination_hourly;
SQL
```

Verify:

```bash
oc exec -i timescaledb-0 -n "$NAMESPACE" -- psql -U spark_writer -d epidemic -P pager=off -c "
SELECT 'batch_window_runs' AS table_name, COUNT(*) FROM batch_window_runs
UNION ALL SELECT 'feature_region_hourly', COUNT(*) FROM feature_region_hourly
UNION ALL SELECT 'feature_clinic_hourly', COUNT(*) FROM feature_clinic_hourly
UNION ALL SELECT 'feature_hospital_hourly', COUNT(*) FROM feature_hospital_hourly
UNION ALL SELECT 'feature_diagnosis_hourly', COUNT(*) FROM feature_diagnosis_hourly
UNION ALL SELECT 'feature_symptom_hourly', COUNT(*) FROM feature_symptom_hourly
UNION ALL SELECT 'feature_vaccination_hourly', COUNT(*) FROM feature_vaccination_hourly
ORDER BY table_name;
"
```

## Kafka Event-Time Check

Use this to verify sampler behavior directly inside the Kafka pod:

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
POD=$(oc get pods -n "$NAMESPACE" --no-headers | awk '$1 ~ /kafka/ && $3=="Running"{print $1; exit}')

oc exec -n "$NAMESPACE" "$POD" -- /bin/bash -lc '
set -euo pipefail
BOOT=kafka:9092
TOPIC=ds551-s26.team02.symptom_reports

OFFS=$(kafka-get-offsets --bootstrap-server "$BOOT" --topic "$TOPIC" --time -1)
echo "$OFFS"

echo "$OFFS" | while IFS=: read -r T P L; do
  [ -n "$L" ] || continue
  [ "$L" -gt 0 ] || continue
  START=$(( L > 200 ? L - 200 : 0 ))
  MAX=$(( L - START ))
  echo "partition=$P start=$START max=$MAX"

  kafka-console-consumer --bootstrap-server "$BOOT" \
    --topic "$T" --partition "$P" --offset "$START" \
    --max-messages "$MAX" --timeout-ms 15000 \
  | python3 -c "import sys,json
count=0
for line in sys.stdin:
  try:
    o=json.loads(line)
    ts=o.get(\"timestamp\") or o.get(\"administered_at\")
    if ts:
      print(ts); count+=1
      if count>=10: break
  except: pass
print(f\"timestamps_found={count}\")"
done
'
```

If this prints multiple event-hour buckets in recent offsets, the stream is out of order by payload event time. That is expected in the current course feed and is why the raw landing path is preferred.

## Summary

1. Use **real** raw Kafka traffic unless loadgen is explicitly needed.
2. Prefer `scripts/run-phase1-rawdb-sequenced.sh` for long runs; it sequences Phase 1, raw ingest, and Spark refresh to stay under quota.
3. Treat payload timestamps as event time; they can arrive out of order and in historical buckets.
4. Keep `raw_typed_events` as the durable collection layer, then refresh dirty feature windows from DB.
5. Tune `BATCH_SIZE`, `PHASE1_RUNS_PER_CYCLE`, `INGEST_RUN_SECONDS`, and `MAX_DIRTY_WINDOWS_PER_RUN` for throughput vs freshness.

