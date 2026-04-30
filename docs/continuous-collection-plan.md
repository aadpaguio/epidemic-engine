# Timed Continuous Collection Plan

This plan avoids Kubernetes `CronJob` scheduling. Use long-lived shell loops from your laptop: Phase 1 Tekton runs drain the real Kafka raw topic into typed topics; a separate loop (or manual ticks) launches Spark on closed UTC hours so TimescaleDB accumulates hourly feature batches.

## Goal

Collect epidemic events into TimescaleDB/PostgreSQL for a controlled period (for example 15 minutes smoke or 3 hours) without depending on cluster scheduling.

End-to-end flow:

1. Real raw events arrive at Kafka (course bootstrap).
2. Phase 1: **validate-events** → `validated` → **route-and-enrich** → typed topics (including `vaccination_records` on the vaccination branch).
3. Phase 2 Spark: reads typed topics for an explicit `**WINDOW_START_UTC` / `WINDOW_END_UTC`**, writes `**feature_*_hourly**` tables.

Spark remains **batch** jobs, not streaming.

## Recommended operating mode

Use **one orchestrator terminal** for full collection:


| Terminal | Purpose                                                                                            |
| -------- | -------------------------------------------------------------------------------------------------- |
| **1**    | `scripts/run-phase1-spark-orchestrated.sh`: sequential Phase 1 runs plus event-time Spark launches |


The older two-terminal pattern is still useful for manual debugging, but the orchestrator is safer for the namespace quota because it alternates Phase 1 and Spark instead of allowing the 4Gi Spark job to collide with active Tekton pods.

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

After changing `**tekton/tasks.yaml**` (memory limits), re-apply Tasks and Pipeline:

```bash
oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
```

The Spark job expects Secret `**epidemic-db-spark**` (`username` / `password`).

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

- `**batch-size`:** tuned for throughput vs Tekton duration (for example `100` for shorter validate steps).
- `**consumer-timeout-ms`:** `120000` (2 minutes) — poll idle timeout per batch logic in tasks.
- `**route-auto-offset-reset`:** `earliest` — route must read records validate already wrote for the same run (fresh route groups must not use `latest` only).

To change batch size permanently, edit that file and `oc apply` is not required for PipelineRun templates — each `oc create` embeds params.

### Batch size vs latency

- **Validate** usually dominates wall time; **route-and-enrich** is often much faster for the same batch.
- Goal for steady operation: **complete validate + route often enough** that typed topics keep moving, not “largest batch possible.”
- If validate routinely exceeds your target (for example 5 minutes), **lower `batch-size`** (for example 250 → 100), not raise it.
- If quota allows and validate is fast, you can **raise** batch size gradually.

### Consumer offsets (short version)

- Repeated runs use the **same consumer group IDs** by default → each run continues from **committed offsets**, not “from the beginning” every time.
- `**route-auto-offset-reset: earliest`** matters when the route group has **no** committed offset yet; otherwise committed offsets win.
- Course text suggests `**auto_offset_reset='earliest'`** for raw backlog replay when appropriate; for ongoing operation, committed offsets are normal.

## Phase 1 task order

Inside each `PipelineRun`:

1. **validate-events** — consumes raw, writes **validated**.
2. **route-and-enrich** — runs **after** validate (`runAfter`); consumes **validated**, writes typed topics.

If validate has not finished, **route does not start**. Logs may show validate “done” in stdout before Tekton marks the TaskRun **Succeeded** — trust `**oc get taskrun`** for completion.

## Shell gotchas (zsh)

- Do **not** put `**#` comments** on the same line as `end=$(( ... ))` in a pasted block; zsh may error (`command not found: #`). Put comments on their own lines or omit them.
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

starts a **new** run every interval **whether or not** the previous run finished. That stacks concurrent `PipelineRun`s, triggers **memory quota** errors, and makes Phase 1 hard to reason about. Prefer `**oc wait`** (above).

### Optional: guard “only if nothing Running”

If you must use sleep between launches, gate on **Tekton `PipelineRun` status**. For `oc get pipelinerun` **without** `--no-headers`, the `**Running`** state is in column **3** (`REASON`):

```bash
running=$(oc get pipelinerun -n "$NAMESPACE" --no-headers \
  | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{c++} END{print c+0}')
```

Using the wrong column caused false “no runs” and double launches.

## Namespace memory quota and Tekton tasks

OpenShift project quotas (for example **8Gi** total `limits.memory`) can block new Task pods with `**ExceededResourceQuota`**. Symptoms:

- `validate` or `route` TaskRuns stay **Pending** / retry.
- Events mention `**requested: limits.memory=4Gi`** vs `**limited: limits.memory=8Gi**`.

Mitigations:

1. **Only one Phase 1 run in flight** (sequential loop above).
2. Keep **explicit low `computeResources`** on **validate** and **route** steps in `**tekton/tasks.yaml`** (for example **512Mi** limit per step after patch) and **re-apply** to the cluster.
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

Same pattern for `**${PR}-route-and-enrich**` to compare validate vs route time.

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

## Event-Time Spark Orchestrator

Spark must target **event-time windows**, not necessarily the current wall-clock hour. Live testing showed Phase 1 can route records at the current time while the payload `timestamp` values are historical (for example `2026-04-29T12:54:35Z`). In that case, a Spark run for the current closed hour succeeds but writes **0 rows**, because no event timestamps fall inside that wall-clock window.

Use the orchestrator so Phase 1 and Spark share the namespace quota safely and Spark picks a typed-topic event-time window with data:

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

Spark reads Kafka with `**startingOffsets`/`endingOffsets**` batch semantics but **filters by event time** using `WINDOW_START_UTC` / `WINDOW_END_UTC`. A successful Spark job with `rows_written_total=0` usually means the selected window did not match the payload timestamps.

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

## Five-Hour Run

The current recommended mode uses the DB-driven event-time cursor:

- Phase 1 runs sequentially.
- Spark checks after every Phase 1 run.
- Kafka sampling only finds the latest event-time upper bound.
- `batch_window_runs` decides which hourly windows are still missing.
- If no DB windows are missing, the orchestrator refreshes the latest sampled event-time hour by default. This is useful when the course feed keeps producing records whose payload timestamps are pinned to the same old hour.
- Up to `EVENTTIME_MAX_WINDOWS_PER_CHECK` missing windows are processed per Spark cycle.

Run:

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

DURATION_SEC=18000 \
FRESH_CONSUMER_GROUPS=1 \
EVENTTIME_CURSOR_MODE=db \
BATCH_SIZE=250 \
CONSUMER_TIMEOUT_MS=60000 \
SPARK_EVERY_PHASE1_RUNS=1 \
EVENTTIME_LOOKBACK_HOURS=2 \
EVENTTIME_MAX_WINDOWS_PER_CHECK=4 \
EVENTTIME_SAMPLE_MESSAGES=200 \
EVENTTIME_SAMPLE_TIMEOUT_MS=15000 \
LOG_FILE=logs/runtime/five-hour-continuous.log \
bash scripts/run-phase1-spark-orchestrated.sh
```

### Sampler tuning note

In this environment, sampler settings that are too small can produce false negatives (`no typed-topic event timestamps found`) even when Phase 1 route logs show healthy nonzero output.

Use:

- `EVENTTIME_SAMPLE_MESSAGES=200`
- `EVENTTIME_SAMPLE_TIMEOUT_MS=15000`

The issue was a Kafka console-consumer timeout with low timeout/slice values, not missing typed-topic data.

### Isolated sampler test

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

If this prints timestamps, keep DB cursor mode and raise sampler timeout/messages as above.

## Summary

1. Use **real** raw Kafka traffic unless loadgen is explicitly needed.
2. Prefer `**scripts/run-phase1-spark-orchestrated.sh`** for long runs; it keeps Phase 1 sequential and launches Spark without quota overlap.
3. Spark should process **payload event-time windows**, not blindly the latest wall-clock hour.
4. A successful Spark run with `rows_written_total=0` usually means the selected window does not match the event timestamps in the typed topics.
5. Tune `**batch-size`**, `**SPARK_EVERY_PHASE1_RUNS**`, and `**EVENTTIME_LOOKBACK_HOURS**` so validate+route complete reliably and Spark has enough routed data to aggregate without missing late event-time windows.

