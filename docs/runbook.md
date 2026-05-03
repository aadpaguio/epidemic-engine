# Runbook

Timed collection **without** Kubernetes `CronJob` scheduling: you drive Phase 1 Tekton, raw landing, and Spark batch jobs from your laptop with shell scripts. Spark stays **batch-only**, not structured streaming.

For the **system map** (components, schemas, idempotency), see [`architecture.md`](architecture.md).

---

## 1. Current operating mode (canonical)

**Use this unless you are debugging a specific sub-component.**

### What to run

| Item | Role |
|------|------|
| **`scripts/run-phase1-rawdb-sequenced.sh`** | Single orchestrator: **Phase 1 Tekton** → **typed Kafka** → **`raw_typed_events`** ingest → **Spark** dirty-window refresh (`FEATURE_SOURCE_MODE=raw_db`) |

One terminal for the full loop. Do **not** run overlapping Phase 1 `PipelineRun`s, a long-lived ingester, and Spark jobs at the same time unless you understand namespace quota risk.

### Why this mode is default

1. **Memory quota:** Sequential runs avoid stacking validate/route pods, the ingester pod, and Spark.
2. **Event time vs Kafka order:** Payload `timestamp` values are **not** aligned with Kafka offset order; `raw_typed_events` stores durable rows keyed by `(topic, partition, offset)` so Spark can aggregate by **event-time** windows from SQL.
3. **Dirty windows:** Feature refresh selects hours that have raw landings but incomplete / stale `batch_window_runs`, instead of blindly rescanning Kafka.

### End-to-end flow

```text
Kafka raw (course generator)
  -> Tekton validate-events -> validated -> route-and-enrich -> typed topics
        -> short ingester poll -> raw_typed_events (dedupe by Kafka offset)
              -> Spark batch -> feature_*_hourly + batch_window_runs
```

### Primary commands

**Stable multi-hour collection** (example: five hours; tune env vars as needed):

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

**Smoke / shorter validation:**

```bash
DURATION_SEC=900 \
BATCH_SIZE=250 \
PHASE1_RUNS_PER_CYCLE=1 \
INGEST_RUN_SECONDS=45 \
MAX_DIRTY_WINDOWS_PER_RUN=6 \
LOG_FILE=logs/runtime/phase1-rawdb-smoke.log \
bash scripts/run-phase1-rawdb-sequenced.sh
```

### Knobs (sequenced script)

| Variable | Typical meaning |
|----------|-----------------|
| `DURATION_SEC` | Wall-clock duration of the orchestrator loop |
| `BATCH_SIZE` | Max raw records per Phase 1 `PipelineRun` (`tekton/pipelinerun-continuous.yaml` template) |
| `PHASE1_RUNS_PER_CYCLE` | How many Phase 1 runs before ingest + Spark refresh |
| `INGEST_RUN_SECONDS` | Bounded ingester poll duration per cycle |
| `INGEST_MAX_MESSAGES` | Optional cap on ingested messages (`0` = use time bound only) |
| `MAX_DIRTY_WINDOWS_PER_RUN` | Max event-time hours Spark refreshes per cycle |
| `PHASE1_TIMEOUT_SEC` | Per-run timeout (default often `1200`); `PHASE1_CANCEL_ON_TIMEOUT=1` avoids overlap with the next step |

### One-time setup (before first canonical run)

From repo root, with `oc project` set:

```bash
oc apply -f infra/database-deployment.yaml
oc apply -f infra/spark-rbac.yaml
oc apply -f tekton/tasks.yaml
oc apply -f tekton/pipeline.yaml

oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -f -

bash scripts/apply-raw-landing-ddl.sh
```

**Notes:**

- `infra/database-deployment.yaml` updates manifests but **does not** re-run init SQL on an existing PVC. Use **`scripts/apply-raw-landing-ddl.sh`** so `raw_typed_events` exists on a live DB.
- Secret **`epidemic-db-spark`** must provide `username` / `password` for Spark and the ingester.
- After editing **`tekton/tasks.yaml`** (e.g. memory limits), re-apply **`tekton/tasks.yaml`** and **`tekton/pipeline.yaml`**.

### Raw traffic vs loadgen

**Preferred:** real events on the team raw topic (generator). Example env for reference:

```bash
export KAFKA_BOOTSTRAP=kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092
export RAW_TOPIC=ds551-s26.team02.raw
```

Do **not** use `loadgen` for production-style collection unless the raw topic is empty or you explicitly need synthetic traffic (see [Optional loadgen](#optional-loadgen)).

---

## 2. Quick checklist before any run

- [ ] `oc project` points at the correct namespace
- [ ] Kafka raw topic is receiving events (or you intend to use loadgen)
- [ ] `epidemic-db-spark` secret exists
- [ ] ConfigMap `spark-etl-script` matches current `spark/etl.py` if you changed ETL logic
- [ ] `raw_typed_events` present (`apply-raw-landing-ddl.sh` if unsure)

---

## 3. Smoke tests (fast gates)

**Phase 1 (Kafka topics):**

```bash
scripts/smoke-phase1-topics.sh
```

**Phase 2 / DB (includes vaccination facts):**

```bash
scripts/smoke-phase2-db.sh
```

---

## 4. Verify collection

### Raw landing

```bash
oc exec timescaledb-0 -- psql -U spark_writer -d epidemic -P pager=off -c "
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

### Feature windows vs raw hours

```bash
oc exec timescaledb-0 -- psql -U spark_writer -d epidemic -P pager=off -c "
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

### Event-time vs ingest-time (disorder check)

```bash
oc exec timescaledb-0 -- psql -U spark_writer -d epidemic -P pager=off -c "
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

### Spark job logs

```bash
oc logs -f job/epidemic-spark-etl
```

### Quick fact counts

```sql
SELECT COUNT(*) FROM feature_region_hourly;
SELECT COUNT(*) FROM feature_vaccination_hourly;
```

---

## 5. Operate during a run

### Watch Tekton (macOS-friendly loop)

```bash
while true; do
  clear
  date
  oc get pipelinerun,taskrun | grep -E 'phase1-continuous-run|NAME'
  sleep 3
done
```

### Logs for a specific PipelineRun

```bash
PR=phase1-continuous-run-xxxxx

oc logs -f "${PR}-validate-pod" --all-containers=true
oc logs -f "${PR}-route-and-enrich-pod" --all-containers=true
```

If pods already exited, use `--tail=200` without `-f`.

### Validate vs route duration

```bash
PR=phase1-continuous-run-xxxxx
oc get taskrun "${PR}-validate" \
  -o jsonpath='Start: {.status.startTime}{"\n"}End:   {.status.completionTime}{"\n"}'
```

Same pattern for `${PR}-route-and-enrich`.

---

## 6. Stop and cancel

**Cancel running continuous Phase 1 runs:**

```bash
oc get pipelinerun --no-headers \
  | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{print $1}' \
  | while read -r pr; do
      oc patch pipelinerun "$pr" --type merge -p '{"spec":{"status":"Cancelled"}}'
    done
```

**Stop Spark + ingester helper:**

```bash
oc delete job epidemic-spark-etl --ignore-not-found
oc delete pod raw-typed-ingester --ignore-not-found
```

Stop shell loops in other terminals (`Ctrl+C`) so they do not spawn new runs after cancel.

---

## 7. Troubleshooting

### Namespace memory quota (`ExceededResourceQuota`)

Symptoms: validate/route TaskRuns **Pending**, limits like **4Gi** vs project cap **8Gi**.

**Mitigations:**

1. Run **one** Phase 1 run at a time (sequenced script already does this).
2. Keep **low `computeResources`** on validate/route in `tekton/tasks.yaml` (re-apply tasks + pipeline).
3. Avoid parallel `PipelineRun` loops.

### Batch size vs latency

- **Validate** often dominates wall time.
- If runs routinely exceed your target (e.g. five minutes), **lower** `BATCH_SIZE` in `tekton/pipelinerun-continuous.yaml` (e.g. 250 → 100), not raise it.
- Goal: complete validate + route **often enough** that typed topics move; not “maximize batch size.”

### Consumer offsets

- Repeated runs reuse **consumer group IDs** → continue from **committed** offsets.
- `route-auto-offset-reset: earliest` in `pipelinerun-continuous.yaml` matters when the route group has **no** committed offset yet.

### Shell gotchas (zsh)

- Do **not** put `#` comments on the same line as `end=$(( ... ))`; put comments on their own line.
- macOS may lack **`watch`** — use the `while` loop in [Operate during a run](#5-operate-during-a-run).

### Anti-pattern: sleep + create without `oc wait`

```bash
while ...; do
  oc create -f tekton/pipelinerun-continuous.yaml
  sleep 45
done
```

This stacks concurrent `PipelineRun`s and blows quota. Prefer **`oc wait --for=condition=Succeeded`** on each run (see [Alternative: Phase 1 only loop](#9-alternative-modes-debugging--legacy)).

---

## 8. Component-level procedures (manual debugging)

Use when isolating ingest vs Spark vs Tekton.

**Apply raw landing DDL:**

```bash
bash scripts/apply-raw-landing-ddl.sh
```

**One bounded ingester pass:**

```bash
RUN_SECONDS=45 MAX_MESSAGES=0 bash scripts/run-raw-typed-ingester.sh once
```

**Ingester lifecycle:**

```bash
bash scripts/run-raw-typed-ingester.sh start
bash scripts/run-raw-typed-ingester.sh logs
bash scripts/run-raw-typed-ingester.sh status
bash scripts/run-raw-typed-ingester.sh stop
```

Default `TOPICS` includes `ds551-s26.team02.vaccination_records` (CO-2026-01).

**One dirty-window feature refresh:**

```bash
MAX_DIRTY_WINDOWS_PER_RUN=6 \
REFRESH_SLEEP_SEC=10 \
LOG_FILE=logs/runtime/rawdb-feature-refresh-test.log \
bash scripts/run-rawdb-feature-refresh.sh once
```

**Manual Spark window (`raw_db`):**

```bash
WINDOW_START_UTC="2026-04-30T00:00:00Z"
WINDOW_END_UTC="2026-04-30T01:00:00Z"
RUN_ID="hourly-$(date -u +%Y%m%dT%H%M%SZ)"
BATCH_ID="dbfeat-$(date -u +%Y%m%dT%H%M%SZ)"

oc delete job epidemic-spark-etl --ignore-not-found --wait=true
oc create -f infra/spark-job.yaml --dry-run=client -o yaml \
  | oc set env -f - --local -o yaml \
      WINDOW_START_UTC="$WINDOW_START_UTC" \
      WINDOW_END_UTC="$WINDOW_END_UTC" \
      REQUIRE_WINDOW_UTC=true \
      FEATURE_SOURCE_MODE=raw_db \
      RUN_ID="$RUN_ID" \
      BATCH_ID="$BATCH_ID" \
  | oc apply -f -
```

---

## 9. Alternative modes (debugging / legacy)

### 9.1 Phase 1 only (Tekton loop, no sequenced raw-db orchestrator)

Creates one `PipelineRun`, **waits** for success, then repeats. Safer than blind sleep loops.

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

deadline=$(( $(date +%s) + 900 ))   # 15 minutes; use 10800 for 3 hours

while [ "$(date +%s)" -lt "$deadline" ]; do
  PR=$(oc create -f tekton/pipelinerun-continuous.yaml -o name)
  echo "Started $PR"
  oc wait --for=condition=Succeeded "$PR" --timeout=3600s || true
done
```

**PipelineRun template:** `tekton/pipelinerun-continuous.yaml`. Typical params:

- `batch-size` — throughput vs Tekton duration (e.g. `500`).
- `consumer-timeout-ms` — e.g. `120000` (poll idle timeout in task logic).
- `route-auto-offset-reset` — **`earliest`** so route reads what validate wrote for fresh consumer groups.

Each `oc create` embeds params; editing the file does not require a separate `oc apply` for PipelineRun **templates**.

Inside each run: **validate-events** then **route-and-enrich** (`runAfter`). Route does not start until validate finishes.

### 9.2 Legacy event-time Spark orchestrator (typed Kafka, not raw-db first)

**Prefer the canonical sequenced script** for collection. Use `scripts/run-phase1-spark-orchestrated.sh` when debugging Kafka-read paths or comparing behavior.

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

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

**Why it exists:** Spark must target **event-time** windows. Phase 1 can deliver messages whose payload timestamps sit in historical hours; a wall-clock Spark window may write **0 rows**. The legacy script samples typed topics and coordinates windows with `batch_window_runs`.

**Manual Spark for a fixed window (Kafka or DB depending on env):**

```bash
WINDOW_START_UTC="2026-04-29T12:00:00Z"
WINDOW_END_UTC="2026-04-29T13:00:00Z"
RUN_ID="eventtime-$(date -u +%Y%m%dT%H%M%SZ)"
BATCH_ID="eventtime-2026-04-29T12-$(date -u +%Y%m%dT%H%M%SZ)"

oc delete job epidemic-spark-etl --ignore-not-found --wait=true

oc create -f infra/spark-job.yaml --dry-run=client -o yaml \
  | oc set env -f - --local -o yaml \
      WINDOW_START_UTC="$WINDOW_START_UTC" \
      WINDOW_END_UTC="$WINDOW_END_UTC" \
      REQUIRE_WINDOW_UTC=true \
      RUN_ID="$RUN_ID" \
      BATCH_ID="$BATCH_ID" \
  | oc apply -f -
```

`rows_written_total=0` usually means no payload timestamps fell in the chosen window.

---

## 10. Clean reset (analytics tables only)

Stops work and truncates fact + window metadata; **keeps** schema, users, secrets, views.

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

oc get pipelinerun --no-headers \
  | awk '$3=="Running"{print $1}' \
  | while read -r pr; do
      oc patch pipelinerun "$pr" --type merge -p '{"spec":{"status":"Cancelled"}}'
    done

oc delete job epidemic-spark-etl --ignore-not-found --wait=false

oc exec -i timescaledb-0 -- psql -U spark_writer -d epidemic -v ON_ERROR_STOP=1 <<'SQL'
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

Verify empty facts:

```bash
oc exec timescaledb-0 -- psql -U spark_writer -d epidemic -P pager=off -c "
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

**Optional:** truncate `raw_typed_events` only if you intend to replay from Kafka from scratch (document team policy before doing this in shared namespaces).

---

## 11. Kafka event-time sampler (deep debug)

Run inside the cluster to confirm payload timestamps span multiple event hours at recent offsets:

```bash
POD=$(oc get pods --no-headers | awk '$1 ~ /kafka/ && $3=="Running"{print $1; exit}')

oc exec "$POD" -- /bin/bash -lc '
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

If multiple event-hour buckets appear in recent offsets, **raw landing + DB-backed Spark** remains the right default.

---

## 12. Optional loadgen

Only when the raw topic is empty or you need synthetic load:

```bash
oc create configmap loadgen-produce-script \
  --from-file=produce.py=loadgen/produce.py \
  --dry-run=client -o yaml | oc apply -f -

oc delete job loadgen-raw-kafka --ignore-not-found

oc create -f infra/loadgen-job.yaml --dry-run=client -o yaml \
  | oc set env -f - --local -o yaml \
      KAFKA_BOOTSTRAP='kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092' \
      LOADGEN_TOPIC='ds551-s26.team02.raw' \
      LOADGEN_DURATION_SEC='10800' \
      LOADGEN_RATE='10' \
  | oc apply -f -

oc logs -f job/loadgen-raw-kafka
```

---

## 13. Continuous multi-window refresh only (no Phase 1 in loop)

When typed data is already landed and you only want Spark to churn dirty windows:

```bash
export NAMESPACE=ds551-2026-spring-a5f0d1
oc project "$NAMESPACE"

PHASE1_RUNS_PER_CYCLE=3 \
MAX_DIRTY_WINDOWS_PER_RUN=6 \
REFRESH_SLEEP_SEC=60 \
LOG_FILE=logs/runtime/rawdb-feature-refresh.log \
bash scripts/run-rawdb-feature-refresh.sh
```

---

## 14. Summary

| Priority | Action |
|----------|--------|
| 1 | Prefer **real** raw Kafka traffic over loadgen unless necessary |
| 2 | Run **`scripts/run-phase1-rawdb-sequenced.sh`** for normal collection — Phase 1, ingest, Spark **sequenced** |
| 3 | Treat payload **`timestamp`** as event time; expect disorder vs Kafka order |
| 4 | Keep **`raw_typed_events`** as the durable layer; refresh **dirty** feature windows from DB |
| 5 | Tune **`BATCH_SIZE`**, **`PHASE1_RUNS_PER_CYCLE`**, **`INGEST_RUN_SECONDS`**, **`MAX_DIRTY_WINDOWS_PER_RUN`** for throughput vs freshness |
| 6 | Use **alternative** sections only for Tekton-only loops, legacy Kafka orchestration, or component isolation |
