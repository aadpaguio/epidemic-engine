# Timed Continuous Collection Plan

This plan avoids Kubernetes `CronJob` scheduling. Instead, keep one terminal running for a fixed wall-clock duration, repeatedly launch the existing one-shot Phase 1 and Spark jobs, and let the database accumulate hourly feature batches.

## Goal

Collect epidemic events into TimescaleDB/PostgreSQL for a controlled period, such as 3 hours, without depending on cluster scheduling.

The existing architecture remains unchanged:

1. Real raw events already arrive in Kafka through the course bootstrap server.
2. Phase 1 Tekton pipeline validates and routes raw events into typed Kafka topics.
3. Spark ETL reads typed Kafka topics for an explicit time window.
4. Spark writes hourly feature tables in the database.

Spark is still batch-oriented. The timed runner simply repeats the batch process until the requested duration ends.

## Recommended Operating Model

Run one long-lived collection terminal, plus optional monitoring terminals:

1. **Timed collection loop:** repeatedly runs Phase 1 against the real raw Kafka topic.
2. **Hourly Spark launch:** writes closed-hour typed records into TimescaleDB.
3. **Monitoring:** watches Tekton, Spark, Kafka lag, or database row counts.

This is easier to debug than a CronJob because logs stay visible in the terminal and each failed command stops where the problem happened.

## Prerequisites

From the repository root:

```bash
export NAMESPACE=<your-openshift-namespace>
oc project "$NAMESPACE"
```

Make sure the database, RBAC, and Spark script ConfigMap are applied:

```bash
oc apply -f infra/database-deployment.yaml -n "$NAMESPACE"
oc apply -f infra/spark-rbac.yaml -n "$NAMESPACE"

oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -
```

The Spark job expects the `epidemic-db-spark` secret to exist with `username` and `password` keys.

## Real Raw Events

For the actual 3-hour collection, do **not** run `loadgen`. Use the real raw events already available from Kafka:

```bash
export KAFKA_BOOTSTRAP=kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092
export RAW_TOPIC=ds551-s26.team02.raw
```

The bootstrap server is the Kafka address. The raw topic is the stream Phase 1 consumes.

## Optional Synthetic Data

Use `loadgen` only if the real raw topic is quiet or unavailable. It is a fallback producer, not part of the preferred collection path:

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
```

Watch it with:

```bash
oc logs -f job/loadgen-raw-kafka -n "$NAMESPACE"
```

## Smoke Test Topic Processing

Before running the full collection loop, verify that Phase 1 can read raw Kafka events and write the validated plus typed topics:

```bash
scripts/smoke-phase1-topics.sh
```

The smoke test creates a short-lived in-cluster producer, runs a temporary Phase 1 `PipelineRun`, then starts an in-cluster verifier. It passes only if records with the same `smoke_run_id` are found in:

- `ds551-s26.team02.validated`
- `ds551-s26.team02.symptom_reports`
- `ds551-s26.team02.clinic_visits`
- `ds551-s26.team02.hospital_admissions`

Useful overrides:

```bash
SMOKE_KEEP_RESOURCES=1 scripts/smoke-phase1-topics.sh
SMOKE_EVENT_COUNT=600 SMOKE_BATCH_SIZE=60 scripts/smoke-phase1-topics.sh
```

## Smoke Test Spark Database Writes

After Phase 1 topic processing passes, verify Phase 2 by producing known records directly into the typed topics, running Spark for an explicit window, and checking the feature tables:

```bash
scripts/smoke-phase2-db.sh
```

The Phase 2 smoke test passes only if Spark writes the unique smoke batch into:

- `batch_window_runs`
- `feature_region_hourly`
- `feature_clinic_hourly`
- `feature_hospital_hourly`
- `feature_diagnosis_hourly`
- `feature_symptom_hourly`

Useful overrides:

```bash
SMOKE_KEEP_RESOURCES=1 scripts/smoke-phase2-db.sh
SPARK_WAIT_TIMEOUT=1200s scripts/smoke-phase2-db.sh
```

## Batch Size

Use a larger Phase 1 batch size rather than a very short sleep. A short sleep creates many Tekton runs and more cluster overhead; a larger batch lets each run drain more Kafka messages.

The repo includes `tekton/pipelinerun-continuous.yaml` for this mode. It keeps the same topics and consumer groups as `tekton/pipelinerun.yaml`, but raises `batch-size` from `100` to `5000`.

Start with:

- `batch-size`: `5000`
- loop sleep: `300` seconds
- collection duration: `10800` seconds, or 3 hours

If Phase 1 consistently finishes quickly and Kafka lag grows, increase `batch-size` to `10000` or reduce sleep to `120` seconds. If pods hit quota or time out, reduce `batch-size`.

## Terminal 1: Run Phase 1 For 3 Hours

Run Phase 1 repeatedly against the real raw topic:

```bash
end=$(( $(date +%s) + 10800 ))

while [ "$(date +%s)" -lt "$end" ]; do
  oc create -f tekton/pipelinerun-continuous.yaml -n "$NAMESPACE"
  sleep 300
done
```

Expected behavior:

- Raw events are consumed from `ds551-s26.team02.raw`.
- Valid records are written to `ds551-s26.team02.validated`.
- Routed records are written to `ds551-s26.team02.symptom_reports`, `ds551-s26.team02.clinic_visits`, and `ds551-s26.team02.hospital_admissions`.
- Processing happens in bursts each time a `PipelineRun` starts.

## Hourly Feature Batching

The database features are still written as hourly facts. Spark uses `WINDOW_START_UTC` and `WINDOW_END_UTC`, filters events to that window, buckets rows by hour, and appends to the `feature_*_hourly` tables.

For clean hourly analytics, prefer closed hourly windows. If the loop runs Spark more frequently than once per hour, it can create multiple batches for the same hour. That is acceptable for collection and debugging because each batch has a distinct `batch_id`, but final reporting should use the latest successful batch for each hour.

## Terminal 2: Launch Spark For Closed Hours

Spark should write closed hourly windows, not every few minutes. Run this in a second terminal to launch Spark once per newly closed hour:

```bash
export KAFKA_BOOTSTRAP=kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092
export DB_HOST=timescaledb-service
export DB_PORT=5432
export DB_NAME=epidemic
export DB_USER=spark_writer
export DB_PASSWORD=<db-password>
export KAFKA_CONSUMER_GROUPS=team02-validate-group,team02-route-group
export REQUIRE_LAG_CHECK=0

end=$(( $(date +%s) + 10800 ))
last_window=""

while [ "$(date +%s)" -lt "$end" ]; do
  window_start="$(date -u -v-1H +"%Y-%m-%dT%H:00:00Z" 2>/dev/null || date -u -d '1 hour ago' +"%Y-%m-%dT%H:00:00Z")"
  if [ "$window_start" != "$last_window" ]; then
    scripts/run-hourly-spark-external.sh
    last_window="$window_start"
  fi
  sleep 300
done
```

This runner computes the previous closed UTC hour and launches Spark with explicit `WINDOW_START_UTC` / `WINDOW_END_UTC`. `REQUIRE_LAG_CHECK=0` avoids blocking forever while real raw events are still arriving. Give Phase 1 time to drain by keeping the larger batch size.

## When To Stop

The loop stops automatically after the requested duration. After it finishes, inspect the database feature tables and Spark logs to confirm rows were written:

```bash
oc logs -f job/epidemic-spark-etl -n "$NAMESPACE"
```

Useful database checks:

```sql
SELECT batch_id, window_start, window_end, status, completed_at
FROM batch_window_runs
ORDER BY completed_at DESC
LIMIT 20;

SELECT COUNT(*) FROM feature_region_hourly;
SELECT COUNT(*) FROM feature_clinic_hourly;
SELECT COUNT(*) FROM feature_hospital_hourly;
SELECT COUNT(*) FROM feature_diagnosis_hourly;
SELECT COUNT(*) FROM feature_symptom_hourly;
```

## Summary

CronJob scheduling is optional. For immediate data collection, keep the system simple:

1. Use the real Kafka raw topic from the bootstrap server.
2. Run Phase 1 repeatedly for 3 hours with `tekton/pipelinerun-continuous.yaml`.
3. Launch Spark once per closed hour.
4. Keep Spark writing hourly feature batches to TimescaleDB.

