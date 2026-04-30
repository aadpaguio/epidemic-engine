#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
DURATION_SEC="${DURATION_SEC:-10800}"
SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
PHASE1_TIMEOUT_SEC="${PHASE1_TIMEOUT_SEC:-3600}"
SPARK_TIMEOUT_SEC="${SPARK_TIMEOUT_SEC:-1800}"
SPARK_WINDOW_MODE="${SPARK_WINDOW_MODE:-eventtime}"
SPARK_EVERY_PHASE1_RUNS="${SPARK_EVERY_PHASE1_RUNS:-1}"
SPARK_BATCH_PREFIX="${SPARK_BATCH_PREFIX:-eventtime}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092}"
KAFKA_INTERNAL_BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}"
RAW_TOPIC="${RAW_TOPIC:-ds551-s26.team02.raw}"
VALIDATED_TOPIC="${VALIDATED_TOPIC:-ds551-s26.team02.validated}"
VALIDATE_DLQ_TOPIC="${VALIDATE_DLQ_TOPIC:-ds551-s26.team02.validate_dlq}"
SYMPTOM_TOPIC="${SYMPTOM_TOPIC:-ds551-s26.team02.symptom_reports}"
CLINIC_TOPIC="${CLINIC_TOPIC:-ds551-s26.team02.clinic_visits}"
HOSPITAL_TOPIC="${HOSPITAL_TOPIC:-ds551-s26.team02.hospital_admissions}"
VACCINATION_TOPIC="${VACCINATION_TOPIC:-ds551-s26.team02.vaccination_records}"
ROUTING_DLQ_TOPIC="${ROUTING_DLQ_TOPIC:-ds551-s26.team02.routing_dlq}"
TEAM_ID="${TEAM_ID:-team02}"
BATCH_SIZE="${BATCH_SIZE:-250}"
CONSUMER_TIMEOUT_MS="${CONSUMER_TIMEOUT_MS:-60000}"
ROUTE_AUTO_OFFSET_RESET="${ROUTE_AUTO_OFFSET_RESET:-earliest}"
FRESH_CONSUMER_GROUPS="${FRESH_CONSUMER_GROUPS:-0}"
RUN_TAG="${RUN_TAG:-}"
VALIDATE_GROUP="${VALIDATE_GROUP:-team02-validate-group}"
ROUTE_GROUP="${ROUTE_GROUP:-team02-route-group}"
EVENTTIME_KAFKA_POD="${EVENTTIME_KAFKA_POD:-}"
EVENTTIME_SAMPLE_MESSAGES="${EVENTTIME_SAMPLE_MESSAGES:-50}"
EVENTTIME_SAMPLE_TIMEOUT_MS="${EVENTTIME_SAMPLE_TIMEOUT_MS:-2000}"
EVENTTIME_LOOKBACK_HOURS="${EVENTTIME_LOOKBACK_HOURS:-2}"
EVENTTIME_MAX_WINDOWS_PER_CHECK="${EVENTTIME_MAX_WINDOWS_PER_CHECK:-4}"
EVENTTIME_CURSOR_START_UTC="${EVENTTIME_CURSOR_START_UTC:-}"
EVENTTIME_CURSOR_MODE="${EVENTTIME_CURSOR_MODE:-db}"
DB_POD="${DB_POD:-timescaledb-0}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"
LOG_DIR="${LOG_DIR:-logs/runtime}"
LOG_FILE="${LOG_FILE:-}"
STOP_REQUESTED=0
CLEANUP_DONE=0

setup_logging() {
  mkdir -p "$LOG_DIR"
  if [ -z "$LOG_FILE" ]; then
    LOG_FILE="${LOG_DIR}/orchestrator-$(date -u +%Y%m%dT%H%M%SZ).log"
  fi
  exec > >(tee -a "$LOG_FILE") 2>&1
}

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

need_oc() {
  command -v oc >/dev/null 2>&1 || {
    echo "oc not found in PATH" >&2
    exit 1
  }
}

on_interrupt() {
  STOP_REQUESTED=1
  log "[orchestrator] stop requested; cancelling active runs"
  cleanup_running_work
}

utc_prev_hour_start() {
  date -u -v-1H +"%Y-%m-%dT%H:00:00Z" 2>/dev/null || date -u -d "1 hour ago" +"%Y-%m-%dT%H:00:00Z"
}

utc_hour_start() {
  date -u +"%Y-%m-%dT%H:00:00Z"
}

typed_topics() {
  printf '%s\n' "$SYMPTOM_TOPIC" "$CLINIC_TOPIC" "$HOSPITAL_TOPIC" "$VACCINATION_TOPIC"
}

initialize_consumer_groups() {
  if [ "$FRESH_CONSUMER_GROUPS" = "1" ]; then
    if [ -z "$RUN_TAG" ]; then
      RUN_TAG="fresh-$(date -u +%Y%m%dT%H%M%SZ)"
    fi
    VALIDATE_GROUP="team02-validate-${RUN_TAG}"
    ROUTE_GROUP="team02-route-${RUN_TAG}"
  fi
}

cancel_running_phase1() {
  oc get pipelinerun -n "$NAMESPACE" --no-headers \
    | awk '$1 ~ /^phase1-continuous-run-/ && $3=="Running"{print $1}' \
    | while read -r pr; do
        oc patch pipelinerun "$pr" -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}' >/dev/null
      done
}

cleanup_running_work() {
  if [ "$CLEANUP_DONE" -eq 1 ]; then
    return 0
  fi
  CLEANUP_DONE=1
  cancel_running_phase1 || true
  oc delete "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
}

find_kafka_pod() {
  if [ -n "$EVENTTIME_KAFKA_POD" ]; then
    echo "$EVENTTIME_KAFKA_POD"
    return 0
  fi
  oc get pods -n "$NAMESPACE" --no-headers \
    | awk '$1 ~ /kafka/ && $3 == "Running" {print $1; exit}'
}

sample_recent_topic_messages() {
  local kafka_pod="$1"
  local topic="$2"

  oc exec -n "$NAMESPACE" "$kafka_pod" -- /bin/bash -lc '
    set -euo pipefail
    topic="$1"
    sample_messages="$2"
    bootstrap="$3"
    timeout_ms="$4"

    offsets="$(
      if command -v kafka-get-offsets >/dev/null 2>&1; then
        kafka-get-offsets --bootstrap-server "$bootstrap" --topic "$topic" --time -1
      else
        kafka-run-class kafka.tools.GetOffsetShell --broker-list "$bootstrap" --topic "$topic" --time -1
      fi
    )"

    echo "$offsets" | while IFS=: read -r topic_name partition latest_offset; do
      [ -n "${latest_offset:-}" ] || continue
      [ "$latest_offset" -gt 0 ] || continue
      start_offset=$(( latest_offset > sample_messages ? latest_offset - sample_messages : 0 ))
      max_messages=$(( latest_offset - start_offset ))
      [ "$max_messages" -gt 0 ] || continue
      kafka-console-consumer \
        --bootstrap-server "$bootstrap" \
        --topic "$topic_name" \
        --partition "$partition" \
        --offset "$start_offset" \
        --max-messages "$max_messages" \
        --timeout-ms "$timeout_ms" 2>/dev/null || true
    done
  ' _ "$topic" "$EVENTTIME_SAMPLE_MESSAGES" "$KAFKA_INTERNAL_BOOTSTRAP" "$EVENTTIME_SAMPLE_TIMEOUT_MS"
}

resolve_eventtime_windows() {
  local kafka_pod
  kafka_pod="$(find_kafka_pod)"
  if [ -z "$kafka_pod" ]; then
    log "[spark] could not find a running Kafka pod for event-time sampling"
    return 1
  fi

  log "[spark] sampling latest typed-topic event timestamps from ${kafka_pod}" >&2
  while read -r topic; do
    sample_recent_topic_messages "$kafka_pod" "$topic" || true
  done < <(typed_topics) | python -c '
import json
import sys
from datetime import datetime, timezone

latest = None

def parse_ts(value):
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

for line in sys.stdin:
    try:
        obj = json.loads(line)
    except Exception:
        continue
    dt = parse_ts(obj.get("timestamp"))
    if dt is None:
        dt = parse_ts(obj.get("administered_at"))
    if dt is not None and (latest is None or dt > latest):
        latest = dt

if latest is None:
    raise SystemExit(1)

latest_start = latest.replace(minute=0, second=0, microsecond=0)
print(latest_start.isoformat().replace("+00:00", "Z"))
'
}

plan_eventtime_windows() {
  local latest_start="$1"
  local cursor_start="$2"

  EVENTTIME_LOOKBACK_HOURS="$EVENTTIME_LOOKBACK_HOURS" \
  EVENTTIME_MAX_WINDOWS_PER_CHECK="$EVENTTIME_MAX_WINDOWS_PER_CHECK" \
  EVENTTIME_CURSOR_START_UTC="$cursor_start" \
  LATEST_EVENTTIME_START_UTC="$latest_start" \
  python -c '
import os
from datetime import datetime, timedelta, timezone

def parse_utc(value):
    value = (value or "").strip()
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")

latest = parse_utc(os.environ["LATEST_EVENTTIME_START_UTC"])
cursor = parse_utc(os.environ.get("EVENTTIME_CURSOR_START_UTC", ""))
lookback_hours = max(1, int(os.environ.get("EVENTTIME_LOOKBACK_HOURS", "2")))
max_windows = max(1, int(os.environ.get("EVENTTIME_MAX_WINDOWS_PER_CHECK", "4")))

if cursor is None:
    cursor = latest - timedelta(hours=lookback_hours - 1)

count = 0
while cursor <= latest and count < max_windows:
    end = cursor + timedelta(hours=1)
    print(iso(cursor), iso(end))
    cursor = end
    count += 1
'
}

plan_eventtime_windows_from_db() {
  local latest_start="$1"
  local cursor_start="$2"

  oc exec -i "$DB_POD" -n "$NAMESPACE" -- psql -U "$DB_USER" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 \
    -v latest_start="$latest_start" \
    -v cursor_start="$cursor_start" \
    -v lookback_hours="$EVENTTIME_LOOKBACK_HOURS" \
    -v max_windows="$EVENTTIME_MAX_WINDOWS_PER_CHECK" <<'SQL'
WITH params AS (
  SELECT
    :'latest_start'::timestamptz AS latest_start,
    NULLIF(:'cursor_start', '')::timestamptz AS cursor_start,
    GREATEST(:'lookback_hours'::int, 1) AS lookback_hours,
    GREATEST(:'max_windows'::int, 1) AS max_windows
),
bounds AS (
  SELECT
    COALESCE(
      cursor_start,
      (
        SELECT MIN(window_start)
        FROM batch_window_runs
        WHERE window_start >= latest_start - ((lookback_hours - 1) * interval '1 hour')
          AND window_start <= latest_start
      ),
      latest_start - ((lookback_hours - 1) * interval '1 hour')
    ) AS first_start,
    latest_start,
    max_windows
  FROM params
),
candidate_windows AS (
  SELECT generate_series(first_start, latest_start, interval '1 hour') AS window_start, max_windows
  FROM bounds
),
latest_success AS (
  SELECT DISTINCT window_start
  FROM batch_window_runs
  WHERE status = 'succeeded'
),
missing AS (
  SELECT c.window_start
  FROM candidate_windows c
  LEFT JOIN latest_success s USING (window_start)
  WHERE s.window_start IS NULL
  ORDER BY c.window_start
  LIMIT (SELECT max_windows FROM bounds)
)
SELECT
  to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ' ' ||
  to_char((window_start + interval '1 hour') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM missing;
SQL
}

summarize_phase1() {
  local pr_ref="$1"
  local pr_name="${pr_ref##*/}"

  log "[phase1] status for ${pr_name}"
  oc get "pipelinerun/${pr_name}" -n "$NAMESPACE" || true
  oc get taskrun -n "$NAMESPACE" | awk -v pr="$pr_name" '$1 ~ pr {print}' || true

  log "[phase1] summary logs for ${pr_name}"
  oc logs -n "$NAMESPACE" -l "tekton.dev/pipelineRun=${pr_name}" \
    --all-containers=true --prefix=true --max-log-requests=10 2>&1 \
    | awk '/SUMMARY|\[PIPELINE\].*"status"|Traceback|ERROR|Error|ExceededResourceQuota/ {print}' || true
}

summarize_spark() {
  log "[spark] job status for ${SPARK_JOB_NAME}"
  oc get "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" -o wide || true
  oc get pods -n "$NAMESPACE" -l "job-name=${SPARK_JOB_NAME}" -o wide || true

  log "[spark] analytics logs for ${SPARK_JOB_NAME}"
  oc logs "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" --tail=250 2>&1 \
    | awk '/\[ANALYTICS\]|Traceback|ERROR|Error|Exception|FailedCreate|ExceededResourceQuota/ {print}' || true
}

launch_spark_for_window() {
  local window_start="$1"
  local window_end="$2"
  local run_id batch_id

  if [ "$STOP_REQUESTED" -eq 1 ]; then
    return 130
  fi

  run_id="hourly-$(date -u +%Y%m%dT%H%M%SZ)"
  batch_id="${SPARK_BATCH_PREFIX}-${window_start:0:13}-$(date -u +%Y%m%dT%H%M%SZ)"

  oc delete "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  oc create -f infra/spark-job.yaml --dry-run=client -o yaml \
    | oc set env -f - --local -o yaml \
        WINDOW_START_UTC="$window_start" \
        WINDOW_END_UTC="$window_end" \
        REQUIRE_WINDOW_UTC=true \
        RUN_ID="$run_id" \
        BATCH_ID="$batch_id" \
    | oc apply -n "$NAMESPACE" -f - >/dev/null

  log "[spark] launched for ${window_start} -> ${window_end} (batch_id=${batch_id})"
  local wait_rc=0
  log "[spark] waiting for job/${SPARK_JOB_NAME}"
  oc wait -n "$NAMESPACE" --for=condition=complete "job/${SPARK_JOB_NAME}" --timeout="${SPARK_TIMEOUT_SEC}s" || wait_rc=$?
  summarize_spark
  if [ "$wait_rc" -eq 130 ] || [ "$STOP_REQUESTED" -eq 1 ]; then
    return 130
  fi
  return "$wait_rc"
}

run_one_phase1() {
  local pr
  if [ "$STOP_REQUESTED" -eq 1 ]; then
    return 130
  fi
  pr="$(oc create -n "$NAMESPACE" -f - -o name <<YAML
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: phase1-continuous-run-
spec:
  pipelineRef:
    name: phase1-pipeline
  params:
    - name: kafka-bootstrap
      value: ${KAFKA_BOOTSTRAP}
    - name: input-topic
      value: ${RAW_TOPIC}
    - name: validated-topic
      value: ${VALIDATED_TOPIC}
    - name: validate-dlq-topic
      value: ${VALIDATE_DLQ_TOPIC}
    - name: symptom-topic
      value: ${SYMPTOM_TOPIC}
    - name: clinic-topic
      value: ${CLINIC_TOPIC}
    - name: hospital-topic
      value: ${HOSPITAL_TOPIC}
    - name: vaccination-topic
      value: ${VACCINATION_TOPIC}
    - name: routing-dlq-topic
      value: ${ROUTING_DLQ_TOPIC}
    - name: team-id
      value: ${TEAM_ID}
    - name: batch-size
      value: "${BATCH_SIZE}"
    - name: consumer-timeout-ms
      value: "${CONSUMER_TIMEOUT_MS}"
    - name: validate-consumer-group-id
      value: ${VALIDATE_GROUP}
    - name: route-consumer-group-id
      value: ${ROUTE_GROUP}
    - name: route-auto-offset-reset
      value: ${ROUTE_AUTO_OFFSET_RESET}
YAML
)"
  log "[phase1] started ${pr}"
  local wait_rc=0
  log "[phase1] waiting for ${pr}"
  oc wait -n "$NAMESPACE" --for=condition=Succeeded "$pr" --timeout="${PHASE1_TIMEOUT_SEC}s" || wait_rc=$?
  summarize_phase1 "$pr"
  if [ "$wait_rc" -eq 130 ] || [ "$STOP_REQUESTED" -eq 1 ]; then
    return 130
  fi
  return 0
}

main() {
  setup_logging
  log "[orchestrator] logging to ${LOG_FILE}"
  need_oc
  trap on_interrupt INT TERM
  oc project "$NAMESPACE" >/dev/null
  initialize_consumer_groups

  local end_epoch current_window_start current_window_end last_spark_window
  local latest_eventtime_start resolved_windows eventtime_cursor_start
  local phase1_runs_since_spark=0
  end_epoch=$(( $(date +%s) + DURATION_SEC ))
  last_spark_window="$(utc_prev_hour_start)"
  eventtime_cursor_start="$EVENTTIME_CURSOR_START_UTC"
  log "[orchestrator] namespace=${NAMESPACE} duration_sec=${DURATION_SEC}"
  log "[orchestrator] initial spark baseline window=${last_spark_window}"
  log "[orchestrator] validate_group=${VALIDATE_GROUP}"
  log "[orchestrator] route_group=${ROUTE_GROUP}"
  log "[orchestrator] spark_window_mode=${SPARK_WINDOW_MODE}"
  log "[orchestrator] spark_every_phase1_runs=${SPARK_EVERY_PHASE1_RUNS}"
  log "[orchestrator] eventtime_lookback_hours=${EVENTTIME_LOOKBACK_HOURS}"
  log "[orchestrator] eventtime_max_windows_per_check=${EVENTTIME_MAX_WINDOWS_PER_CHECK}"
  log "[orchestrator] eventtime_cursor_mode=${EVENTTIME_CURSOR_MODE}"
  if [ -n "$eventtime_cursor_start" ]; then
    log "[orchestrator] eventtime_cursor_start=${eventtime_cursor_start}"
  fi

  while [ "$(date +%s)" -lt "$end_epoch" ]; do
    if [ "$STOP_REQUESTED" -eq 1 ]; then
      break
    fi

    if [ "$SPARK_WINDOW_MODE" = "eventtime" ]; then
      if [ "$phase1_runs_since_spark" -ge "$SPARK_EVERY_PHASE1_RUNS" ]; then
        latest_eventtime_start="$(resolve_eventtime_windows || true)"
        if [ -n "$latest_eventtime_start" ]; then
          log "[spark] latest sampled event-time hour ${latest_eventtime_start}"
          if [ "$EVENTTIME_CURSOR_MODE" = "db" ]; then
            resolved_windows="$(plan_eventtime_windows_from_db "$latest_eventtime_start" "$eventtime_cursor_start" || true)"
            if [ -z "$resolved_windows" ]; then
              log "[spark] DB cursor found no missing event-time windows through ${latest_eventtime_start}"
            fi
          else
            resolved_windows="$(plan_eventtime_windows "$latest_eventtime_start" "$eventtime_cursor_start")"
          fi
        else
          resolved_windows=""
        fi
        if [ -n "$resolved_windows" ]; then
          while read -r current_window_start current_window_end; do
            [ -n "${current_window_start:-}" ] || continue
            if [ "$STOP_REQUESTED" -eq 1 ]; then
              break
            fi
            log "[spark] selected event-time window ${current_window_start} -> ${current_window_end}"
            if launch_spark_for_window "$current_window_start" "$current_window_end"; then
              eventtime_cursor_start="$current_window_end"
              last_spark_window="$current_window_start"
            fi
          done <<< "$resolved_windows"
          phase1_runs_since_spark=0
        else
          log "[spark] no typed-topic event timestamps found; continuing Phase 1"
          if run_one_phase1; then
            phase1_runs_since_spark=$(( phase1_runs_since_spark + 1 ))
          fi
        fi
      else
        if run_one_phase1; then
          phase1_runs_since_spark=$(( phase1_runs_since_spark + 1 ))
          log "[orchestrator] phase1_runs_since_spark=${phase1_runs_since_spark}/${SPARK_EVERY_PHASE1_RUNS}"
        fi
      fi
    else
      current_window_end="$(utc_hour_start)"
      current_window_start="$(utc_prev_hour_start)"

      if [ "$current_window_start" != "$last_spark_window" ]; then
        cancel_running_phase1
        if launch_spark_for_window "$current_window_start" "$current_window_end"; then
          last_spark_window="$current_window_start"
        fi
      else
        run_one_phase1 || true
      fi
    fi
  done

  if [ "$STOP_REQUESTED" -eq 1 ]; then
    cleanup_running_work
    log "[orchestrator] stopped by user"
  else
    log "[orchestrator] completed"
  fi
}

main "$@"
