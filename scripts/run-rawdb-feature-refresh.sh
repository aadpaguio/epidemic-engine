#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
ACTION="${1:-loop}"

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
SPARK_TIMEOUT_SEC="${SPARK_TIMEOUT_SEC:-1800}"
DB_POD="${DB_POD:-timescaledb-0}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"
MAX_DIRTY_WINDOWS_PER_RUN="${MAX_DIRTY_WINDOWS_PER_RUN:-6}"
REFRESH_SLEEP_SEC="${REFRESH_SLEEP_SEC:-60}"
FEATURE_SOURCE_MODE="${FEATURE_SOURCE_MODE:-raw_db}"
SPARK_LOG_TAIL="${SPARK_LOG_TAIL:-600}"
LOG_DIR="${LOG_DIR:-logs/runtime}"
LOG_FILE="${LOG_FILE:-}"
STOP_REQUESTED=0

setup_logging() {
  mkdir -p "$LOG_DIR"
  if [ -z "$LOG_FILE" ]; then
    LOG_FILE="${LOG_DIR}/rawdb-feature-refresh-$(date -u +%Y%m%dT%H%M%SZ).log"
  fi
  exec > >(tee -a "$LOG_FILE") 2>&1
}

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

on_interrupt() {
  STOP_REQUESTED=1
  log "[refresh] stop requested"
}

find_dirty_windows() {
  oc exec -i "$DB_POD" -n "$NAMESPACE" -- psql -U "$DB_USER" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 \
    -v max_windows="$MAX_DIRTY_WINDOWS_PER_RUN" <<'SQL'
WITH latest_success AS (
  SELECT window_start, MAX(completed_at) AS completed_at
  FROM batch_window_runs
  WHERE status = 'succeeded'
  GROUP BY window_start
),
raw_hours AS (
  SELECT
    date_trunc('hour', event_ts) AS window_start,
    MAX(ingested_at) AS last_ingested_at
  FROM raw_typed_events
  WHERE event_ts IS NOT NULL
  GROUP BY 1
),
dirty AS (
  SELECT r.window_start
  FROM raw_hours r
  LEFT JOIN latest_success s USING (window_start)
  WHERE s.completed_at IS NULL OR r.last_ingested_at > s.completed_at
  ORDER BY r.window_start
  LIMIT GREATEST(:'max_windows'::int, 1)
)
SELECT to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM dirty;
SQL
}

collapse_window_range() {
  python3 -c '
import sys
from datetime import datetime, timedelta, timezone

rows = [line.strip() for line in sys.stdin if line.strip()]
if not rows:
    raise SystemExit(1)

starts = []
for row in rows:
    norm = row[:-1] + "+00:00" if row.endswith("Z") else row
    dt = datetime.fromisoformat(norm).astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    starts.append(dt)

range_start = min(starts)
range_end = max(starts) + timedelta(hours=1)
print(range_start.isoformat().replace("+00:00", "Z"))
print(range_end.isoformat().replace("+00:00", "Z"))
'
}

launch_spark_range() {
  local window_start="$1"
  local window_end="$2"
  local run_id batch_id

  run_id="rawdb-refresh-$(date -u +%Y%m%dT%H%M%SZ)"
  batch_id="rawdb-range-${window_start:0:13}-$(date -u +%Y%m%dT%H%M%SZ)"

  oc delete "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  oc create -f infra/spark-job.yaml --dry-run=client -o yaml \
    | oc set env -f - --local -o yaml \
        WINDOW_START_UTC="$window_start" \
        WINDOW_END_UTC="$window_end" \
        REQUIRE_WINDOW_UTC=true \
        FEATURE_SOURCE_MODE="$FEATURE_SOURCE_MODE" \
        RUN_ID="$run_id" \
        BATCH_ID="$batch_id" \
    | oc apply -n "$NAMESPACE" -f - >/dev/null

  log "[spark] launched refresh for ${window_start} -> ${window_end} (batch_id=${batch_id})"
  local wait_rc=0
  oc wait -n "$NAMESPACE" --for=condition=complete "job/${SPARK_JOB_NAME}" --timeout="${SPARK_TIMEOUT_SEC}s" || wait_rc=$?
  log "[spark] full driver logs job/${SPARK_JOB_NAME} (tail=${SPARK_LOG_TAIL})"
  oc logs "job/${SPARK_JOB_NAME}" -n "$NAMESPACE" --tail="${SPARK_LOG_TAIL}" 2>&1 || true
  return "$wait_rc"
}

main() {
  setup_logging
  trap on_interrupt INT TERM
  oc project "$NAMESPACE" >/dev/null
  log "[refresh] namespace=${NAMESPACE} mode=${FEATURE_SOURCE_MODE} max_windows=${MAX_DIRTY_WINDOWS_PER_RUN}"

  while true; do
    if [ "$STOP_REQUESTED" -eq 1 ]; then
      break
    fi

    dirty_windows="$(find_dirty_windows || true)"
    if [ -z "${dirty_windows:-}" ]; then
      log "[refresh] no dirty windows found; sleeping ${REFRESH_SLEEP_SEC}s"
      sleep "$REFRESH_SLEEP_SEC"
      continue
    fi

    range_bounds="$(printf '%s\n' "$dirty_windows" | collapse_window_range || true)"
    window_start="$(printf '%s\n' "$range_bounds" | sed -n '1p')"
    window_end="$(printf '%s\n' "$range_bounds" | sed -n '2p')"
    if [ -z "$window_start" ] || [ -z "$window_end" ]; then
      log "[refresh] failed to resolve dirty window range; sleeping ${REFRESH_SLEEP_SEC}s"
      sleep "$REFRESH_SLEEP_SEC"
      continue
    fi
    log "[refresh] dirty windows selected:"
    while read -r w; do
      [ -n "$w" ] && log "[refresh]   $w"
    done <<< "$dirty_windows"

    if ! launch_spark_range "$window_start" "$window_end"; then
      log "[refresh] spark refresh failed; retrying after ${REFRESH_SLEEP_SEC}s"
      sleep "$REFRESH_SLEEP_SEC"
      continue
    fi

    if [ "$ACTION" = "once" ]; then
      break
    fi
  done

  log "[refresh] stopped"
}

main "$@"
