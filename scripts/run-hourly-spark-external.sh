#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
PHASE1_WAIT_TIMEOUT_SECS="${PHASE1_WAIT_TIMEOUT_SECS:-1800}"
GATE_POLL_SECS="${GATE_POLL_SECS:-60}"
REQUIRE_LAG_CHECK="${REQUIRE_LAG_CHECK:-1}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-}"
KAFKA_CONSUMER_GROUPS="${KAFKA_CONSUMER_GROUPS:-team02-validate-group,team02-route-group}"
MAX_ALLOWED_LAG="${MAX_ALLOWED_LAG:-0}"
SPARK_JOB_TEMPLATE_PATH="${SPARK_JOB_TEMPLATE_PATH:-infra/spark-job.yaml}"
LAG_CHECK_COMMAND="${LAG_CHECK_COMMAND:-}"

DB_HOST="${DB_HOST:-timescaledb-service}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_SSLMODE="${DB_SSLMODE:-prefer}"

need_oc() {
  command -v oc >/dev/null 2>&1 || {
    echo "oc not found in PATH" >&2
    exit 1
  }
}

need_psql() {
  command -v psql >/dev/null 2>&1 || {
    echo "psql not found in PATH (required for direct DB gate checks)" >&2
    exit 1
  }
}

need_kafka_lag_cli() {
  if command -v kafka-consumer-groups >/dev/null 2>&1; then
    echo "kafka-consumer-groups"
    return 0
  fi
  if command -v kafka-consumer-groups.sh >/dev/null 2>&1; then
    echo "kafka-consumer-groups.sh"
    return 0
  fi
  return 1
}

utc_hour_window() {
  local start end
  if date -u -d "1 hour ago" +"%Y-%m-%dT%H:00:00Z" >/dev/null 2>&1; then
    # GNU date (Linux/CI)
    start="$(date -u -d "1 hour ago" +"%Y-%m-%dT%H:00:00Z")"
    end="$(date -u +"%Y-%m-%dT%H:00:00Z")"
  else
    # BSD date (macOS)
    end="$(date -u +"%Y-%m-%dT%H:00:00Z")"
    start="$(date -u -j -v-1H -f "%Y-%m-%dT%H:%M:%SZ" "$end" "+%Y-%m-%dT%H:00:00Z")"
  fi
  echo "$start" "$end"
}

resolve_window() {
  local env_start="${WINDOW_START_UTC:-}"
  local env_end="${WINDOW_END_UTC:-}"

  if [[ -n "$env_start" || -n "$env_end" ]]; then
    if [[ -z "$env_start" || -z "$env_end" ]]; then
      echo "[window] Set both WINDOW_START_UTC and WINDOW_END_UTC, or neither." >&2
      return 1
    fi
    echo "$env_start" "$env_end"
    return 0
  fi

  utc_hour_window
}

pg_query() {
  local sql="$1"
  local db_password_for_cmd="${DB_PASSWORD}"
  if [[ -z "$db_password_for_cmd" ]] && [[ -n "${PGPASSWORD:-}" ]]; then
    db_password_for_cmd="${PGPASSWORD}"
  fi

  PGPASSWORD="$db_password_for_cmd" psql \
    "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER} sslmode=${DB_SSLMODE}" \
    -At -v ON_ERROR_STOP=1 -c "$sql"
}

window_has_running_batch() {
  local window_start="$1"
  local table_probe
  table_probe="$(pg_query "SELECT to_regclass('public.batch_window_runs');" 2>/dev/null || true)"
  if [[ "$table_probe" != "batch_window_runs" ]]; then
    echo "[gate] Missing table batch_window_runs in ${DB_NAME}." >&2
    echo "[gate] Apply infra/database-deployment.yaml against the target database first." >&2
    return 2
  fi

  local q out
  q="$(cat <<EOF
SELECT 1
FROM batch_window_runs
WHERE window_start = '${window_start}'::timestamptz
  AND status = 'running'
LIMIT 1;
EOF
)"
  out="$(pg_query "$q" 2>/dev/null || true)"
  [[ "$out" == "1" ]]
}

wait_for_phase1_ready() {
  local started_at
  started_at="$(date +%s)"
  while true; do
    local pr
    pr="$(oc get pipelinerun --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null || true)"
    if [[ -n "$pr" ]]; then
      local pr_succeeded
      pr_succeeded="$(oc get "pipelinerun/${pr}" -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].status}' 2>/dev/null || true)"
      local running_tr
      running_tr="$(oc get taskrun -l tekton.dev/pipelineRun="${pr}" -o jsonpath='{range .items[?(@.status.conditions[0].status!="True")]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
      if [[ "$pr_succeeded" == "True" ]] && [[ -z "$running_tr" ]]; then
        if ! run_lag_check; then
          echo "[gate] Lag check failed; retrying."
        else
          echo "[gate] Phase 1 ready and lag check passed."
          return 0
        fi
      fi
    fi

    local now
    now="$(date +%s)"
    if (( now - started_at > PHASE1_WAIT_TIMEOUT_SECS )); then
      echo "[gate] Timed out waiting for Phase 1 readiness."
      return 1
    fi
    sleep "$GATE_POLL_SECS"
  done
}

run_lag_check() {
  if [[ -n "$LAG_CHECK_COMMAND" ]]; then
    bash -lc "$LAG_CHECK_COMMAND"
    return $?
  fi

  if [[ "$REQUIRE_LAG_CHECK" != "1" ]]; then
    echo "[gate] Lag check disabled by REQUIRE_LAG_CHECK=${REQUIRE_LAG_CHECK}."
    return 0
  fi

  [[ -n "$KAFKA_BOOTSTRAP" ]] || {
    echo "[gate] KAFKA_BOOTSTRAP is required when REQUIRE_LAG_CHECK=1." >&2
    return 1
  }

  local lag_cli
  lag_cli="$(need_kafka_lag_cli || true)"
  if [[ -z "$lag_cli" ]]; then
    echo "[gate] kafka-consumer-groups CLI not found (required for direct lag checks)." >&2
    return 1
  fi

  local total_lag=0
  IFS=',' read -r -a groups <<< "$KAFKA_CONSUMER_GROUPS"
  local group
  for group in "${groups[@]}"; do
    group="$(echo "$group" | xargs)"
    [[ -n "$group" ]] || continue
    local group_lag
    group_lag="$(
      "$lag_cli" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --group "$group" 2>/dev/null \
      | awk '
          NF == 0 { next }
          /TOPIC/ { next }
          /Error/ { next }
          $NF ~ /^[0-9]+$/ { s += $NF }
          END { print s + 0 }
        '
    )"
    if [[ -z "$group_lag" ]]; then
      echo "[gate] Unable to compute lag for group ${group}." >&2
      return 1
    fi
    echo "[gate] Consumer group ${group} lag=${group_lag}"
    total_lag=$(( total_lag + group_lag ))
  done

  echo "[gate] Total lag=${total_lag} (threshold=${MAX_ALLOWED_LAG})"
  [[ "$total_lag" -le "$MAX_ALLOWED_LAG" ]]
}

launch_spark_for_window() {
  local window_start="$1"
  local window_end="$2"
  local batch_id="hourly-${window_start:0:13}"
  local run_id="${batch_id}-$(date -u +%Y%m%dT%H%M%SZ)"

  if [[ ! -f "${SPARK_JOB_TEMPLATE_PATH}" ]]; then
    echo "[gate] Spark job template not found at ${SPARK_JOB_TEMPLATE_PATH}" >&2
    return 1
  fi

  oc delete "job/${SPARK_JOB_NAME}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  oc create -f "${SPARK_JOB_TEMPLATE_PATH}" --dry-run=client -o yaml |
  oc set env -f - \
    --local \
    -o yaml \
    WINDOW_START_UTC="${window_start}" \
    WINDOW_END_UTC="${window_end}" \
    RUN_ID="${run_id}" \
    BATCH_ID="${batch_id}" \
    REQUIRE_WINDOW_UTC="true" \
  | oc apply -f -

  echo "[spark] launched ${SPARK_JOB_NAME} for ${window_start} to ${window_end} (batch_id=${batch_id})"
}

main() {
  need_oc
  need_psql

  local window_start window_end
  read -r window_start window_end < <(resolve_window)
  echo "[window] closed window: ${window_start} to ${window_end}"

  set +e
  window_has_running_batch "$window_start"
  local running_check_rc=$?
  set -e
  if [[ "$running_check_rc" -eq 0 ]]; then
    echo "[gate] window already has a running batch; skipping Spark launch"
    exit 0
  elif [[ "$running_check_rc" -eq 2 ]]; then
    exit 1
  fi

  wait_for_phase1_ready
  launch_spark_for_window "$window_start" "$window_end"
}

main "$@"
