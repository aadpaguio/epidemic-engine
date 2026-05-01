#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
DURATION_SEC="${DURATION_SEC:-10800}"
PHASE1_TIMEOUT_SEC="${PHASE1_TIMEOUT_SEC:-3600}"
PHASE1_RUNS_PER_CYCLE="${PHASE1_RUNS_PER_CYCLE:-3}"
INGEST_RUN_SECONDS="${INGEST_RUN_SECONDS:-30}"
INGEST_MAX_MESSAGES="${INGEST_MAX_MESSAGES:-0}"
MAX_DIRTY_WINDOWS_PER_RUN="${MAX_DIRTY_WINDOWS_PER_RUN:-6}"
REFRESH_SLEEP_SEC="${REFRESH_SLEEP_SEC:-30}"
LOG_DIR="${LOG_DIR:-logs/runtime}"
LOG_FILE="${LOG_FILE:-}"
STOP_REQUESTED=0

setup_logging() {
  mkdir -p "$LOG_DIR"
  if [ -z "$LOG_FILE" ]; then
    LOG_FILE="${LOG_DIR}/phase1-rawdb-sequenced-$(date -u +%Y%m%dT%H%M%SZ).log"
  fi
  exec > >(tee -a "$LOG_FILE") 2>&1
}

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

on_interrupt() {
  STOP_REQUESTED=1
}

run_phase1_once() {
  local pr wait_rc=0
  pr="$(oc create -n "$NAMESPACE" -f tekton/pipelinerun-continuous.yaml -o name)"
  log "[phase1] started ${pr}"
  oc wait -n "$NAMESPACE" --for=condition=Succeeded "$pr" --timeout="${PHASE1_TIMEOUT_SEC}s" || wait_rc=$?
  oc get "$pr" -n "$NAMESPACE" || true
  return "$wait_rc"
}

main() {
  setup_logging
  trap on_interrupt INT TERM
  oc project "$NAMESPACE" >/dev/null

  local end_epoch
  end_epoch=$(( $(date +%s) + DURATION_SEC ))
  log "[seq] namespace=${NAMESPACE} duration_sec=${DURATION_SEC} phase1_runs_per_cycle=${PHASE1_RUNS_PER_CYCLE}"

  while [ "$(date +%s)" -lt "$end_epoch" ]; do
    if [ "$STOP_REQUESTED" -eq 1 ]; then
      break
    fi

    for i in $(seq 1 "$PHASE1_RUNS_PER_CYCLE"); do
      if [ "$STOP_REQUESTED" -eq 1 ] || [ "$(date +%s)" -ge "$end_epoch" ]; then
        break
      fi
      log "[seq] phase1 cycle run ${i}/${PHASE1_RUNS_PER_CYCLE}"
      if ! run_phase1_once; then
        log "[seq] phase1 run ${i}/${PHASE1_RUNS_PER_CYCLE} failed; continuing to ingest/refresh"
        break
      fi
    done

    log "[seq] running raw ingest catch-up (RUN_SECONDS=${INGEST_RUN_SECONDS}, MAX_MESSAGES=${INGEST_MAX_MESSAGES})"
    RUN_SECONDS="$INGEST_RUN_SECONDS" MAX_MESSAGES="$INGEST_MAX_MESSAGES" \
      bash scripts/run-raw-typed-ingester.sh once || true

    log "[seq] running one rawdb feature refresh cycle"
    MAX_DIRTY_WINDOWS_PER_RUN="$MAX_DIRTY_WINDOWS_PER_RUN" REFRESH_SLEEP_SEC="$REFRESH_SLEEP_SEC" \
      bash scripts/run-rawdb-feature-refresh.sh once || true
  done

  log "[seq] completed"
}

main "$@"
