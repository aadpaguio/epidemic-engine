#!/usr/bin/env bash
# Canonical cluster smoke for continuous collection: Phase 1 PipelineRuns -> bounded raw
# typed-topic ingest -> dirty-window Spark refresh (FEATURE_SOURCE_MODE=raw_db).
# See docs/continuous-collection-plan.md and docs/testing-plan.md.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
DURATION_SEC="${DURATION_SEC:-10800}"
PHASE1_TIMEOUT_SEC="${PHASE1_TIMEOUT_SEC:-1200}"
PHASE1_CANCEL_ON_TIMEOUT="${PHASE1_CANCEL_ON_TIMEOUT:-1}"
PHASE1_RUNS_PER_CYCLE="${PHASE1_RUNS_PER_CYCLE:-3}"
BATCH_SIZE="${BATCH_SIZE:-500}"
CONSUMER_TIMEOUT_MS="${CONSUMER_TIMEOUT_MS:-120000}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092}"
RAW_TOPIC="${RAW_TOPIC:-ds551-s26.team02.raw}"
VALIDATED_TOPIC="${VALIDATED_TOPIC:-ds551-s26.team02.validated}"
VALIDATE_DLQ_TOPIC="${VALIDATE_DLQ_TOPIC:-ds551-s26.team02.validate_dlq}"
SYMPTOM_TOPIC="${SYMPTOM_TOPIC:-ds551-s26.team02.symptom_reports}"
CLINIC_TOPIC="${CLINIC_TOPIC:-ds551-s26.team02.clinic_visits}"
HOSPITAL_TOPIC="${HOSPITAL_TOPIC:-ds551-s26.team02.hospital_admissions}"
VACCINATION_TOPIC="${VACCINATION_TOPIC:-ds551-s26.team02.vaccination_records}"
ROUTING_DLQ_TOPIC="${ROUTING_DLQ_TOPIC:-ds551-s26.team02.routing_dlq}"
TEAM_ID="${TEAM_ID:-team02}"
VALIDATE_GROUP="${VALIDATE_GROUP:-team02-validate-group}"
ROUTE_GROUP="${ROUTE_GROUP:-team02-route-group}"
ROUTE_AUTO_OFFSET_RESET="${ROUTE_AUTO_OFFSET_RESET:-earliest}"
INGEST_RUN_SECONDS="${INGEST_RUN_SECONDS:-60}"
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
  oc wait -n "$NAMESPACE" --for=condition=Succeeded "$pr" --timeout="${PHASE1_TIMEOUT_SEC}s" || wait_rc=$?
  oc get "$pr" -n "$NAMESPACE" || true
  if [ "$wait_rc" -ne 0 ] && [ "$PHASE1_CANCEL_ON_TIMEOUT" = "1" ]; then
    log "[phase1] ${pr} did not finish within ${PHASE1_TIMEOUT_SEC}s; cancelling before continuing"
    oc patch "$pr" -n "$NAMESPACE" --type merge -p '{"spec":{"status":"Cancelled"}}' >/dev/null || true
    oc wait -n "$NAMESPACE" --for=condition=Succeeded "$pr" --timeout=60s >/dev/null 2>&1 || true
    oc get "$pr" -n "$NAMESPACE" || true
  fi
  return "$wait_rc"
}

main() {
  setup_logging
  trap on_interrupt INT TERM
  oc project "$NAMESPACE" >/dev/null

  local end_epoch
  end_epoch=$(( $(date +%s) + DURATION_SEC ))
  log "[seq] namespace=${NAMESPACE} duration_sec=${DURATION_SEC} phase1_runs_per_cycle=${PHASE1_RUNS_PER_CYCLE} batch_size=${BATCH_SIZE} phase1_timeout_sec=${PHASE1_TIMEOUT_SEC}"

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
