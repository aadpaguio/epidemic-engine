#!/usr/bin/env bash
# Run Phase 1 (Tekton) then Phase 2 (Spark ETL Job) against the current oc project.
# Assumes reruns need cleanup: removes the previous Spark Job, optional Tekton pod/DB cleanup.
#
# Usage (from repo root, or any cwd if oc project is set):
#   ./scripts/run-phase1-and-phase2.sh
#   ./scripts/run-phase1-and-phase2.sh --truncate-db
#   ./scripts/run-phase1-and-phase2.sh --skip-phase1
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
PIPELINERUN_MANIFEST="${PIPELINERUN_MANIFEST:-tekton/pipelinerun.yaml}"
DB_POD="${DB_POD:-timescaledb-0}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"
PHASE1_WAIT_TIMEOUT="${PHASE1_WAIT_TIMEOUT:-600s}"
SPARK_WAIT_TIMEOUT="${SPARK_WAIT_TIMEOUT:-900s}"

DO_CLEANUP=1
DO_PHASE1=1
DO_PHASE2=1
TRUNCATE_DB=0
DELETE_SUCCEEDED_PHASE1_PODS=1

usage() {
  sed -n '1,40p' "$0" | tail -n +2
  cat <<'EOF'

Options:
  --no-cleanup                 Skip all cleanup steps (still creates a new PipelineRun).
  --no-delete-phase1-pods      Do not delete Succeeded phase1 Task pods (may affect quota).
  --truncate-db                TRUNCATE the five feature_*_hourly tables before Spark (destructive).
  --skip-phase1                Only run Phase 2 (Spark); assumes topics already populated.
  --skip-phase2                Only run Phase 1.
  -h, --help                   Show this help.

Environment:
  SPARK_JOB_NAME, PIPELINERUN_MANIFEST, DB_POD, DB_NAME, DB_USER,
  PHASE1_WAIT_TIMEOUT, SPARK_WAIT_TIMEOUT
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-cleanup) DO_CLEANUP=0; DELETE_SUCCEEDED_PHASE1_PODS=0 ;;
    --no-delete-phase1-pods) DELETE_SUCCEEDED_PHASE1_PODS=0 ;;
    --truncate-db) TRUNCATE_DB=1 ;;
    --skip-phase1) DO_PHASE1=0 ;;
    --skip-phase2) DO_PHASE2=0 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

need_oc() {
  command -v oc >/dev/null 2>&1 || {
    echo "oc not found in PATH" >&2
    exit 1
  }
}

current_ns() {
  oc config view --minify -o jsonpath='{..namespace}' 2>/dev/null || true
}

cleanup_spark_job() {
  echo "[cleanup] Deleting Job/${SPARK_JOB_NAME} (if present)..."
  oc delete "job/${SPARK_JOB_NAME}" --ignore-not-found --wait=false
}

cleanup_phase1_succeeded_pods() {
  [[ "$DELETE_SUCCEEDED_PHASE1_PODS" -eq 1 ]] || return 0
  echo "[cleanup] Deleting Succeeded phase1 Task pods (phase1-run-*-validate-pod / *-route-and-enrich-pod)..."
  local names
  names="$(
    oc get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}' |
      awk '$2=="Succeeded" && $1 ~ /^phase1-run-.*-(validate|route-and-enrich)-pod$/ {print $1}'
  )"
  if [[ -z "$names" ]]; then
    echo "[cleanup] No matching Succeeded pods."
    return 0
  fi
  # shellcheck disable=SC2086
  oc delete pod $names --ignore-not-found --wait=false
}

truncate_feature_tables() {
  echo "[cleanup] TRUNCATE feature_*_hourly tables in ${DB_NAME}..."
  oc exec -i "${DB_POD}" -- psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE
  feature_region_hourly,
  feature_clinic_hourly,
  feature_hospital_hourly,
  feature_diagnosis_hourly,
  feature_symptom_hourly;
SQL
}

run_phase1() {
  echo "[phase1] Applying Tasks and Pipeline..."
  oc apply -f tekton/tasks.yaml
  oc apply -f tekton/pipeline.yaml
  echo "[phase1] Creating PipelineRun from ${PIPELINERUN_MANIFEST}..."
  oc create -f "${PIPELINERUN_MANIFEST}"
  local pr
  pr="$(oc get pipelinerun --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')"
  echo "[phase1] Waiting for PipelineRun/${pr} (timeout ${PHASE1_WAIT_TIMEOUT})..."
  oc wait --for=condition=Succeeded "pipelinerun/${pr}" --timeout="${PHASE1_WAIT_TIMEOUT}"
  echo "[phase1] Succeeded: ${pr}"
}

run_phase2() {
  echo "[phase2] RBAC + ETL ConfigMap..."
  oc apply -f infra/spark-rbac.yaml
  oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
    --dry-run=client -o yaml | oc apply -f -
  echo "[phase2] Creating Job/${SPARK_JOB_NAME}..."
  oc apply -f infra/spark-job.yaml
  echo "[phase2] Waiting for job completion (timeout ${SPARK_WAIT_TIMEOUT})..."
  if oc wait --for=condition=complete "job/${SPARK_JOB_NAME}" --timeout="${SPARK_WAIT_TIMEOUT}"; then
    echo "[phase2] Job completed successfully."
  else
    echo "[phase2] Job did not report Complete in time or failed." >&2
    oc describe "job/${SPARK_JOB_NAME}" || true
    oc logs "job/${SPARK_JOB_NAME}" --tail=80 >&2 || true
    exit 1
  fi
  echo "[phase2] Last [ANALYTICS] summary lines:"
  oc logs "job/${SPARK_JOB_NAME}" 2>&1 | grep '\[ANALYTICS\]' | tail -20 || true
}

main() {
  need_oc
  local ns
  ns="$(current_ns)"
  if [[ -z "$ns" ]]; then
    echo "No namespace in current oc context; run: oc project <namespace>" >&2
    exit 1
  fi
  echo "Using OpenShift project: ${ns}"

  if [[ "$DO_CLEANUP" -eq 1 ]]; then
    cleanup_spark_job
    cleanup_phase1_succeeded_pods
  fi

  if [[ "$TRUNCATE_DB" -eq 1 ]]; then
    truncate_feature_tables
  fi

  if [[ "$DO_PHASE1" -eq 1 ]]; then
    run_phase1
  else
    echo "[phase1] Skipped."
  fi

  if [[ "$DO_PHASE2" -eq 1 ]]; then
    if [[ "$DO_CLEANUP" -eq 1 ]]; then
      cleanup_spark_job
      oc wait --for=delete "job/${SPARK_JOB_NAME}" --timeout=60s 2>/dev/null || true
    fi
    run_phase2
  else
    echo "[phase2] Skipped."
  fi

  echo "Done."
}

main
