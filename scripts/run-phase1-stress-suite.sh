#!/usr/bin/env bash
# Run isolated Phase 1 stress scenarios:
# - smoke:    10 eps for 60s
# - moderate: 100 eps for 600s
# - burst:    1000 eps for 60s
#
# Each scenario uses a unique RUN_ID and creates isolated topics + consumer groups
# via tekton/pipelinerun-stress-template.yaml.
#
# Usage:
#   bash scripts/run-phase1-stress-suite.sh
#   bash scripts/run-phase1-stress-suite.sh --only smoke
#   bash scripts/run-phase1-stress-suite.sh --no-wait
#   STRESS_BATCH_SIZE=200000 bash scripts/run-phase1-stress-suite.sh --only moderate
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LOADGEN_JOB_MANIFEST="${LOADGEN_JOB_MANIFEST:-infra/loadgen-job.yaml}"
PIPELINE_MANIFEST="${PIPELINE_MANIFEST:-tekton/pipeline.yaml}"
TASKS_MANIFEST="${TASKS_MANIFEST:-tekton/tasks.yaml}"
STRESS_TEMPLATE="${STRESS_TEMPLATE:-tekton/pipelinerun-stress-template.yaml}"
# Max messages per Phase 1 task per PipelineRun (validate + route each process up to this many).
STRESS_BATCH_SIZE="${STRESS_BATCH_SIZE:-100000}"
# Idle timeout (ms) when no new Kafka messages; keep high for large backlogs.
STRESS_CONSUMER_TIMEOUT_MS="${STRESS_CONSUMER_TIMEOUT_MS:-300000}"
LOADGEN_JOB_NAME="${LOADGEN_JOB_NAME:-loadgen-raw-kafka}"
SPARK_JOB_MANIFEST="${SPARK_JOB_MANIFEST:-infra/spark-job.yaml}"
SPARK_RBAC_MANIFEST="${SPARK_RBAC_MANIFEST:-infra/spark-rbac.yaml}"
SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
LOADGEN_TIMEOUT="${LOADGEN_TIMEOUT:-30m}"
# Large batches need long Tekton runs (validate then route, Python consumer loop).
PHASE1_WAIT_TIMEOUT="${PHASE1_WAIT_TIMEOUT:-120m}"
SPARK_WAIT_TIMEOUT="${SPARK_WAIT_TIMEOUT:-90m}"
WAIT_FOR_PIPELINE=1
WITH_PHASE2=0
ONLY_SCENARIO="${ONLY_SCENARIO:-}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run-phase1-stress-suite.sh [--only smoke|moderate|burst] [--no-wait] [--with-phase2]

Options:
  --only <name>   Run only one scenario (smoke, moderate, burst).
  --no-wait       Do not wait for PipelineRun completion.
  --with-phase2   After Phase 1, run Spark Phase 2 on the same isolated RUN_ID topics.
  -h, --help      Show this help.

Environment overrides:
  LOADGEN_JOB_MANIFEST, PIPELINE_MANIFEST, TASKS_MANIFEST, STRESS_TEMPLATE,
  STRESS_BATCH_SIZE, STRESS_CONSUMER_TIMEOUT_MS,
  LOADGEN_JOB_NAME, LOADGEN_TIMEOUT, PHASE1_WAIT_TIMEOUT, SPARK_WAIT_TIMEOUT,
  SPARK_JOB_MANIFEST, SPARK_RBAC_MANIFEST, SPARK_JOB_NAME
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --only)
      ONLY_SCENARIO="${2:-}"
      shift 2
      ;;
    --no-wait)
      WAIT_FOR_PIPELINE=0
      shift
      ;;
    --with-phase2)
      WITH_PHASE2=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

need_oc() {
  command -v oc >/dev/null 2>&1 || {
    echo "error: oc not found in PATH" >&2
    exit 1
  }
}

current_ns() {
  oc config view --minify -o jsonpath='{..namespace}' 2>/dev/null || true
}

ensure_manifests() {
  [[ -f "$LOADGEN_JOB_MANIFEST" ]] || { echo "missing $LOADGEN_JOB_MANIFEST" >&2; exit 1; }
  [[ -f "$PIPELINE_MANIFEST" ]] || { echo "missing $PIPELINE_MANIFEST" >&2; exit 1; }
  [[ -f "$TASKS_MANIFEST" ]] || { echo "missing $TASKS_MANIFEST" >&2; exit 1; }
  [[ -f "$STRESS_TEMPLATE" ]] || { echo "missing $STRESS_TEMPLATE" >&2; exit 1; }
  [[ -f "$SPARK_JOB_MANIFEST" ]] || { echo "missing $SPARK_JOB_MANIFEST" >&2; exit 1; }
  [[ -f "$SPARK_RBAC_MANIFEST" ]] || { echo "missing $SPARK_RBAC_MANIFEST" >&2; exit 1; }
}

deploy_phase1() {
  echo "[setup] Applying Tekton tasks/pipeline..."
  oc apply -f "$TASKS_MANIFEST"
  oc apply -f "$PIPELINE_MANIFEST"
}

ensure_loadgen_configmap() {
  echo "[setup] Applying loadgen-produce-script ConfigMap..."
  oc create configmap loadgen-produce-script \
    --from-file=produce.py=loadgen/produce.py \
    --dry-run=client -o yaml | oc apply -f -
}

render_loadgen_manifest() {
  local topic="$1"
  local rate="$2"
  local duration="$3"
  awk -v topic="$topic" -v rate="$rate" -v duration="$duration" '
    {
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+LOADGEN_TOPIC[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" topic "\"")
        print
        next
      }
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+LOADGEN_RATE[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" rate "\"")
        print
        next
      }
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+LOADGEN_DURATION_SEC[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" duration "\"")
        print
        next
      }
      print
    }
  ' "$LOADGEN_JOB_MANIFEST"
}

create_pr_from_template() {
  local run_id="$1"
  local batch_size="$2"
  local consumer_timeout_ms="$3"
  sed -e "s/__RUN_ID__/${run_id}/g" \
    -e "s/__BATCH_SIZE__/${batch_size}/g" \
    -e "s/__CONSUMER_TIMEOUT_MS__/${consumer_timeout_ms}/g" \
    "$STRESS_TEMPLATE" | oc create -f -
}

render_spark_manifest() {
  local run_id="$1"
  local topic_symptom="$2"
  local topic_clinic="$3"
  local topic_hospital="$4"
  awk -v run_id="$run_id" -v topic_symptom="$topic_symptom" -v topic_clinic="$topic_clinic" -v topic_hospital="$topic_hospital" '
    {
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+TOPIC_SYMPTOM[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" topic_symptom "\"")
        print
        next
      }
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+TOPIC_CLINIC[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" topic_clinic "\"")
        print
        next
      }
      if ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+TOPIC_HOSPITAL[[:space:]]*$/) {
        print
        getline
        sub(/value:[[:space:]]*.*/, "value: \"" topic_hospital "\"")
        print
        next
      }
      if ($0 ~ /^[[:space:]]*volumeMounts:[[:space:]]*$/) {
        print "            - name: RUN_ID"
        print "              value: \"" run_id "\""
        print "            - name: BATCH_ID"
        print "              value: \"" run_id "\""
        print
        next
      }
      print
    }
  ' "$SPARK_JOB_MANIFEST"
}

latest_pr_name() {
  oc get pipelinerun --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}'
}

run_phase2_for_run() {
  local run_id="$1"
  local out_dir="$2"
  local topic_symptom="$3"
  local topic_clinic="$4"
  local topic_hospital="$5"

  echo "[phase2] Applying RBAC and Spark ETL script ConfigMap..."
  oc apply -f "$SPARK_RBAC_MANIFEST"
  oc create configmap spark-etl-script \
    --from-file=etl.py=spark/etl.py \
    --dry-run=client -o yaml | oc apply -f -

  echo "[phase2] Recreating Job/${SPARK_JOB_NAME} for run_id=${run_id}..."
  oc delete "job/${SPARK_JOB_NAME}" --ignore-not-found
  render_spark_manifest "$run_id" "$topic_symptom" "$topic_clinic" "$topic_hospital" | oc create -f -

  echo "[phase2] Waiting for Job completion (timeout ${SPARK_WAIT_TIMEOUT})..."
  if ! oc wait --for=condition=complete "job/${SPARK_JOB_NAME}" --timeout="${SPARK_WAIT_TIMEOUT}"; then
    echo "[phase2] Job did not complete in time; dumping describe/logs..."
    oc describe "job/${SPARK_JOB_NAME}" || true
    oc logs "job/${SPARK_JOB_NAME}" || true
    exit 1
  fi

  oc logs "job/${SPARK_JOB_NAME}" | tee "${out_dir}/phase2-spark.log" >/dev/null
}

run_scenario() {
  local scenario="$1"
  local rate="$2"
  local duration="$3"
  local run_id raw_topic pr_name out_dir
  local topic_symptom topic_clinic topic_hospital

  run_id="${scenario}-$(date +%Y%m%d-%H%M%S)"
  raw_topic="ds551-s26.team02.raw.${run_id}"
  topic_symptom="ds551-s26.team02.symptom_reports.${run_id}"
  topic_clinic="ds551-s26.team02.clinic_visits.${run_id}"
  topic_hospital="ds551-s26.team02.hospital_admissions.${run_id}"
  out_dir="logs/stress/${run_id}"
  mkdir -p "$out_dir"

  echo
  echo "=== Scenario: ${scenario} | rate=${rate}/s duration=${duration}s | run_id=${run_id} | batch-size=${STRESS_BATCH_SIZE} | consumer-timeout-ms=${STRESS_CONSUMER_TIMEOUT_MS} ==="

  echo "[loadgen] Recreating Job/${LOADGEN_JOB_NAME}..."
  oc delete "job/${LOADGEN_JOB_NAME}" --ignore-not-found
  render_loadgen_manifest "$raw_topic" "$rate" "$duration" | oc create -f -

  echo "[loadgen] Waiting for Job completion (timeout ${LOADGEN_TIMEOUT})..."
  if ! oc wait --for=condition=complete "job/${LOADGEN_JOB_NAME}" --timeout="$LOADGEN_TIMEOUT"; then
    echo "[loadgen] Job did not complete in time; dumping describe/logs..."
    oc describe "job/${LOADGEN_JOB_NAME}" || true
    oc logs "job/${LOADGEN_JOB_NAME}" || true
    exit 1
  fi
  oc logs "job/${LOADGEN_JOB_NAME}" | tee "${out_dir}/loadgen.log" >/dev/null

  echo "[phase1] Creating isolated PipelineRun..."
  create_pr_from_template "$run_id" "$STRESS_BATCH_SIZE" "$STRESS_CONSUMER_TIMEOUT_MS"
  pr_name="$(latest_pr_name)"
  echo "[phase1] Created PipelineRun/${pr_name}"

  if [[ "$WAIT_FOR_PIPELINE" -eq 1 ]]; then
    echo "[phase1] Waiting for PipelineRun/${pr_name} (timeout ${PHASE1_WAIT_TIMEOUT})..."
    if ! oc wait --for=condition=Succeeded "pipelinerun/${pr_name}" --timeout="${PHASE1_WAIT_TIMEOUT}"; then
      echo "[phase1] PipelineRun did not succeed in time; dumping describe..."
      oc describe "pipelinerun/${pr_name}" || true
      exit 1
    fi
  fi

  oc get "pipelinerun/${pr_name}" -o yaml > "${out_dir}/pipelinerun.yaml"
  oc get taskrun -l "tekton.dev/pipelineRun=${pr_name}" -o yaml > "${out_dir}/taskruns.yaml"

  if [[ "$WITH_PHASE2" -eq 1 ]]; then
    run_phase2_for_run "$run_id" "$out_dir" "$topic_symptom" "$topic_clinic" "$topic_hospital"
  fi

  echo "[done] Artifacts written to ${out_dir}"
}

main() {
  need_oc
  ensure_manifests

  local ns
  ns="$(current_ns)"
  if [[ -z "$ns" ]]; then
    echo "error: no namespace in current oc context (run: oc project <namespace>)" >&2
    exit 1
  fi
  echo "Using OpenShift project: ${ns}"

  deploy_phase1
  ensure_loadgen_configmap

  case "$ONLY_SCENARIO" in
    "" )
      run_scenario smoke 10 60
      run_scenario moderate 100 600
      run_scenario burst 1000 60
      ;;
    smoke)
      run_scenario smoke 10 60
      ;;
    moderate)
      run_scenario moderate 100 600
      ;;
    burst)
      run_scenario burst 1000 60
      ;;
    *)
      echo "error: --only must be one of: smoke, moderate, burst" >&2
      exit 1
      ;;
  esac

  echo
  echo "All requested scenarios complete."
}

main
