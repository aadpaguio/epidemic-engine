#!/usr/bin/env bash
# Commands to deploy and test the Phase 1 Tekton pipeline.
#
# Prerequisites: oc login, correct kube context, Tekton installed in the namespace.
#
# Usage (from anywhere):
#   export NAMESPACE="your-openshift-project"
#   ./tekton/pipeline-test-commands.sh deploy   # apply Task + Pipeline (idempotent)
#   ./tekton/pipeline-test-commands.sh run      # start a new PipelineRun
#   ./tekton/pipeline-test-commands.sh status   # list PipelineRuns and TaskRuns
#
# pipelinerun.yaml uses metadata.generateName — use "oc create" for each run, not "oc apply".
#
# Manual copy-paste (from repository root), after: export NAMESPACE=...
#
#   oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
#   oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
#   oc create -f tekton/pipelinerun.yaml -n "$NAMESPACE"
#
#   oc get pipelinerun -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp
#   oc get taskrun -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp
#   oc get pods -n "$NAMESPACE" -l tekton.dev/pipelineRun
#
#   oc describe pipelinerun <pipelinerun-name> -n "$NAMESPACE"
#   oc logs -f <task-step-pod-name> -n "$NAMESPACE"
#
# Optional (Tekton CLI):
#   tkn pipelinerun logs <pipelinerun-name> -n "$NAMESPACE" -f
#   tkn taskrun logs <taskrun-name> -n "$NAMESPACE" -f

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="${NAMESPACE:-}"

need_ns() {
  if [[ -z "$NS" ]]; then
    echo "error: set NAMESPACE to your OpenShift project (e.g. export NAMESPACE=ds551-2026-spring-team-02)" >&2
    exit 1
  fi
}

cmd_deploy() {
  need_ns
  oc apply -f "$ROOT/tekton/tasks.yaml" -n "$NS"
  oc apply -f "$ROOT/tekton/pipeline.yaml" -n "$NS"
}

cmd_run() {
  need_ns
  oc create -f "$ROOT/tekton/pipelinerun.yaml" -n "$NS"
}

cmd_status() {
  need_ns
  echo "=== PipelineRuns ==="
  oc get pipelinerun -n "$NS" --sort-by=.metadata.creationTimestamp
  echo
  echo "=== TaskRuns ==="
  oc get taskrun -n "$NS" --sort-by=.metadata.creationTimestamp
}

usage() {
  echo "usage: NAMESPACE=<project> $0 {deploy|run|status}" >&2
  exit 1
}

case "${1:-}" in
  deploy) cmd_deploy ;;
  run)    cmd_run ;;
  status) cmd_status ;;
  *)      usage ;;
esac
