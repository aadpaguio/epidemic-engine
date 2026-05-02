#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-start}"
NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
POD_NAME="${POD_NAME:-raw-typed-ingester}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-raw-typed-ingester-script}"
IMAGE="${IMAGE:-python:3.12-slim}"
CPU_REQUEST="${CPU_REQUEST:-100m}"
CPU_LIMIT="${CPU_LIMIT:-500m}"
MEM_REQUEST="${MEM_REQUEST:-256Mi}"
MEM_LIMIT="${MEM_LIMIT:-512Mi}"
GROUP_ID="${RAW_INGEST_GROUP_ID:-team02-raw-typed-ingest}"
AUTO_OFFSET_RESET="${AUTO_OFFSET_RESET:-latest}"
TOPICS="${TOPICS:-ds551-s26.team02.symptom_reports,ds551-s26.team02.clinic_visits,ds551-s26.team02.hospital_admissions,ds551-s26.team02.vaccination_records}"
POLL_TIMEOUT_MS="${POLL_TIMEOUT_MS:-1000}"
DB_FLUSH_EVERY="${DB_FLUSH_EVERY:-200}"
RUN_SECONDS="${RUN_SECONDS:-0}"
MAX_MESSAGES="${MAX_MESSAGES:-0}"

fetch_secret() {
  local field="$1"
  oc get secret epidemic-db-spark -n "$NAMESPACE" -o "jsonpath={.data.${field}}" \
    | python -c 'import base64,sys; print(base64.b64decode(sys.stdin.read()).decode())'
}

start_ingester() {
  local db_user db_password
  db_user="$(fetch_secret username)"
  db_password="$(fetch_secret password)"

  oc create configmap "$CONFIGMAP_NAME" \
    --from-file=ingest-typed-events-to-db.py=scripts/ingest-typed-events-to-db.py \
    --dry-run=client -o yaml \
    | oc apply -n "$NAMESPACE" -f - >/dev/null

  oc delete pod "$POD_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
  oc apply -n "$NAMESPACE" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
spec:
  restartPolicy: Never
  containers:
    - name: ${POD_NAME}
      image: ${IMAGE}
      resources:
        requests:
          cpu: "${CPU_REQUEST}"
          memory: "${MEM_REQUEST}"
        limits:
          cpu: "${CPU_LIMIT}"
          memory: "${MEM_LIMIT}"
      env:
        - name: KAFKA_BOOTSTRAP
          value: "kafka:9092"
        - name: DB_HOST
          value: "timescaledb-service"
        - name: DB_PORT
          value: "5432"
        - name: DB_NAME
          value: "epidemic"
        - name: DB_USER
          value: "${db_user}"
        - name: DB_PASSWORD
          value: "${db_password}"
        - name: RAW_INGEST_GROUP_ID
          value: "${GROUP_ID}"
        - name: AUTO_OFFSET_RESET
          value: "${AUTO_OFFSET_RESET}"
        - name: TOPICS
          value: "${TOPICS}"
        - name: POLL_TIMEOUT_MS
          value: "${POLL_TIMEOUT_MS}"
        - name: DB_FLUSH_EVERY
          value: "${DB_FLUSH_EVERY}"
        - name: RUN_SECONDS
          value: "${RUN_SECONDS}"
        - name: MAX_MESSAGES
          value: "${MAX_MESSAGES}"
      volumeMounts:
        - name: ingester-script
          mountPath: /opt/ingester
          readOnly: true
      command: ["/bin/bash", "-lc"]
      args:
        - |
          export HOME=/tmp
          export PIP_NO_CACHE_DIR=1
          pip install kafka-python-ng psycopg2-binary --target=/tmp/pkgs
          export PYTHONPATH="/tmp/pkgs${PYTHONPATH:+:$PYTHONPATH}"
          python -u /opt/ingester/ingest-typed-events-to-db.py
  volumes:
    - name: ingester-script
      configMap:
        name: ${CONFIGMAP_NAME}
EOF

  echo "Started pod/${POD_NAME} in namespace ${NAMESPACE}"
  echo "Use: bash scripts/run-raw-typed-ingester.sh logs"
}

case "$ACTION" in
  start)
    start_ingester
    ;;
  stop)
    oc delete pod "$POD_NAME" -n "$NAMESPACE" --ignore-not-found
    ;;
  logs)
    oc logs -f "pod/${POD_NAME}" -n "$NAMESPACE"
    ;;
  status)
    oc get pod "$POD_NAME" -n "$NAMESPACE" -o wide
    ;;
  once)
    RUN_SECONDS="${RUN_SECONDS:-30}" MAX_MESSAGES="${MAX_MESSAGES:-0}" start_ingester
    oc wait -n "$NAMESPACE" --for=condition=Ready "pod/${POD_NAME}" --timeout=120s >/dev/null || true
    oc wait -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded "pod/${POD_NAME}" --timeout=900s || true
    oc logs "pod/${POD_NAME}" -n "$NAMESPACE" --tail=200 || true
    ;;
  *)
    echo "Usage: $0 {start|stop|logs|status|once}" >&2
    exit 1
    ;;
esac
