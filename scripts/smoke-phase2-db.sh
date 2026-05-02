#!/usr/bin/env bash
# Smoke test Phase 2 Spark ETL:
#   typed Kafka topics -> Spark ETL -> feature_*_hourly tables (incl. vaccination).
# Does not run raw typed-topic ingest into raw_typed_events; for the recommended path
# (Phase 1 -> ingest -> Spark from DB), use scripts/run-phase1-rawdb-sequenced.sh instead.
#
# Run from repo root after `oc login` and `oc project <namespace>`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka.ds551-2026-spring-a5f0d1.svc.cluster.local:9092}"
TOPIC_SYMPTOM="${TOPIC_SYMPTOM:-ds551-s26.team02.symptom_reports}"
TOPIC_CLINIC="${TOPIC_CLINIC:-ds551-s26.team02.clinic_visits}"
TOPIC_HOSPITAL="${TOPIC_HOSPITAL:-ds551-s26.team02.hospital_admissions}"
TOPIC_VACCINATION="${TOPIC_VACCINATION:-ds551-s26.team02.vaccination_records}"

SPARK_JOB_NAME="${SPARK_JOB_NAME:-epidemic-spark-etl}"
SPARK_WAIT_TIMEOUT="${SPARK_WAIT_TIMEOUT:-900s}"
DB_POD="${DB_POD:-timescaledb-0}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"

SMOKE_RUN_ID="${SMOKE_RUN_ID:-phase2-smoke-$(date -u +%Y%m%d%H%M%S)}"
SMOKE_BATCH_ID="${SMOKE_BATCH_ID:-${SMOKE_RUN_ID}}"
SMOKE_KEEP_RESOURCES="${SMOKE_KEEP_RESOURCES:-0}"

need_oc() {
  command -v oc >/dev/null 2>&1 || {
    echo "oc not found in PATH" >&2
    exit 1
  }
}

current_ns() {
  oc config view --minify -o jsonpath='{..namespace}' 2>/dev/null || true
}

resolve_window() {
  python - <<'PY'
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)
start = now.replace(minute=0, second=0, microsecond=0)
end = start + timedelta(hours=1)
event_ts = now.replace(microsecond=0)

def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")

print(iso(start), iso(end), iso(event_ts))
PY
}

create_typed_producer_job() {
  local job_name="$1"
  local event_ts="$2"
  oc create -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app: phase2-db-smoke
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: phase2-db-smoke
    spec:
      restartPolicy: Never
      containers:
        - name: produce-typed-smoke-events
          image: python:3.11-slim
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-lc"]
          args:
            - |
              set -euo pipefail
              export HOME=/tmp
              pip install --no-cache-dir -q --user kafka-python-ng
              python - <<'PY'
              import json
              import os

              from kafka import KafkaProducer

              bootstrap = os.environ["KAFKA_BOOTSTRAP"]
              smoke_run_id = os.environ["SMOKE_RUN_ID"]
              event_ts = os.environ["SMOKE_EVENT_TS"]
              region = os.environ["SMOKE_REGION"]
              clinic_id = os.environ["SMOKE_CLINIC_ID"]
              hospital_id = os.environ["SMOKE_HOSPITAL_ID"]
              diagnosis_code = os.environ["SMOKE_DIAGNOSIS_CODE"]
              symptom_token = os.environ["SMOKE_SYMPTOM_TOKEN"]
              vaccine_type = os.environ["SMOKE_VACCINE_TYPE"]
              dose_number = int(os.environ["SMOKE_DOSE_NUMBER"])

              producer = KafkaProducer(
                  bootstrap_servers=bootstrap,
                  value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                  key_serializer=lambda k: k.encode("utf-8"),
                  acks="all",
              )

              records = [
                  (
                      os.environ["TOPIC_SYMPTOM"],
                      {
                          "event_id": f"{smoke_run_id}-symptom",
                          "event_type": "symptom_report",
                          "timestamp": event_ts,
                          "region": region,
                          "patient_id": f"{smoke_run_id}-patient-s",
                          "age": 42,
                          "symptoms": [symptom_token],
                          "severity": "moderate",
                          "duration_days": 2.0,
                          "reported_via": "phase2_smoke",
                          "available_beds": 7,
                      },
                  ),
                  (
                      os.environ["TOPIC_CLINIC"],
                      {
                          "event_id": f"{smoke_run_id}-clinic",
                          "event_type": "clinic_visit",
                          "timestamp": event_ts,
                          "region": region,
                          "patient_id": f"{smoke_run_id}-patient-c",
                          "visit_id": f"{smoke_run_id}-visit",
                          "clinic_id": clinic_id,
                          "visit_type": "walk_in",
                          "primary_complaint": "fever",
                          "temperature_f": 101.2,
                          "diagnosis_code": diagnosis_code,
                          "prescribed_medication": True,
                          "follow_up_required": True,
                          "available_beds": 8,
                      },
                  ),
                  (
                      os.environ["TOPIC_HOSPITAL"],
                      {
                          "event_id": f"{smoke_run_id}-hospital",
                          "event_type": "hospital_admission",
                          "timestamp": event_ts,
                          "region": region,
                          "patient_id": f"{smoke_run_id}-patient-h",
                          "admission_id": f"{smoke_run_id}-admission",
                          "hospital_id": hospital_id,
                          "admission_reason": "observation",
                          "severity": "moderate",
                          "temperature_f": 100.4,
                          "oxygen_level": 96.0,
                          "expected_los_days": 1.0,
                          "available_beds": 9,
                      },
                  ),
                  (
                      os.environ["TOPIC_VACCINATION"],
                      {
                          "event_id": f"{smoke_run_id}-vax",
                          "event_type": "vaccination_record",
                          "schema_version": 2,
                          "timestamp": event_ts,
                          "region": region,
                          "patient_id": f"{smoke_run_id}-patient-v",
                          "record_id": f"{smoke_run_id}-vr",
                          "vaccine_type": vaccine_type,
                          "dose_number": dose_number,
                          "administered_at": event_ts,
                      },
                  ),
              ]

              for topic, payload in records:
                  producer.send(topic, key=payload["event_id"], value=payload)

              producer.flush()
              producer.close()
              print(json.dumps({
                  "smoke_run_id": smoke_run_id,
                  "event_ts": event_ts,
                  "topics": [topic for topic, _ in records],
              }))
              PY
          env:
            - name: KAFKA_BOOTSTRAP
              value: "${KAFKA_BOOTSTRAP}"
            - name: TOPIC_SYMPTOM
              value: "${TOPIC_SYMPTOM}"
            - name: TOPIC_CLINIC
              value: "${TOPIC_CLINIC}"
            - name: TOPIC_HOSPITAL
              value: "${TOPIC_HOSPITAL}"
            - name: TOPIC_VACCINATION
              value: "${TOPIC_VACCINATION}"
            - name: SMOKE_RUN_ID
              value: "${SMOKE_RUN_ID}"
            - name: SMOKE_EVENT_TS
              value: "${event_ts}"
            - name: SMOKE_REGION
              value: "${SMOKE_REGION}"
            - name: SMOKE_CLINIC_ID
              value: "${SMOKE_CLINIC_ID}"
            - name: SMOKE_HOSPITAL_ID
              value: "${SMOKE_HOSPITAL_ID}"
            - name: SMOKE_DIAGNOSIS_CODE
              value: "${SMOKE_DIAGNOSIS_CODE}"
            - name: SMOKE_SYMPTOM_TOKEN
              value: "${SMOKE_SYMPTOM_TOKEN}"
            - name: SMOKE_VACCINE_TYPE
              value: "${SMOKE_VACCINE_TYPE}"
            - name: SMOKE_DOSE_NUMBER
              value: "${SMOKE_DOSE_NUMBER}"
YAML
}

launch_spark_smoke_job() {
  local window_start="$1"
  local window_end="$2"

  oc delete "job/${SPARK_JOB_NAME}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  oc create -f infra/spark-job.yaml --dry-run=client -o yaml |
    oc set env -f - \
      --local \
      -o yaml \
      KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP}" \
      TOPIC_SYMPTOM="${TOPIC_SYMPTOM}" \
      TOPIC_CLINIC="${TOPIC_CLINIC}" \
      TOPIC_HOSPITAL="${TOPIC_HOSPITAL}" \
      TOPIC_VACCINATION="${TOPIC_VACCINATION}" \
      WINDOW_START_UTC="${window_start}" \
      WINDOW_END_UTC="${window_end}" \
      REQUIRE_WINDOW_UTC="true" \
      RUN_ID="${SMOKE_RUN_ID}" \
      BATCH_ID="${SMOKE_BATCH_ID}" |
    oc apply -f -
}

verify_db_rows() {
  local out
  out="$(
    oc exec -i "${DB_POD}" -- psql -U "${DB_USER}" -d "${DB_NAME}" -At -v ON_ERROR_STOP=1 \
      -v batch_id="${SMOKE_BATCH_ID}" \
      -v run_id="${SMOKE_RUN_ID}" \
      -v region="${SMOKE_REGION}" \
      -v clinic_id="${SMOKE_CLINIC_ID}" \
      -v hospital_id="${SMOKE_HOSPITAL_ID}" \
      -v diagnosis_code="${SMOKE_DIAGNOSIS_CODE}" \
      -v symptom_token="${SMOKE_SYMPTOM_TOKEN}" \
      -v vaccine_type="${SMOKE_VACCINE_TYPE}" \
      -v dose_number="${SMOKE_DOSE_NUMBER}" <<'SQL'
WITH checks(name, ok_count) AS (
  SELECT 'batch_window_runs', COUNT(*)
  FROM batch_window_runs
  WHERE batch_id = :'batch_id'
    AND run_id = :'run_id'
    AND status = 'succeeded'

  UNION ALL
  SELECT 'feature_region_hourly', COUNT(*)
  FROM feature_region_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND symptom_count >= 1
    AND clinic_visit_count >= 1
    AND admission_count >= 1

  UNION ALL
  SELECT 'feature_clinic_hourly', COUNT(*)
  FROM feature_clinic_hourly
  WHERE batch_id = :'batch_id'
    AND clinic_id = :'clinic_id'
    AND visit_count >= 1

  UNION ALL
  SELECT 'feature_hospital_hourly', COUNT(*)
  FROM feature_hospital_hourly
  WHERE batch_id = :'batch_id'
    AND hospital_id = :'hospital_id'
    AND admission_count >= 1

  UNION ALL
  SELECT 'feature_diagnosis_hourly', COUNT(*)
  FROM feature_diagnosis_hourly
  WHERE batch_id = :'batch_id'
    AND clinic_id = :'clinic_id'
    AND diagnosis_code = :'diagnosis_code'
    AND diagnosis_visit_count >= 1

  UNION ALL
  SELECT 'feature_symptom_hourly', COUNT(*)
  FROM feature_symptom_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND symptom_token = :'symptom_token'
    AND symptom_token_count >= 1

  UNION ALL
  SELECT 'feature_vaccination_hourly', COUNT(*)
  FROM feature_vaccination_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND vaccine_type = :'vaccine_type'
    AND dose_number = :'dose_number'::integer
    AND vaccination_count >= 1
)
SELECT name || '=' || ok_count
FROM checks
ORDER BY name;

WITH checks(name, ok_count) AS (
  SELECT 'batch_window_runs', COUNT(*)
  FROM batch_window_runs
  WHERE batch_id = :'batch_id'
    AND run_id = :'run_id'
    AND status = 'succeeded'
  UNION ALL
  SELECT 'feature_region_hourly', COUNT(*)
  FROM feature_region_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND symptom_count >= 1
    AND clinic_visit_count >= 1
    AND admission_count >= 1
  UNION ALL
  SELECT 'feature_clinic_hourly', COUNT(*)
  FROM feature_clinic_hourly
  WHERE batch_id = :'batch_id'
    AND clinic_id = :'clinic_id'
    AND visit_count >= 1
  UNION ALL
  SELECT 'feature_hospital_hourly', COUNT(*)
  FROM feature_hospital_hourly
  WHERE batch_id = :'batch_id'
    AND hospital_id = :'hospital_id'
    AND admission_count >= 1
  UNION ALL
  SELECT 'feature_diagnosis_hourly', COUNT(*)
  FROM feature_diagnosis_hourly
  WHERE batch_id = :'batch_id'
    AND clinic_id = :'clinic_id'
    AND diagnosis_code = :'diagnosis_code'
    AND diagnosis_visit_count >= 1
  UNION ALL
  SELECT 'feature_symptom_hourly', COUNT(*)
  FROM feature_symptom_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND symptom_token = :'symptom_token'
    AND symptom_token_count >= 1
  UNION ALL
  SELECT 'feature_vaccination_hourly', COUNT(*)
  FROM feature_vaccination_hourly
  WHERE batch_id = :'batch_id'
    AND region = :'region'
    AND vaccine_type = :'vaccine_type'
    AND dose_number = :'dose_number'::integer
    AND vaccination_count >= 1
)
SELECT 'missing=' || COALESCE(string_agg(name, ',' ORDER BY name), '')
FROM checks
WHERE ok_count < 1;
SQL
  )"

  echo "$out"
  if echo "$out" | grep -q '^missing=.$\|^missing=[^[:space:]]'; then
    echo "[smoke] DB verification failed" >&2
    return 1
  fi
}

main() {
  need_oc
  local ns
  ns="$(current_ns)"
  if [[ -z "$ns" ]]; then
    echo "No namespace in current oc context; run: oc project <namespace>" >&2
    exit 1
  fi

  local safe_run_id
  safe_run_id="$(echo "$SMOKE_RUN_ID" | tr '[:upper:]_' '[:lower:]-' | tr -c 'a-z0-9-' '-' | sed 's/^-*//;s/-*$//')"
  safe_run_id="${safe_run_id:-phase2-smoke}"

  SMOKE_REGION="${SMOKE_REGION:-smoke-region-${safe_run_id}}"
  SMOKE_CLINIC_ID="${SMOKE_CLINIC_ID:-smoke-clinic-${safe_run_id}}"
  SMOKE_HOSPITAL_ID="${SMOKE_HOSPITAL_ID:-smoke-hospital-${safe_run_id}}"
  SMOKE_DIAGNOSIS_CODE="${SMOKE_DIAGNOSIS_CODE:-SMOKE-${safe_run_id}}"
  SMOKE_SYMPTOM_TOKEN="${SMOKE_SYMPTOM_TOKEN:-smoke-symptom-${safe_run_id}}"
  SMOKE_VACCINE_TYPE="${SMOKE_VACCINE_TYPE:-influenza}"
  SMOKE_DOSE_NUMBER="${SMOKE_DOSE_NUMBER:-1}"

  local window_start window_end event_ts
  read -r window_start window_end event_ts < <(resolve_window)

  local producer_job="phase2-smoke-producer-${safe_run_id}"
  producer_job="${producer_job:0:63}"

  echo "[smoke] namespace=${ns}"
  echo "[smoke] run_id=${SMOKE_RUN_ID}"
  echo "[smoke] batch_id=${SMOKE_BATCH_ID}"
  echo "[smoke] window=${window_start} to ${window_end}"

  echo "[smoke] applying database schema/RBAC and refreshing Spark script"
  oc apply -f infra/database-deployment.yaml
  oc apply -f infra/spark-rbac.yaml
  oc create configmap spark-etl-script --from-file=etl.py=spark/etl.py \
    --dry-run=client -o yaml | oc apply -f -

  echo "[smoke] producing typed Kafka records"
  oc delete "job/${producer_job}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  create_typed_producer_job "$producer_job" "$event_ts"
  oc wait --for=condition=complete "job/${producer_job}" --timeout=180s
  oc logs "job/${producer_job}"

  echo "[smoke] launching Spark ETL job/${SPARK_JOB_NAME}"
  launch_spark_smoke_job "$window_start" "$window_end"
  if ! oc wait --for=condition=complete "job/${SPARK_JOB_NAME}" --timeout="${SPARK_WAIT_TIMEOUT}"; then
    echo "[smoke] Spark job failed or timed out" >&2
    oc describe "job/${SPARK_JOB_NAME}" >&2 || true
    oc logs "job/${SPARK_JOB_NAME}" --tail=160 >&2 || true
    exit 1
  fi

  echo "[smoke] Spark analytics summary"
  oc logs "job/${SPARK_JOB_NAME}" 2>&1 | grep '\[ANALYTICS\]' | tail -20 || true

  echo "[smoke] verifying DB feature rows"
  verify_db_rows

  if [[ "$SMOKE_KEEP_RESOURCES" != "1" ]]; then
    echo "[smoke] cleaning smoke producer job"
    oc delete "job/${producer_job}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi

  echo "[smoke] passed: Spark read typed topics and wrote feature tables"
}

main "$@"
