#!/usr/bin/env bash
# Smoke test Phase 1 Kafka processing:
#   raw topic -> validated topic -> typed symptom/clinic/hospital/vaccination topics.
#
# Run from repo root after `oc login` and `oc project <namespace>`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

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

SMOKE_RUN_ID="${SMOKE_RUN_ID:-smoke-$(date -u +%Y%m%d%H%M%S)}"
SMOKE_EVENT_COUNT="${SMOKE_EVENT_COUNT:-720}"
SMOKE_BATCH_SIZE="${SMOKE_BATCH_SIZE:-30}"
SMOKE_CONSUMER_TIMEOUT_MS="${SMOKE_CONSUMER_TIMEOUT_MS:-120000}"
SMOKE_VERIFY_TIMEOUT_SEC="${SMOKE_VERIFY_TIMEOUT_SEC:-90}"
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

create_smoke_producer_job() {
  local job_name="$1"
  oc create -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app: phase1-topic-smoke
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: phase1-topic-smoke
    spec:
      restartPolicy: Never
      containers:
        - name: produce-smoke-events
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
              import time
              from datetime import datetime, timezone

              from kafka import KafkaProducer

              bootstrap = os.environ["KAFKA_BOOTSTRAP"]
              topic = os.environ["RAW_TOPIC"]
              smoke_run_id = os.environ["SMOKE_RUN_ID"]
              count = int(os.environ["SMOKE_EVENT_COUNT"])

              producer = KafkaProducer(
                  bootstrap_servers=bootstrap,
                  value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                  key_serializer=lambda k: k.encode("utf-8"),
                  acks="all",
              )

              event_types = (
                  "symptom_report",
                  "clinic_visit",
                  "hospital_admission",
                  "vaccination_record",
              )

              def now_utc():
                  return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

              for i in range(count):
                  event_type = event_types[i % len(event_types)]
                  payload = {
                      "event_id": f"{smoke_run_id}-{i:04d}",
                      "smoke_run_id": smoke_run_id,
                      "event_type": event_type,
                      "timestamp": now_utc(),
                      "region": "Boston",
                      "patient_id": f"SMOKE-P{i:04d}",
                  }
                  if event_type == "symptom_report":
                      payload.update({
                          "age": 42,
                          "symptoms": ["fever", "cough"],
                          "severity": "moderate",
                          "duration_days": 2.0,
                          "reported_via": "smoke_test",
                      })
                  elif event_type == "clinic_visit":
                      payload.update({
                          "visit_id": f"SMOKE-V{i:04d}",
                          "clinic_id": "SMOKE-CLINIC",
                          "visit_type": "walk_in",
                          "primary_complaint": "fever",
                          "temperature_f": 101.2,
                          "diagnosis_code": "ICD-SMOKE",
                          "prescribed_medication": True,
                          "follow_up_required": False,
                      })
                  elif event_type == "hospital_admission":
                      payload.update({
                          "admission_id": f"SMOKE-A{i:04d}",
                          "hospital_id": "SMOKE-HOSPITAL",
                          "admission_reason": "observation",
                          "severity": "moderate",
                          "temperature_f": 100.4,
                          "oxygen_level": 96.0,
                          "expected_los_days": 1.0,
                          "available_beds": 10,
                      })
                  else:
                      payload.update({
                          "schema_version": 2,
                          "record_id": f"VR-{smoke_run_id}-{i:04d}",
                          "vaccine_type": "influenza",
                          "dose_number": 1,
                          "administered_at": now_utc(),
                      })

                  producer.send(topic, key=payload["event_id"], value=payload)
                  time.sleep(0.25)

              producer.flush()
              producer.close()
              print(json.dumps({
                  "smoke_run_id": smoke_run_id,
                  "topic": topic,
                  "events_produced": count,
              }))
              PY
          env:
            - name: KAFKA_BOOTSTRAP
              value: "${KAFKA_BOOTSTRAP}"
            - name: RAW_TOPIC
              value: "${RAW_TOPIC}"
            - name: SMOKE_RUN_ID
              value: "${SMOKE_RUN_ID}"
            - name: SMOKE_EVENT_COUNT
              value: "${SMOKE_EVENT_COUNT}"
YAML
}

create_smoke_pipelinerun() {
  oc create -f - -o name <<YAML
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: phase1-smoke-
  labels:
    app: phase1-topic-smoke
    smoke-run-id: ${SMOKE_RUN_ID}
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
      value: "${SMOKE_BATCH_SIZE}"
    - name: consumer-timeout-ms
      value: "${SMOKE_CONSUMER_TIMEOUT_MS}"
    - name: validate-consumer-group-id
      value: ${SMOKE_RUN_ID}-validate
    - name: route-consumer-group-id
      value: ${SMOKE_RUN_ID}-route
    - name: route-auto-offset-reset
      value: earliest
YAML
}

create_smoke_verifier_job() {
  local job_name="$1"
  oc create -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app: phase1-topic-smoke
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: phase1-topic-smoke
    spec:
      restartPolicy: Never
      containers:
        - name: verify-smoke-events
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
              import time

              from kafka import KafkaConsumer

              bootstrap = os.environ["KAFKA_BOOTSTRAP"]
              smoke_run_id = os.environ["SMOKE_RUN_ID"]
              timeout_sec = int(os.environ["SMOKE_VERIFY_TIMEOUT_SEC"])

              topics = {
                  "validated": os.environ["VALIDATED_TOPIC"],
                  "symptom": os.environ["SYMPTOM_TOPIC"],
                  "clinic": os.environ["CLINIC_TOPIC"],
                  "hospital": os.environ["HOSPITAL_TOPIC"],
                  "vaccination": os.environ["VACCINATION_TOPIC"],
              }
              expected_types = {
                  "symptom": "symptom_report",
                  "clinic": "clinic_visit",
                  "hospital": "hospital_admission",
                  "vaccination": "vaccination_record",
              }

              found = {name: 0 for name in topics}
              samples = {}

              for name, topic in topics.items():
                  deadline = time.time() + timeout_sec
                  consumer = KafkaConsumer(
                      topic,
                      bootstrap_servers=bootstrap,
                      group_id=f"{smoke_run_id}-verify-{name}",
                      auto_offset_reset="earliest",
                      enable_auto_commit=False,
                      consumer_timeout_ms=5000,
                      value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                  )
                  try:
                      while time.time() < deadline:
                          saw_any = False
                          for rec in consumer:
                              saw_any = True
                              payload = rec.value
                              if payload.get("smoke_run_id") != smoke_run_id:
                                  continue
                              if name in expected_types and payload.get("event_type") != expected_types[name]:
                                  continue
                              found[name] += 1
                              samples.setdefault(name, payload.get("event_id"))
                              break
                          if found[name] > 0:
                              break
                          if not saw_any:
                              time.sleep(1)
                  finally:
                      consumer.close()

              missing = [name for name, count in found.items() if count < 1]
              result = {
                  "smoke_run_id": smoke_run_id,
                  "found": found,
                  "sample_event_ids": samples,
                  "status": "passed" if not missing else "failed",
                  "missing": missing,
              }
              print(json.dumps(result, indent=2, sort_keys=True))

              if missing:
                  raise SystemExit(1)
              PY
          env:
            - name: KAFKA_BOOTSTRAP
              value: "${KAFKA_BOOTSTRAP}"
            - name: SMOKE_RUN_ID
              value: "${SMOKE_RUN_ID}"
            - name: SMOKE_VERIFY_TIMEOUT_SEC
              value: "${SMOKE_VERIFY_TIMEOUT_SEC}"
            - name: VALIDATED_TOPIC
              value: "${VALIDATED_TOPIC}"
            - name: SYMPTOM_TOPIC
              value: "${SYMPTOM_TOPIC}"
            - name: CLINIC_TOPIC
              value: "${CLINIC_TOPIC}"
            - name: HOSPITAL_TOPIC
              value: "${HOSPITAL_TOPIC}"
            - name: VACCINATION_TOPIC
              value: "${VACCINATION_TOPIC}"
YAML
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
  safe_run_id="${safe_run_id:-smoke}"

  local producer_job="phase1-smoke-producer-${safe_run_id}"
  local verifier_job="phase1-smoke-verifier-${safe_run_id}"
  producer_job="${producer_job:0:63}"
  verifier_job="${verifier_job:0:63}"

  echo "[smoke] namespace=${ns}"
  echo "[smoke] smoke_run_id=${SMOKE_RUN_ID}"
  echo "[smoke] applying Phase 1 Tekton resources"
  oc apply -f tekton/tasks.yaml
  oc apply -f tekton/pipeline.yaml

  echo "[smoke] creating temporary PipelineRun"
  local pr_ref pr_name
  pr_ref="$(create_smoke_pipelinerun)"
  pr_name="${pr_ref##*/}"

  echo "[smoke] starting producer job/${producer_job}"
  oc delete "job/${producer_job}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  create_smoke_producer_job "$producer_job"

  echo "[smoke] waiting for PipelineRun/${pr_name}"
  if ! oc wait --for=condition=Succeeded "pipelinerun/${pr_name}" --timeout=300s; then
    echo "[smoke] PipelineRun failed or timed out" >&2
    oc describe "pipelinerun/${pr_name}" >&2 || true
    oc logs -l "tekton.dev/pipelineRun=${pr_name}" --all-containers=true --tail=120 >&2 || true
    exit 1
  fi

  echo "[smoke] waiting for producer completion"
  oc wait --for=condition=complete "job/${producer_job}" --timeout=180s

  echo "[smoke] starting verifier job/${verifier_job}"
  oc delete "job/${verifier_job}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  create_smoke_verifier_job "$verifier_job"

  if ! oc wait --for=condition=complete "job/${verifier_job}" --timeout=180s; then
    echo "[smoke] verifier failed" >&2
    oc logs "job/${verifier_job}" >&2 || true
    exit 1
  fi

  echo "[smoke] verifier output"
  oc logs "job/${verifier_job}"

  if [[ "$SMOKE_KEEP_RESOURCES" != "1" ]]; then
    echo "[smoke] cleaning smoke producer/verifier jobs"
    oc delete "job/${producer_job}" "job/${verifier_job}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi

  echo "[smoke] passed: validated and typed topics contain smoke records"
}

main "$@"
