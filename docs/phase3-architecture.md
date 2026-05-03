# Phase 3 Architecture

## System Overview

Our Epidemic Engine is a cloud-native, multi-stage data platform for synthetic epidemiological events. The system is designed around a staged event-processing model:

1. Kafka receives the raw stream
2. Tekton validates, enriches, and routes events into typed topics
3. Spark computes canonical hourly fact tables
4. TimescaleDB stores both the fact layer and the serving layer
5. Phase 3 features consume the database for visualization, alerting, and outbreak prediction

The architecture is intentionally split into **Layer A** and **Layer B**:

- **Layer A** = canonical Spark-written hourly fact tables
- **Layer B** = SQL serving views built on top of Layer A

This separation gives us a reusable feature-store design where Spark handles materialization of reliable hourly facts and SQL handles downstream analytics, ML features, and serving logic. 

## Kubernetes / OpenShift Architecture

```text
infra namespace
  └─ Event Generator
       ↓
team02 namespace
  ├─ Kafka topics
  │   ├─ ds551-s26.team02.raw
  │   ├─ ds551-s26.team02.validated
  │   ├─ ds551-s26.team02.symptom_reports
  │   ├─ ds551-s26.team02.clinic_visits
  │   ├─ ds551-s26.team02.hospital_admissions
  │   └─ ds551-s26.team02.vaccination_records
  │
  ├─ Tekton Pipeline
  │   ├─ Task: validate-events
  │   └─ Task: route-and-enrich
  │
  ├─ Spark Job
  │   └─ Job: epidemic-spark-etl
  │
  ├─ Database
  │   ├─ StatefulSet: timescaledb
  │   ├─ PVC: timescaledb-pvc
  │   ├─ Service: timescaledb-service
  │   └─ Headless Service: timescaledb-headless
  │
  └─ Phase 3 Consumers
      ├─ Dashboard / notebooks (Streamlit `epidemic-dashboard`)
      ├─ Alerting service (`epidemic-alert-service` Deployment)
      │   ├─ ConfigMap: `epidemic-alert-config`  (env vars)
      │   ├─ ConfigMap: `epidemic-alert-rules`   (`thresholds.json`)
      │   ├─ Secret:    `epidemic-slack-webhook` (`webhook_url`, optional)
      │   ├─ DB rules:  pipeline freshness + ML outbreak agreement
      │   ├─ Metric rules (multi-window): `event_count`, `avg_severity` from `raw_typed_events`
      │   └─ Outputs:   `alert_events` table, structured `[ALERT]` logs, Slack `#epidemic-alerts`
      └─ ML scoring / outbreak prediction

The alerting service is the rubric Category A feature. See
`docs/alerting-guide.md` for rule semantics and `docs/alert-config.md`
for the operational runbook (Slack secrets, threshold tuning, smoke
modes, failure recovery).