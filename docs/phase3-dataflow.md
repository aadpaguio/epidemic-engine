# Phase 3 Data Flow

## End-to-End System Flow

Our Epidemic Engine processes synthetic epidemiological events through a multi-stage pipeline:

```text
Event Generator (infra namespace)
  ↓
Kafka raw topic: ds551-s26.team02.raw
  ↓
Tekton Task: validate-events
  ↓
Kafka validated topic: ds551-s26.team02.validated
  ↓
Tekton Task: route-and-enrich
  ↓
Kafka typed topics:
  - ds551-s26.team02.symptom_reports
  - ds551-s26.team02.clinic_visits
  - ds551-s26.team02.hospital_admissions
  - ds551-s26.team02.vaccination_records
  ↓
Spark batch ETL job: epidemic-spark-etl
  ↓
TimescaleDB Layer A fact tables
  ↓
TimescaleDB Layer B SQL views
  ↓
Phase 3 consumers:
  - Dashboard / visualization
  - Alerting service (`epidemic-alert-service`):
      - reads `raw_typed_events`, `ml_outbreak_predictions`, `vw_region_hourly_ml_features_windowed`
      - writes `alert_events` and structured `[ALERT]` JSON logs
      - posts to Slack `#epidemic-alerts` via incoming webhook
  - Outbreak prediction model