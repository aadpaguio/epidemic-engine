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
  ↓
Spark batch ETL job: epidemic-spark-etl
  ↓
TimescaleDB Layer A fact tables
  ↓
TimescaleDB Layer B SQL views
  ↓
Phase 3 consumers:
  - Dashboard / visualization
  - Alerting service
  - Outbreak prediction model