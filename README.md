# Epidemic Engine

Epidemic Engine is a data pipeline for synthetic disease-surveillance events. It reads raw events from Kafka, validates them, and stores hourly regional statistics in TimescaleDB. A dashboard, an outbreak-risk model, and an alert service read those statistics.

The pipeline runs on OpenShift (Kubernetes). We built it as a four-person team project for DS-551 Data Engineering at Scale at Boston University (Spring 2026).

## How it works

```text
Kafka raw topic
  -> Tekton: validate-events      (rejects bad JSON, missing fields, bad timestamps)
  -> Tekton: route-and-enrich     (splits events into typed topics)
  -> Kafka typed topics           (symptom reports, clinic visits, hospital admissions, vaccination records)
  -> Ingester                     (writes each event once to raw_typed_events)
  -> Spark ETL                    (aggregates events into hourly fact tables)
  -> TimescaleDB views            (analytics and ML features)
  -> Streamlit dashboard, ML scoring, alert service
```

| Stage | Technology | What it does |
| --- | --- | --- |
| Ingestion | Kafka, Tekton | Validates events, sends bad records to a dead-letter topic, and routes good records to one topic per event type. |
| Raw landing | Python, TimescaleDB | Stores each typed event in `raw_typed_events`. The key is `(topic, partition, offset)`, so a replay does not create duplicates. |
| Batch ETL | Apache Spark | Reads raw events, groups them by hour, and writes the `feature_*_hourly` tables. |
| Serving | PostgreSQL views | Expose the latest row for each hour, regional trends, data-quality checks, and ML features. |
| Dashboard | Streamlit | Shows event volume, regional severity, hospital load, predictions, and alerts. |
| ML | Python | Trains an outbreak-risk model on windowed regional features and writes scores to `ml_outbreak_predictions`. |
| Alerts | Python | Compares ML features with thresholds and writes alerts to `alert_events`. |

For the full system map, see [`docs/architecture.md`](docs/architecture.md).

## Run it

You need access to an OpenShift namespace with Kafka and TimescaleDB.

1. Deploy the Phase 1 Tekton pipeline:

   ```bash
   oc apply -f tekton/tasks.yaml -n <namespace>
   oc apply -f tekton/pipeline.yaml -n <namespace>
   oc create -f tekton/pipelinerun.yaml -n <namespace>
   ```

2. Set up the database, the ingester, and the Spark job. Follow [`docs/runbook.md`](docs/runbook.md). It also shows how to run the pipeline for several hours within namespace quotas.

3. Start the dashboard on your computer. Follow [`docs/visualization-guide.md`](docs/visualization-guide.md).

4. Run the tests:

   ```bash
   scripts/run-test-suite.sh
   ```

## Repository layout

```text
tekton/    Phase 1 Tekton tasks, pipelines, and pipeline runs
scripts/   Ingester, orchestration loops, and smoke tests
spark/     Phase 2 Spark ETL (etl.py)
infra/     Kubernetes manifests and database schema (database-init.sql)
app/       Streamlit dashboard and its SQL queries
ml/        Model training and batch scoring
alert/     Alert service and its manifests
loadgen/   Kafka load generator
tests/     Unit and integration tests
docs/      Architecture, runbooks, and guides
```

## Documentation

- [Architecture](docs/architecture.md)
- [Runbook](docs/runbook.md)
- [Spark ETL and environment variables](spark/README.md)
- [ML model](docs/ml-model-guide.md)
- [Alerting](docs/alerting-guide.md) and [alert configuration](docs/alert-config.md)
- [Data-quality monitoring](docs/data-quality-monitoring.md)
- [Dashboard](docs/visualization-guide.md)
- [Testing](docs/testing-suite.md)
- [Stress test results](docs/phase2-stress-test.md)
- [Vaccination change order (CO-2026-01)](docs/change-order-vaccination.md)

The course assignment is in [`INSTRUCTIONS.md`](INSTRUCTIONS.md).

## Team

- Aidan Smith
- Arnald Paguio
- Evan Ferguson-Hull
- Jason Nguyen
