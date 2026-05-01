# ML Model Guide

## Goal

Predict outbreak risk by region using Phase 2 analytics stored in TimescaleDB.

The ML component is a Phase 3 proof-of-concept outbreak prediction workflow. It shows that our pipeline can turn Kafka/Tekton/Spark/TimescaleDB analytics into model-ready features, train a supervised model, score recent regional windows, and write predictions back into the database.

## Database Objects

The ML feature store is defined in the TimescaleDB schema and includes:

- `vw_region_hourly_ml_features`
- `vw_region_hourly_ml_features_windowed`
- `ml_outbreak_predictions`

The base ML view reads from `analytic_regional_disease_severity_trend`. The windowed view adds rolling and lagged regional features. The prediction table stores scored outputs from `ml/predict.py` so dashboard and alerting consumers can read model results from TimescaleDB.

## Input Data

The model reads from:

- `vw_region_hourly_ml_features`
- `vw_region_hourly_ml_features_windowed`

These views are built on top of the Phase 2 TimescaleDB feature store.

The current model uses synthetic course-generated data that passed through the deployed end-to-end pipeline:

```text
Kafka raw topic
→ Tekton validation/routing
→ typed Kafka topics
→ Spark hourly facts
→ TimescaleDB Layer A / Layer B
→ ML feature views
```

This is real pipeline output, but it is still synthetic project data, not real clinical or public health data.

## Candidate Features

The model uses regional outbreak-related signals:

- `total_case_events`
- `avg_severity`
- `symptom_count`
- `admission_count`
- `clinic_visit_count`
- `admission_share`
- `cases_prev_6h`
- `cases_prev_24h`
- `severity_prev_6h_avg`
- `severity_prev_24h_avg`
- `total_case_events_lag_6h`
- `total_case_events_lag_24h`

These features focus on active outbreak signals: symptom reports, clinic visits, hospital admissions, regional severity, and recent temporal trends.

## Labeling Strategy

The course-generated data does not include real ground-truth outbreak labels, so the training script creates a synthetic label called `has_outbreak`.

Initial baseline rule:

```text
has_outbreak = 1 when total_case_events >= 50 and avg_severity >= 2.3
has_outbreak = 0 otherwise
```

This means a region-hour is labeled as an outbreak only when both event volume and severity are elevated.

This label is a heuristic for supervised ML in the project. It is not a clinically validated public-health label.

## Model

The current model is a `RandomForestClassifier`.

The model is trained by:

```text
ml/train.py
```

The training script:

1. connects to TimescaleDB using `DATABASE_URL`
2. reads rows from `vw_region_hourly_ml_features_windowed`
3. creates the synthetic `has_outbreak` label
4. trains a Random Forest model
5. saves the model and feature column list to `ml/model.pkl`

## Moving average detector

Alongside the Random Forest, `ml/predict.py` runs a **k-bucket moving-average** detector implemented in `ml/detectors.py`.

- For each region, hourly rows are ordered by `bucket_start`.
- The baseline for the current bucket is the **mean of the previous k** `total_case_events` values (the current bucket is excluded so a spike is not averaged into its own baseline).
- A z-score compares the current count to the standard deviation of those previous k counts (with a small floor when the std is zero).
- `high_risk` when `z >= MOVING_AVG_Z_THRESHOLD` (default `2.0`) **and** `avg_severity >= MOVING_AVG_SEVERITY_THRESHOLD` (default `2.3`).
- `risk_score` is `min(z / 4, 1.0)` for dashboard-friendly scaling.

Rows are written to `ml_outbreak_predictions` with `model_version` set by `MOVING_AVG_MODEL_VERSION` (default `moving_avg_k6_v1`). The forest continues to use `MODEL_VERSION` (default `v1`).

**Lookback:** The scoring query reads `ML_LOOKBACK_HOURS` (default `72`) of history so the first buckets in the output window still have a trailing baseline. Only the last `ML_OUTPUT_WINDOW_HOURS` (default `24`) are written for both methods.

Relevant environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `ML_LOOKBACK_HOURS` | 72 | SQL window for feature rows |
| `ML_OUTPUT_WINDOW_HOURS` | 24 | Buckets to insert per run |
| `MOVING_AVG_K` | 6 | Prior-window length |
| `MOVING_AVG_Z_THRESHOLD` | 2.0 | Stored in `threshold` for MA rows |
| `MOVING_AVG_SEVERITY_THRESHOLD` | 2.3 | Severity gate |
| `MOVING_AVG_MODEL_VERSION` | moving_avg_k6_v1 | `model_version` column |

For a quick local check that does not connect to TimescaleDB or insert rows, run:

```bash
bash scripts/run-local-outbreak-smoke.sh
```

## Current Training Data Status

The latest retraining used synthetic pipeline-generated data from TimescaleDB.

Current training view summary:

- 30 regional hourly rows
- 5 regions
- bucket range: `2026-04-29 12:00:00+00` to `2026-04-30 18:00:00+00`
- 23 synthetic normal examples
- 7 synthetic outbreak examples

Because the dataset is small and labels are synthetic, the model should be described as a proof-of-concept outbreak prediction workflow, not a production-quality predictor.

## Output

Batch predictions are written to:

```text
ml_outbreak_predictions
```

The prediction table stores:

- `prediction_timestamp`
- `bucket_start`
- `region`
- `model_version`
- `risk_score`
- `predicted_label`
- `threshold`
- `run_id`

The scoring script is:

```text
ml/predict.py
```

It loads `ml/model.pkl`, scores recent rows from `vw_region_hourly_ml_features_windowed`, converts model probability into `high_risk` or `normal` using a threshold of `0.5` (for the Random Forest), runs the moving-average detector on the same rows, and inserts **two rows per bucket per region** (one per method) into `ml_outbreak_predictions`, sharing the same `run_id`.

## Current Moving Average Smoke Test

We also ran the moving-average detector against the current TimescaleDB feature view using the live port-forwarded database connection. The test read 30 regional hourly feature rows from `vw_region_hourly_ml_features_windowed`, covering `2026-04-29 12:00:00+00` through `2026-04-30 18:00:00+00`.

The real data had `total_case_events` ranging from 156 to 2367 per region-hour bucket. The moving-average detector flagged the latest high-volume `2026-04-30 18:00:00+00` bucket across all five regions, where counts rose from recent baselines around 476-499 to roughly 2139-2367. This looked like the intended behavior: the detector responded to sudden recent spikes while ignoring smaller elevated-but-less-spiky buckets.

## Mini Prediction Run (outdated, see above)

The latest scoring run successfully wrote 20 predictions to TimescaleDB.

Example run ID:

```text
a9a4cd63-7b58-492b-8fa6-60d501e8577b
```

The outputs were logically consistent with the synthetic label rule. Region-hours that met the elevated volume and severity rule received high risk scores, while rows below the severity threshold were predicted as normal.

## Vaccination Change Order Impact

CO-2026-01 added `vaccination_record` events to the pipeline. These records are routed to:

```text
ds551-s26.team02.vaccination_records
```

Spark aggregates them into:

```text
feature_vaccination_hourly
```

TimescaleDB exposes them through:

```text
analytic_vaccination_volume_by_type
```

Vaccination data is intentionally excluded from the current outbreak prediction model. The current model focuses on active outbreak signals such as symptom reports, clinic visits, hospital admissions, severity, and recent temporal trends.

Vaccination analytics are handled separately through the change-order analytic and dashboard visualization.

## Dashboard Integration

The ML pipeline writes scored predictions to `ml_outbreak_predictions`.

The dashboard can display this table as an operational view of model output, but the dashboard does not train the model or run prediction logic.

Recommended dashboard display:

- latest predictions table
- risk score by region
- predicted label: `high_risk` or `normal`
- prediction timestamp
- model version

## Serving Path

```text
Phase 1 Kafka
→ Phase 2 Spark
→ TimescaleDB feature store
→ ML batch scoring
→ ml_outbreak_predictions
→ dashboard / alerting consumers
```

## Limitations

- The data is synthetic course-generated data.
- The labels are synthetic heuristic labels.
- The current training set is small.
- The model demonstrates an operational ML pipeline, not a clinically validated public-health predictor.