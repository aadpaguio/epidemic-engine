# ML Model Guide

## Goal
Predict outbreak risk by region using Phase 2 analytics stored in TimescaleDB.

## Input Data
The model reads from:
- `vw_region_hourly_ml_features`
- `vw_region_hourly_ml_features_windowed`

These views are built on top of the Phase 2 feature store.

## Candidate Features
- total_case_events
- avg_severity
- symptom_count
- admission_count
- clinic_visit_count
- admission_share
- cases_prev_6h
- cases_prev_24h
- severity_prev_6h_avg
- severity_prev_24h_avg
- total_case_events_lag_6h
- total_case_events_lag_24h

## Labeling Strategy
Synthetic outbreak labels will be generated from historical analytics windows.
Initial baseline rule:
- outbreak = 1 when event volume and severity are both elevated
- outbreak = 0 otherwise

This rule may be refined after exploratory analysis.

## Model Candidates
- Logistic Regression
- Random Forest

## Output
Batch predictions are written to `ml_outbreak_predictions`.

## Serving Path
Phase 1 Kafka -> Phase 2 Spark -> TimescaleDB feature store -> ML batch scoring -> predictions table -> alerting/dashboard consumers