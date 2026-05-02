CREATE OR REPLACE VIEW vw_region_hourly_ml_features AS
SELECT
    bucket_start,
    region,
    avg_severity,
    total_case_events,
    symptom_count,
    admission_count,
    clinic_visit_count,
    CASE
        WHEN total_case_events > 0 THEN admission_count::double precision / total_case_events
        ELSE 0.0
    END AS admission_share
FROM analytic_regional_disease_severity_trend;

CREATE OR REPLACE VIEW vw_region_hourly_ml_features_windowed AS
SELECT
    bucket_start,
    region,
    avg_severity,
    total_case_events,
    symptom_count,
    admission_count,
    clinic_visit_count,
    admission_share,
    COALESCE(
        SUM(total_case_events) OVER (
            PARTITION BY region
            ORDER BY bucket_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ), 0
    ) AS cases_prev_6h,
    COALESCE(
        SUM(total_case_events) OVER (
            PARTITION BY region
            ORDER BY bucket_start
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ), 0
    ) AS cases_prev_24h,
    AVG(avg_severity) OVER (
        PARTITION BY region
        ORDER BY bucket_start
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS severity_prev_6h_avg,
    AVG(avg_severity) OVER (
        PARTITION BY region
        ORDER BY bucket_start
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS severity_prev_24h_avg,
    LAG(total_case_events, 6) OVER (
        PARTITION BY region
        ORDER BY bucket_start
    ) AS total_case_events_lag_6h,
    LAG(total_case_events, 24) OVER (
        PARTITION BY region
        ORDER BY bucket_start
    ) AS total_case_events_lag_24h
FROM vw_region_hourly_ml_features;

CREATE TABLE IF NOT EXISTS ml_outbreak_predictions (
    prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    bucket_start         TIMESTAMPTZ NOT NULL,
    region               TEXT        NOT NULL,
    model_version        TEXT        NOT NULL,
    risk_score           DOUBLE PRECISION,
    predicted_label      TEXT,
    threshold            DOUBLE PRECISION,
    run_id               TEXT,
    PRIMARY KEY (prediction_timestamp, bucket_start, region, model_version)
);

CREATE INDEX IF NOT EXISTS idx_ml_outbreak_predictions_region_bucket
    ON ml_outbreak_predictions (region, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_ml_outbreak_predictions_label_time
    ON ml_outbreak_predictions (predicted_label, prediction_timestamp DESC);

CREATE TABLE IF NOT EXISTS alert_events (
    alert_key      TEXT PRIMARY KEY,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    alert_type     TEXT        NOT NULL,
    severity       TEXT        NOT NULL,
    region         TEXT,
    bucket_start   TIMESTAMPTZ,
    model_versions TEXT[],
    max_risk_score DOUBLE PRECISION,
    run_id         TEXT,
    status         TEXT        NOT NULL DEFAULT 'active',
    message        TEXT,
    raw_payload    JSONB
);

CREATE INDEX IF NOT EXISTS idx_alert_events_updated
    ON alert_events (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_status
    ON alert_events (status, updated_at DESC);