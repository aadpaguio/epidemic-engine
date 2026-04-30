-- TimescaleDB Phase 2 feature store: canonical hourly facts (Layer A) and derived analytics (Layer B).
-- Executed on first database init via /docker-entrypoint-initdb.d/ (see infra/database-deployment.yaml).
-- JDBC target: jdbc:postgresql://timescaledb-service:5432/epidemic
-- After editing this file, refresh the embedded ConfigMap in database-deployment.yaml, e.g.:
--   kubectl create configmap timescaledb-init-sql --from-file=database-init.sql=infra/database-init.sql -o yaml --dry-run=client
--   (merge the data: block into the ConfigMap document, preserving kind/metadata order.)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Layer A: canonical Spark-written hourly fact hypertables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS feature_region_hourly (
    bucket_start        TIMESTAMPTZ NOT NULL,
    region              TEXT        NOT NULL,
    batch_id            TEXT        NOT NULL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now(),
    symptom_count       BIGINT      NOT NULL DEFAULT 0,
    admission_count     BIGINT      NOT NULL DEFAULT 0,
    clinic_visit_count  BIGINT      NOT NULL DEFAULT 0,
    severity_sum        DOUBLE PRECISION,
    severity_count      BIGINT      NOT NULL DEFAULT 0,
    duration_days_sum   DOUBLE PRECISION,
    duration_days_count BIGINT      NOT NULL DEFAULT 0,
    temperature_sum     DOUBLE PRECISION,
    temperature_count   BIGINT      NOT NULL DEFAULT 0,
    available_beds_min  DOUBLE PRECISION,
    available_beds_avg  DOUBLE PRECISION,
    available_beds_last DOUBLE PRECISION,
    available_beds_sample_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, region, batch_id)
);

COMMENT ON TABLE feature_region_hourly IS 'Hourly aggregates by region; Spark appends rows per batch_id.';
COMMENT ON COLUMN feature_region_hourly.bucket_start IS 'UTC start of hour bucket [bucket_start, bucket_start + 1h).';
COMMENT ON COLUMN feature_region_hourly.available_beds_min IS 'Gauge rollup: min sampled available_beds in bucket (not additive).';

CREATE TABLE IF NOT EXISTS feature_clinic_hourly (
    bucket_start        TIMESTAMPTZ NOT NULL,
    clinic_id           TEXT        NOT NULL,
    batch_id            TEXT        NOT NULL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now(),
    visit_count         BIGINT      NOT NULL DEFAULT 0,
    follow_up_true_count BIGINT     NOT NULL DEFAULT 0,
    temperature_sum     DOUBLE PRECISION,
    temperature_count   BIGINT      NOT NULL DEFAULT 0,
    available_beds_min  DOUBLE PRECISION,
    available_beds_avg  DOUBLE PRECISION,
    available_beds_last DOUBLE PRECISION,
    available_beds_sample_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, clinic_id, batch_id)
);

COMMENT ON TABLE feature_clinic_hourly IS 'Hourly clinic visit facts for follow-up and temperature analytics.';

CREATE TABLE IF NOT EXISTS feature_hospital_hourly (
    bucket_start        TIMESTAMPTZ NOT NULL,
    hospital_id         TEXT        NOT NULL,
    batch_id            TEXT        NOT NULL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now(),
    admission_count     BIGINT      NOT NULL DEFAULT 0,
    severity_sum        DOUBLE PRECISION,
    severity_count      BIGINT      NOT NULL DEFAULT 0,
    temperature_sum     DOUBLE PRECISION,
    temperature_count   BIGINT      NOT NULL DEFAULT 0,
    oxygen_sum          DOUBLE PRECISION,
    oxygen_count        BIGINT      NOT NULL DEFAULT 0,
    expected_los_sum    DOUBLE PRECISION,
    expected_los_count  BIGINT      NOT NULL DEFAULT 0,
    available_beds_min  DOUBLE PRECISION,
    available_beds_avg  DOUBLE PRECISION,
    available_beds_last DOUBLE PRECISION,
    available_beds_sample_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, hospital_id, batch_id)
);

COMMENT ON TABLE feature_hospital_hourly IS 'Hourly hospital admission and capacity gauge facts.';

CREATE TABLE IF NOT EXISTS feature_diagnosis_hourly (
    bucket_start        TIMESTAMPTZ NOT NULL,
    clinic_id           TEXT        NOT NULL,
    diagnosis_code      TEXT        NOT NULL,
    batch_id            TEXT        NOT NULL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now(),
    diagnosis_visit_count BIGINT    NOT NULL DEFAULT 0,
    temperature_sum     DOUBLE PRECISION,
    temperature_count   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, clinic_id, diagnosis_code, batch_id)
);

COMMENT ON TABLE feature_diagnosis_hourly IS 'Sparse diagnosis-level hourly facts for temperature-by-diagnosis analytics.';

CREATE TABLE IF NOT EXISTS feature_symptom_hourly (
    bucket_start        TIMESTAMPTZ NOT NULL,
    region              TEXT        NOT NULL,
    symptom_token       TEXT        NOT NULL,
    batch_id            TEXT        NOT NULL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now(),
    symptom_token_count BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, region, symptom_token, batch_id)
);

COMMENT ON TABLE feature_symptom_hourly IS 'Exploded symptom token counts per region and hour.';

CREATE TABLE IF NOT EXISTS feature_vaccination_hourly (
    bucket_start         TIMESTAMPTZ NOT NULL,
    region               TEXT        NOT NULL,
    vaccine_type         TEXT        NOT NULL,
    dose_number          INTEGER     NOT NULL,
    batch_id             TEXT        NOT NULL,
    run_timestamp        TIMESTAMPTZ NOT NULL DEFAULT now(),
    vaccination_count    BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, region, vaccine_type, dose_number, batch_id)
);

COMMENT ON TABLE feature_vaccination_hourly IS 'CO-2026-01: hourly vaccination counts by region, vaccine_type, and dose_number.';

-- Hypertables (time column bucket_start)
SELECT public.create_hypertable(
    'feature_region_hourly',
    'bucket_start',
    if_not_exists => TRUE
);
SELECT public.create_hypertable(
    'feature_clinic_hourly',
    'bucket_start',
    if_not_exists => TRUE
);
SELECT public.create_hypertable(
    'feature_hospital_hourly',
    'bucket_start',
    if_not_exists => TRUE
);
SELECT public.create_hypertable(
    'feature_diagnosis_hourly',
    'bucket_start',
    if_not_exists => TRUE
);
SELECT public.create_hypertable(
    'feature_symptom_hourly',
    'bucket_start',
    if_not_exists => TRUE
);
SELECT public.create_hypertable(
    'feature_vaccination_hourly',
    'bucket_start',
    if_not_exists => TRUE
);

-- Indexes for Phase 3 query patterns (composite + diagnosis)
CREATE INDEX IF NOT EXISTS idx_feature_region_hourly_region_bucket
    ON feature_region_hourly (region, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_feature_clinic_hourly_clinic_bucket
    ON feature_clinic_hourly (clinic_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_feature_hospital_hourly_hospital_bucket
    ON feature_hospital_hourly (hospital_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_feature_diagnosis_hourly_diagnosis
    ON feature_diagnosis_hourly (diagnosis_code, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_feature_vaccination_hourly_vaccine_bucket
    ON feature_vaccination_hourly (vaccine_type, bucket_start DESC);

-- ---------------------------------------------------------------------------
-- Layer B: six derived analytics (views on latest batch per bucket for serving;
-- Spark may append multiple batch_id rows — views filter to max(run_timestamp) per key.)
-- ---------------------------------------------------------------------------

-- Helper: latest batch row per natural key (excluding batch_id from key).
CREATE OR REPLACE VIEW v_latest_feature_region_hourly AS
SELECT DISTINCT ON (bucket_start, region)
    *
FROM feature_region_hourly
ORDER BY bucket_start, region, run_timestamp DESC, batch_id DESC;

CREATE OR REPLACE VIEW v_latest_feature_clinic_hourly AS
SELECT DISTINCT ON (bucket_start, clinic_id)
    *
FROM feature_clinic_hourly
ORDER BY bucket_start, clinic_id, run_timestamp DESC, batch_id DESC;

CREATE OR REPLACE VIEW v_latest_feature_hospital_hourly AS
SELECT DISTINCT ON (bucket_start, hospital_id)
    *
FROM feature_hospital_hourly
ORDER BY bucket_start, hospital_id, run_timestamp DESC, batch_id DESC;

CREATE OR REPLACE VIEW v_latest_feature_diagnosis_hourly AS
SELECT DISTINCT ON (bucket_start, clinic_id, diagnosis_code)
    *
FROM feature_diagnosis_hourly
ORDER BY bucket_start, clinic_id, diagnosis_code, run_timestamp DESC, batch_id DESC;

CREATE OR REPLACE VIEW v_latest_feature_vaccination_hourly AS
SELECT DISTINCT ON (bucket_start, region, vaccine_type, dose_number)
    *
FROM feature_vaccination_hourly
ORDER BY bucket_start, region, vaccine_type, dose_number, run_timestamp DESC, batch_id DESC;

-- 1) Regional disease severity trend: average severity and case volume per region per hour.
CREATE OR REPLACE VIEW analytic_regional_disease_severity_trend AS
SELECT
    bucket_start,
    region,
    CASE WHEN severity_count > 0 THEN severity_sum / severity_count END AS avg_severity,
    (symptom_count + admission_count + clinic_visit_count) AS total_case_events,
    symptom_count,
    admission_count,
    clinic_visit_count,
    batch_id,
    run_timestamp
FROM v_latest_feature_region_hourly;

COMMENT ON VIEW analytic_regional_disease_severity_trend IS
    'Rubric analytic 1: regional severity trend — avg_severity from Layer A severity_sum/count plus event counts.';

-- 2) Hospital capacity pressure index: admissions relative to last sampled bed gauge (higher = more pressure).
CREATE OR REPLACE VIEW analytic_hospital_capacity_pressure AS
SELECT
    bucket_start,
    hospital_id,
    admission_count,
    available_beds_last,
    CASE
        WHEN available_beds_last IS NULL OR available_beds_last <= 0 THEN NULL
        ELSE admission_count::double precision / available_beds_last
    END AS capacity_pressure_index,
    available_beds_min,
    available_beds_avg,
    available_beds_sample_count,
    batch_id,
    run_timestamp
FROM v_latest_feature_hospital_hourly;

COMMENT ON VIEW analytic_hospital_capacity_pressure IS
    'Rubric analytic 2: pressure_index = admission_count / available_beds_last when beds_last > 0.';

-- 3) Symptom-to-admission escalation rate per region and hour.
CREATE OR REPLACE VIEW analytic_symptom_to_admission_escalation AS
SELECT
    bucket_start,
    region,
    symptom_count,
    admission_count,
    CASE
        WHEN symptom_count IS NULL OR symptom_count = 0 THEN NULL
        ELSE admission_count::double precision / symptom_count
    END AS escalation_rate,
    batch_id,
    run_timestamp
FROM v_latest_feature_region_hourly;

COMMENT ON VIEW analytic_symptom_to_admission_escalation IS
    'Rubric analytic 3: escalation_rate = admission_count / NULLIF(symptom_count, 0).';

-- 4) Event volume by type over time (hourly), global rollup from region facts.
CREATE OR REPLACE VIEW analytic_event_volume_by_type AS
SELECT
    bucket_start,
    SUM(symptom_count) AS symptom_report_volume,
    SUM(admission_count) AS hospital_admission_volume,
    SUM(clinic_visit_count) AS clinic_visit_volume,
    MAX(run_timestamp) AS as_of_run_timestamp
FROM v_latest_feature_region_hourly
GROUP BY bucket_start;

COMMENT ON VIEW analytic_event_volume_by_type IS
    'Rubric analytic 4: hourly volumes by event family from region fact table.';

-- 5) Average patient temperature by diagnosis code (and clinic), latest batch per key.
CREATE OR REPLACE VIEW analytic_avg_temperature_by_diagnosis AS
SELECT
    bucket_start,
    clinic_id,
    diagnosis_code,
    diagnosis_visit_count,
    CASE
        WHEN temperature_count > 0 THEN temperature_sum / temperature_count
    END AS avg_temperature_f,
    batch_id,
    run_timestamp
FROM v_latest_feature_diagnosis_hourly;

COMMENT ON VIEW analytic_avg_temperature_by_diagnosis IS
    'Rubric analytic 5: avg temperature = temperature_sum / temperature_count per diagnosis bucket.';

-- 6) Follow-up requirement rate by clinic.
CREATE OR REPLACE VIEW analytic_follow_up_rate_by_clinic AS
SELECT
    bucket_start,
    clinic_id,
    visit_count,
    follow_up_true_count,
    CASE
        WHEN visit_count IS NULL OR visit_count = 0 THEN NULL
        ELSE follow_up_true_count::double precision / visit_count
    END AS follow_up_rate,
    batch_id,
    run_timestamp
FROM v_latest_feature_clinic_hourly;

COMMENT ON VIEW analytic_follow_up_rate_by_clinic IS
    'Rubric analytic 6: follow_up_rate = follow_up_true_count / NULLIF(visit_count, 0).';

-- 7) CO-2026-01: Vaccination volume by vaccine type and dose (hourly).
CREATE OR REPLACE VIEW analytic_vaccination_volume_by_type AS
SELECT
    bucket_start,
    region,
    vaccine_type,
    dose_number,
    vaccination_count,
    batch_id,
    run_timestamp
FROM v_latest_feature_vaccination_hourly;

COMMENT ON VIEW analytic_vaccination_volume_by_type IS
    'CO-2026-01 change order: hourly vaccination counts keyed by vaccine_type and dose_number.';
