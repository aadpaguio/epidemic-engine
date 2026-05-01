import os
import uuid
import joblib
import pandas as pd
from sqlalchemy import create_engine, text

from ml.detectors import ewma_detector, kalman_detector, moving_average_detector

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL is not set")

MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
THRESHOLD = float(os.getenv("THRESHOLD", "0.5"))

LOOKBACK_HOURS = int(os.getenv("ML_LOOKBACK_HOURS", "72"))
OUTPUT_WINDOW_HOURS = int(os.getenv("ML_OUTPUT_WINDOW_HOURS", "24"))

MOVING_AVG_K = int(os.getenv("MOVING_AVG_K", "6"))
MOVING_AVG_MIN_PERIODS = int(os.getenv("MOVING_AVG_MIN_PERIODS", "2"))
MOVING_AVG_Z_THRESHOLD = float(os.getenv("MOVING_AVG_Z_THRESHOLD", "2.0"))
MOVING_AVG_SEVERITY_THRESHOLD = float(os.getenv("MOVING_AVG_SEVERITY_THRESHOLD", "2.3"))
MOVING_AVG_MODEL_VERSION = os.getenv("MOVING_AVG_MODEL_VERSION", "moving_avg_k6_v1")

EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.35"))
EWMA_MIN_PERIODS = int(os.getenv("EWMA_MIN_PERIODS", "2"))
EWMA_Z_THRESHOLD = float(os.getenv("EWMA_Z_THRESHOLD", "2.0"))
EWMA_SEVERITY_THRESHOLD = float(os.getenv("EWMA_SEVERITY_THRESHOLD", "2.3"))
EWMA_MODEL_VERSION = os.getenv("EWMA_MODEL_VERSION", "ewma_v1")

KALMAN_PROCESS_VAR = float(os.getenv("KALMAN_PROCESS_VAR", "0.05"))
KALMAN_OBSERVATION_VAR = float(os.getenv("KALMAN_OBSERVATION_VAR", "0.2"))
KALMAN_Z_THRESHOLD = float(os.getenv("KALMAN_Z_THRESHOLD", "2.0"))
KALMAN_SEVERITY_THRESHOLD = float(os.getenv("KALMAN_SEVERITY_THRESHOLD", "2.3"))
KALMAN_MODEL_VERSION = os.getenv("KALMAN_MODEL_VERSION", "kalman_v1")

QUERY = f"""
SELECT
    bucket_start,
    region,
    avg_severity,
    total_case_events,
    symptom_count,
    admission_count,
    clinic_visit_count,
    admission_share,
    cases_prev_6h,
    cases_prev_24h,
    severity_prev_6h_avg,
    severity_prev_24h_avg,
    COALESCE(total_case_events_lag_6h, 0) AS total_case_events_lag_6h,
    COALESCE(total_case_events_lag_24h, 0) AS total_case_events_lag_24h
FROM vw_region_hourly_ml_features_windowed
WHERE bucket_start >= now() - interval '{LOOKBACK_HOURS} hours'
"""

INSERT_SQL = """
INSERT INTO ml_outbreak_predictions
(prediction_timestamp, bucket_start, region, model_version, risk_score, predicted_label, threshold, run_id)
VALUES
(now(), :bucket_start, :region, :model_version, :risk_score, :predicted_label, :threshold, :run_id)
"""


def main() -> None:
    engine = create_engine(DB_URL)
    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    feature_cols = bundle["feature_cols"]

    df = pd.read_sql(QUERY, engine)
    if df.empty:
        print("No rows to score.")
        return

    df["bucket_start"] = pd.to_datetime(df["bucket_start"], utc=True)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=OUTPUT_WINDOW_HOURS)
    df_scored = moving_average_detector(
        df,
        k=MOVING_AVG_K,
        min_periods=MOVING_AVG_MIN_PERIODS,
        z_threshold=MOVING_AVG_Z_THRESHOLD,
        severity_threshold=MOVING_AVG_SEVERITY_THRESHOLD,
    )
    df_scored = ewma_detector(
        df_scored,
        alpha=EWMA_ALPHA,
        min_periods=EWMA_MIN_PERIODS,
        z_threshold=EWMA_Z_THRESHOLD,
        severity_threshold=EWMA_SEVERITY_THRESHOLD,
    )
    df_scored = kalman_detector(
        df_scored,
        process_var=KALMAN_PROCESS_VAR,
        observation_var=KALMAN_OBSERVATION_VAR,
        z_threshold=KALMAN_Z_THRESHOLD,
        severity_threshold=KALMAN_SEVERITY_THRESHOLD,
    )
    df_out = df_scored[df_scored["bucket_start"] >= cutoff].copy()
    if df_out.empty:
        print("No rows in output window.")
        return

    X = df_out[feature_cols].fillna(0)

    if hasattr(model, "predict_proba"):
        scores = model.predict_proba(X)[:, 1]
    else:
        scores = model.predict(X)

    labels = ["high_risk" if s >= THRESHOLD else "normal" for s in scores]
    run_id = str(uuid.uuid4())

    rows = []
    for (_, row), score, label in zip(df_out.iterrows(), scores, labels):
        rows.append(
            {
                "bucket_start": row["bucket_start"],
                "region": row["region"],
                "model_version": MODEL_VERSION,
                "risk_score": float(score),
                "predicted_label": label,
                "threshold": THRESHOLD,
                "run_id": run_id,
            }
        )
        rows.append(
            {
                "bucket_start": row["bucket_start"],
                "region": row["region"],
                "model_version": MOVING_AVG_MODEL_VERSION,
                "risk_score": float(row["ma_risk_score"]),
                "predicted_label": str(row["ma_predicted_label"]),
                "threshold": MOVING_AVG_Z_THRESHOLD,
                "run_id": run_id,
            }
        )
        rows.append(
            {
                "bucket_start": row["bucket_start"],
                "region": row["region"],
                "model_version": EWMA_MODEL_VERSION,
                "risk_score": float(row["ewma_risk_score"]),
                "predicted_label": str(row["ewma_predicted_label"]),
                "threshold": EWMA_Z_THRESHOLD,
                "run_id": run_id,
            }
        )
        rows.append(
            {
                "bucket_start": row["bucket_start"],
                "region": row["region"],
                "model_version": KALMAN_MODEL_VERSION,
                "risk_score": float(row["kf_risk_score"]),
                "predicted_label": str(row["kf_predicted_label"]),
                "threshold": KALMAN_Z_THRESHOLD,
                "run_id": run_id,
            }
        )

    with engine.begin() as conn:
        for r in rows:
            conn.execute(text(INSERT_SQL), r)

    print(
        f"Wrote {len(rows)} prediction row(s) for {len(df_out)} bucket-region row(s) "
        f"(random_forest + moving_avg + ewma + kalman) run_id={run_id}"
    )


if __name__ == "__main__":
    main()
