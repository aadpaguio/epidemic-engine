import os
import uuid
import joblib
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL is not set")
    
MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
THRESHOLD = float(os.getenv("THRESHOLD", "0.5"))

QUERY = """
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
WHERE bucket_start >= now() - interval '24 hours'
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

    X = df[feature_cols].fillna(0)

    if hasattr(model, "predict_proba"):
        scores = model.predict_proba(X)[:, 1]
    else:
        scores = model.predict(X)

    labels = ["high_risk" if s >= THRESHOLD else "normal" for s in scores]
    run_id = str(uuid.uuid4())

    rows = []
    for (_, row), score, label in zip(df.iterrows(), scores, labels):
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

    with engine.begin() as conn:
        for r in rows:
            conn.execute(text(INSERT_SQL), r)

    print(f"Wrote {len(rows)} predictions with run_id={run_id}")

if __name__ == "__main__":
    main()