import os
import joblib
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL is not set")

MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.pkl")

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
"""

def add_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["has_outbreak"] = (
        (df["total_case_events"] >= 50) &
        (df["avg_severity"].fillna(0) >= 2.3)
    ).astype(int)
    return df

def main() -> None:
    engine = create_engine(DB_URL)
    df = pd.read_sql(QUERY, engine)
    df = add_labels(df)

    feature_cols = [
        "avg_severity",
        "total_case_events",
        "symptom_count",
        "admission_count",
        "clinic_visit_count",
        "admission_share",
        "cases_prev_6h",
        "cases_prev_24h",
        "severity_prev_6h_avg",
        "severity_prev_24h_avg",
        "total_case_events_lag_6h",
        "total_case_events_lag_24h",
    ]

    X = df[feature_cols].fillna(0)
    y = df["has_outbreak"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    print(classification_report(y_test, preds))

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump({"model": model, "feature_cols": feature_cols}, MODEL_PATH)
    print(f"Saved model to {MODEL_PATH}")

if __name__ == "__main__":
    main()