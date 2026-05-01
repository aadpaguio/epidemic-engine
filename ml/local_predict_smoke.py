"""Local smoke test for outbreak scoring.

This does not connect to TimescaleDB or write predictions. It loads the saved
RandomForest bundle and runs the moving-average detector over synthetic hourly
regional buckets so the scoring path can be checked quickly from a laptop.

Run from the repo root:
    python ml/local_predict_smoke.py
"""

from __future__ import annotations

import argparse

import joblib
import pandas as pd

from ml.detectors import moving_average_detector


def build_sample_features() -> pd.DataFrame:
    base_ts = pd.Timestamp("2026-04-30T00:00:00Z")
    rows = []

    for region in ["Boston", "Cambridge"]:
        for i in range(8):
            is_spike = region == "Boston" and i == 7
            total = 90 if is_spike else 12 + i
            severity = 3.4 if is_spike else 1.8
            symptom_count = int(total * 0.6)
            admission_count = int(total * 0.25)
            clinic_visit_count = total - symptom_count - admission_count

            rows.append(
                {
                    "bucket_start": base_ts + pd.Timedelta(hours=i),
                    "region": region,
                    "avg_severity": severity,
                    "total_case_events": total,
                    "symptom_count": symptom_count,
                    "admission_count": admission_count,
                    "clinic_visit_count": clinic_visit_count,
                    "admission_share": admission_count / total if total else 0.0,
                    "cases_prev_6h": total * 6,
                    "cases_prev_24h": total * 8,
                    "severity_prev_6h_avg": severity,
                    "severity_prev_24h_avg": severity,
                    "total_case_events_lag_6h": 12,
                    "total_case_events_lag_24h": 10,
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local outbreak scoring smoke test.")
    parser.add_argument("--model-path", default="ml/model.pkl")
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--moving-avg-k", type=int, default=6)
    parser.add_argument("--moving-avg-z-threshold", type=float, default=2.0)
    parser.add_argument("--severity-threshold", type=float, default=2.3)
    args = parser.parse_args()

    bundle = joblib.load(args.model_path)
    model = bundle["model"]
    feature_cols = bundle["feature_cols"]

    df = build_sample_features()
    df = moving_average_detector(
        df,
        k=args.moving_avg_k,
        z_threshold=args.moving_avg_z_threshold,
        severity_threshold=args.severity_threshold,
    )

    x = df[feature_cols].fillna(0)
    if hasattr(model, "predict_proba"):
        rf_scores = model.predict_proba(x)[:, 1]
    else:
        rf_scores = model.predict(x)

    df["rf_risk_score"] = rf_scores
    df["rf_predicted_label"] = [
        "high_risk" if score >= args.threshold else "normal" for score in rf_scores
    ]

    display_cols = [
        "bucket_start",
        "region",
        "total_case_events",
        "avg_severity",
        "rf_risk_score",
        "rf_predicted_label",
        "ma_baseline_count",
        "ma_count_z",
        "ma_risk_score",
        "ma_predicted_label",
    ]

    df_display = df.sort_values(["bucket_start", "region"]).copy()
    high_risk = df_display[
        (df_display["rf_predicted_label"] == "high_risk")
        | (df_display["ma_predicted_label"] == "high_risk")
    ]

    print("\nLocal outbreak scoring smoke test")
    print(f"model_path={args.model_path}")
    print(f"rows_scored={len(df)}")
    print(
        "high_risk_rows="
        f"{len(high_risk)} "
        "(flagged by RandomForest or moving-average detector)"
    )
    print(df_display[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
