"""
Time-series outbreak detectors that work on regional hourly buckets.

Moving-average path uses a trailing mean of *previous* k buckets (not including
the current bucket) so a spike is not smoothed into its own baseline.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def moving_average_detector(
    df: pd.DataFrame,
    *,
    k: int = 6,
    min_periods: int = 2,
    z_threshold: float = 2.0,
    severity_threshold: float = 2.3,
    risk_scale: float = 4.0,
) -> pd.DataFrame:
    """
    Add columns per row:
    - ma_baseline_count: mean of previous k bucket totals (excludes current).
    - ma_count_z: z-score of current count vs trailing window (std of previous k counts).
    - ma_risk_score: min(ma_count_z / risk_scale, 1.0), clamped to [0, 1].
    - ma_predicted_label: high_risk if ma_count_z >= z_threshold and severity high enough.
    """
    if df.empty:
        return df.copy()

    required = {"region", "bucket_start", "total_case_events", "avg_severity"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"moving_average_detector: missing columns {missing}")

    out_frames: list[pd.DataFrame] = []
    df = df.copy()
    df["_bucket_start"] = pd.to_datetime(df["bucket_start"], utc=True)

    for _, group in df.groupby("region", sort=False):
        g = group.sort_values("_bucket_start").copy()
        counts = g["total_case_events"].astype(float)
        baseline = counts.shift(1).rolling(window=k, min_periods=min_periods).mean()
        std = counts.shift(1).rolling(window=k, min_periods=min_periods).std()
        std = std.replace(0, np.nan).fillna(1.0)
        residual = counts - baseline
        z = residual / std
        risk = (z / risk_scale).clip(lower=0.0, upper=1.0)
        sev = g["avg_severity"].fillna(0).astype(float)
        high = (z >= z_threshold) & (sev >= severity_threshold)
        g["ma_baseline_count"] = baseline
        g["ma_count_z"] = z
        g["ma_risk_score"] = risk
        g["ma_predicted_label"] = np.where(high, "high_risk", "normal")
        out_frames.append(g.drop(columns=["_bucket_start"]))

    result = pd.concat(out_frames, ignore_index=True)
    return result
