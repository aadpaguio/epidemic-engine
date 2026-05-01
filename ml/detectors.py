"""Time-series outbreak detectors that work on regional hourly buckets.

Each detector walks forward by ``bucket_start`` within each region. Baselines use
prior buckets where appropriate so the current bucket does not dilute its own
outbreak signal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {"region", "bucket_start", "total_case_events", "avg_severity"}


def _prepare_detector_input(df: pd.DataFrame, detector_name: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"{detector_name}: missing columns {missing}")

    prepared = df.copy()
    prepared["_bucket_start"] = pd.to_datetime(prepared["bucket_start"], utc=True)
    return prepared


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
    out_frames: list[pd.DataFrame] = []
    df = _prepare_detector_input(df, "moving_average_detector")
    if df.empty:
        return df

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


def ewma_detector(
    df: pd.DataFrame,
    *,
    alpha: float = 0.35,
    min_periods: int = 2,
    z_threshold: float = 2.0,
    severity_threshold: float = 2.3,
    risk_scale: float = 4.0,
) -> pd.DataFrame:
    """
    Add columns per row:
    - ewma_baseline_count: EWMA of previous bucket totals (excludes current).
    - ewma_count_z: z-score of current count vs EWMA baseline.
    - ewma_risk_score: min(ewma_count_z / risk_scale, 1.0), clamped to [0, 1].
    - ewma_predicted_label: high_risk if EWMA z-score and severity are high enough.
    """
    if not 0 < alpha <= 1:
        raise ValueError("ewma_detector: alpha must be in (0, 1]")

    out_frames: list[pd.DataFrame] = []
    df = _prepare_detector_input(df, "ewma_detector")
    if df.empty:
        return df

    for _, group in df.groupby("region", sort=False):
        g = group.sort_values("_bucket_start").copy()
        counts = g["total_case_events"].astype(float)
        prior_counts = counts.shift(1)
        baseline = prior_counts.ewm(
            alpha=alpha, adjust=False, min_periods=min_periods
        ).mean()
        std = prior_counts.ewm(alpha=alpha, adjust=False, min_periods=min_periods).std()
        std = std.replace(0, np.nan).fillna(1.0)
        z = (counts - baseline) / std
        risk = (z / risk_scale).clip(lower=0.0, upper=1.0)
        sev = g["avg_severity"].fillna(0).astype(float)
        high = (z >= z_threshold) & (sev >= severity_threshold)
        g["ewma_baseline_count"] = baseline
        g["ewma_count_z"] = z
        g["ewma_risk_score"] = risk
        g["ewma_predicted_label"] = np.where(high, "high_risk", "normal")
        out_frames.append(g.drop(columns=["_bucket_start"]))

    return pd.concat(out_frames, ignore_index=True)


def kalman_detector(
    df: pd.DataFrame,
    *,
    process_var: float = 0.05,
    observation_var: float = 0.2,
    z_threshold: float = 2.0,
    severity_threshold: float = 2.3,
    risk_scale: float = 4.0,
) -> pd.DataFrame:
    """
    Add columns per row using a simple one-dimensional Kalman filter per region.

    The observation is log1p(total_case_events), which keeps large count buckets
    numerically stable. `kf_innovation_z` measures how surprising the current
    bucket is versus the prior filtered state.
    """
    if process_var <= 0:
        raise ValueError("kalman_detector: process_var must be positive")
    if observation_var <= 0:
        raise ValueError("kalman_detector: observation_var must be positive")

    out_frames: list[pd.DataFrame] = []
    df = _prepare_detector_input(df, "kalman_detector")
    if df.empty:
        return df

    for _, group in df.groupby("region", sort=False):
        g = group.sort_values("_bucket_start").copy()
        observations = np.log1p(g["total_case_events"].astype(float).clip(lower=0.0))
        states: list[float] = []
        innovation_zs: list[float] = []
        risks: list[float] = []

        state: float | None = None
        covariance = 1.0

        for obs in observations:
            obs = float(obs)
            if state is None:
                state = obs
                covariance = observation_var
                innovation_z = 0.0
            else:
                predicted_state = state
                predicted_covariance = covariance + process_var
                innovation = obs - predicted_state
                innovation_variance = predicted_covariance + observation_var
                innovation_z = innovation / float(np.sqrt(innovation_variance))
                gain = predicted_covariance / innovation_variance
                state = predicted_state + gain * innovation
                covariance = (1 - gain) * predicted_covariance

            states.append(state)
            innovation_zs.append(innovation_z)
            risks.append(float(np.clip(innovation_z / risk_scale, 0.0, 1.0)))

        sev = g["avg_severity"].fillna(0).astype(float)
        z_series = pd.Series(innovation_zs, index=g.index)
        high = (z_series >= z_threshold) & (sev >= severity_threshold)
        g["kf_state"] = states
        g["kf_innovation_z"] = innovation_zs
        g["kf_risk_score"] = risks
        g["kf_predicted_label"] = np.where(high, "high_risk", "normal")
        out_frames.append(g.drop(columns=["_bucket_start"]))

    return pd.concat(out_frames, ignore_index=True)
