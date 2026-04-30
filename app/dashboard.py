import pandas as pd
import streamlit as st
import re

from components import (
    area_total_cases,
    bar_capacity_pressure,
    line_diagnosis_temperature,
    line_event_volume,
    line_follow_up_rates,
    line_regional_severity,
    line_vaccination_volume,
    metric_cards,
)
from db import render_connection_status
from sql_queries import (
    get_diagnosis_temperature,
    get_event_volume,
    get_follow_up_rates,
    get_hospital_pressure,
    get_regions,
    get_regional_trends,
    get_vaccination_volume,
)


st.set_page_config(page_title="Epidemic Engine Dashboard", layout="wide")
st.title("Epidemic Engine: Phase 3 Streamlit Mockup")
st.caption("Layer B analytics from TimescaleDB (local port-forward mode).")


def _latest_bucket_start(dfs: list[pd.DataFrame]):
    latest = None
    for df in dfs:
        if df is None or df.empty or "bucket_start" not in df.columns:
            continue
        series = pd.to_datetime(df["bucket_start"], utc=True, errors="coerce").dropna()
        if series.empty:
            continue
        candidate = series.max()
        if latest is None or candidate > latest:
            latest = candidate
    return latest


def _latest_processing_metadata(dfs: list[pd.DataFrame]):
    latest_ts = None
    latest_batch = None
    for df in dfs:
        if df is None or df.empty:
            continue

        ts_col = None
        if "run_timestamp" in df.columns:
            ts_col = "run_timestamp"
        elif "as_of_run_timestamp" in df.columns:
            ts_col = "as_of_run_timestamp"

        if ts_col is None:
            continue

        ts_series = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        if ts_series.dropna().empty:
            continue

        idx = ts_series.idxmax()
        candidate_ts = ts_series.loc[idx]
        candidate_batch = None
        if "batch_id" in df.columns and idx in df.index:
            candidate_batch = df.loc[idx, "batch_id"]

        if latest_ts is None or candidate_ts > latest_ts:
            latest_ts = candidate_ts
            latest_batch = candidate_batch

    return latest_ts, latest_batch


def _parse_batch_id(batch_id: str):
    # Expected pattern example:
    # eventtime-2026-04-30T00-20260430T131838Z
    m = re.match(
        r"^eventtime-(\d{4}-\d{2}-\d{2}T\d{2})-(\d{8}T\d{6}Z)$",
        str(batch_id),
    )
    if not m:
        return None, None
    window_start_hour = m.group(1)
    run_stamp_raw = m.group(2)
    run_stamp = pd.to_datetime(run_stamp_raw, format="%Y%m%dT%H%M%SZ", utc=True, errors="coerce")
    run_stamp_text = None if pd.isna(run_stamp) else run_stamp.strftime("%Y-%m-%d %H:%M:%S UTC")
    return window_start_hour, run_stamp_text


with st.sidebar:
    st.header("Controls")
    page = st.radio(
        "Page",
        [
            "Overview",
            "Regional Trends",
            "Hospital & Clinic Operations",
            "Clinical Signals",
        ],
    )
    hours = st.slider("Time Window (hours)", min_value=6, max_value=168, value=72, step=6)
    if st.button("Refresh Data"):
        st.cache_data.clear()


if not render_connection_status():
    st.stop()


event_volume = get_event_volume(hours=hours)
regional = get_regional_trends(hours=hours)
hospital_pressure = get_hospital_pressure(hours=min(hours, 48))
follow_up = get_follow_up_rates(hours=min(hours, 48))
diagnosis = get_diagnosis_temperature(hours=min(hours, 72))
vaccination = get_vaccination_volume(hours=hours)

latest_bucket = _latest_bucket_start(
    [event_volume, regional, hospital_pressure, follow_up, diagnosis, vaccination]
)
latest_proc_ts, latest_batch_id = _latest_processing_metadata(
    [event_volume, regional, hospital_pressure, follow_up, diagnosis, vaccination]
)
if latest_proc_ts is not None:
    message = f"As of processing run (UTC): {latest_proc_ts.strftime('%Y-%m-%d %H:%M:%S')}"
    if latest_batch_id:
        parsed_window, parsed_run = _parse_batch_id(str(latest_batch_id))
        if parsed_window:
            message += f" | batch window: {parsed_window}:00 UTC"
        if parsed_run:
            message += f" | batch created: {parsed_run}"
        message += f" | batch_id: {latest_batch_id}"
    if latest_bucket is not None:
        message += f" | latest bucket: {latest_bucket.strftime('%Y-%m-%d %H:%M')}"
    st.info(message)
elif latest_bucket is not None:
    st.info(f"As of bucket (UTC): {latest_bucket.strftime('%Y-%m-%d %H:%M')}")
else:
    st.warning("As of: no data returned for the selected time window.")


if page == "Overview":
    st.subheader("Overview")
    latest_events = 0
    avg_severity = None
    max_pressure = None
    follow_up_latest = None

    if not event_volume.empty:
        latest = event_volume.sort_values("bucket_start").iloc[-1]
        latest_events = int(
            latest["symptom_report_volume"]
            + latest["hospital_admission_volume"]
            + latest["clinic_visit_volume"]
        )

    if not regional.empty:
        latest_bucket = regional["bucket_start"].max()
        sev = regional.loc[regional["bucket_start"] == latest_bucket, "avg_severity"].dropna()
        if not sev.empty:
            avg_severity = float(sev.mean())

    if not hospital_pressure.empty:
        pressure = hospital_pressure["capacity_pressure_index"].dropna()
        if not pressure.empty:
            max_pressure = float(pressure.max())

    if not follow_up.empty:
        latest_bucket = follow_up["bucket_start"].max()
        rate = follow_up.loc[follow_up["bucket_start"] == latest_bucket, "follow_up_rate"].dropna()
        if not rate.empty:
            follow_up_latest = float(rate.mean())

    metric_cards(latest_events, avg_severity, max_pressure, follow_up_latest)
    line_event_volume(event_volume)

elif page == "Regional Trends":
    st.subheader("Regional Trends")
    region_options = ["ALL"] + get_regions()
    region = st.selectbox("Region", region_options, index=0)
    regional_filtered = get_regional_trends(hours=hours, region=region)
    line_regional_severity(regional_filtered)
    area_total_cases(regional_filtered)
    st.dataframe(regional_filtered, use_container_width=True)

elif page == "Hospital & Clinic Operations":
    st.subheader("Hospital & Clinic Operations")
    bar_capacity_pressure(hospital_pressure)
    line_follow_up_rates(follow_up)

else:
    st.subheader("Clinical Signals")
    line_diagnosis_temperature(diagnosis)
    st.markdown("#### Vaccination Signals (CO-2026-01)")
    line_vaccination_volume(vaccination)
