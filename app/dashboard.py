import re

import pandas as pd
import streamlit as st

from components import (
    area_total_cases,
    bar_capacity_pressure,
    bar_detector_agreement,
    bar_detector_scores,
    bar_top_diagnoses,
    bar_top_vaccination_regions,
    line_detector_agreement,
    line_detector_risk,
    line_diagnosis_temperature,
    line_event_volume,
    line_follow_up_rates,
    line_regional_severity,
    line_vaccination_volume,
)
from db import render_connection_status
from sql_queries import (
    get_detector_consensus,
    get_diagnosis_temperature,
    get_event_volume,
    get_follow_up_rates,
    get_hospital_pressure,
    get_latest_prediction_run,
    get_prediction_methods,
    get_prediction_rows,
    get_regions,
    get_regional_trends,
    get_vaccination_volume,
)


st.set_page_config(page_title="Epidemic Engine Dashboard", layout="wide")
st.title("Epidemic Engine: Outbreak Situation Room")
st.caption(
    "Final operations dashboard over TimescaleDB analytics and outbreak signals. "
    "Trend visuals are ordered by event time."
)

MODEL_LABELS = {
    "v1": "Random Forest",
    "moving_avg_k6_v1": "Moving Average",
    "ewma_v1": "EWMA",
    "kalman_v1": "Kalman Filter",
}


def _coerce_bucket_start(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "bucket_start" in out.columns:
        out["bucket_start"] = pd.to_datetime(out["bucket_start"], utc=True, errors="coerce")
    return out


def _coerce_prediction_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "prediction_timestamp" in out.columns:
        out["prediction_timestamp"] = pd.to_datetime(
            out["prediction_timestamp"], utc=True, errors="coerce"
        )
    if "latest_prediction_timestamp" in out.columns:
        out["latest_prediction_timestamp"] = pd.to_datetime(
            out["latest_prediction_timestamp"], utc=True, errors="coerce"
        )
    return out


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


def _latest_bucket_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "bucket_start" not in df.columns:
        return df.iloc[0:0].copy()
    out = _coerce_bucket_start(df)
    latest_bucket = out["bucket_start"].dropna().max()
    if pd.isna(latest_bucket):
        return out.iloc[0:0].copy()
    return out.loc[out["bucket_start"] == latest_bucket].copy()


def _parse_batch_id(batch_id: str):
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


def _format_ts(ts) -> str:
    if ts is None or pd.isna(ts):
        return "N/A"
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M UTC")


def _format_float(value, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.{digits}f}"


def _format_pct(value) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.1%}"


def _pretty_model_name(model_version: str) -> str:
    return MODEL_LABELS.get(str(model_version), str(model_version))


def _label_prediction_methods(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "model_version" not in df.columns:
        return df.copy()
    out = df.copy()
    out["model_version"] = out["model_version"].map(_pretty_model_name)
    return out


def _build_watchlist(
    predictions: pd.DataFrame,
    consensus: pd.DataFrame,
    regional: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "bucket_start",
        "region",
        "signal_status",
        "high_risk_method_count",
        "methods_reporting",
        "avg_risk_score",
        "max_risk_score",
        "top_method",
        "high_risk_models",
        "total_case_events",
        "avg_severity",
        "narrative",
    ]
    if predictions.empty or consensus.empty:
        return pd.DataFrame(columns=columns)

    pred = _coerce_bucket_start(_coerce_prediction_timestamp(predictions))
    cons = _coerce_bucket_start(consensus)
    regional_latest = _latest_bucket_rows(regional)

    latest_bucket = cons["bucket_start"].dropna().max()
    latest_cons = cons.loc[cons["bucket_start"] == latest_bucket].copy()
    latest_pred = pred.loc[pred["bucket_start"] == latest_bucket].copy()

    grouped = latest_pred.groupby("region", dropna=False)
    score_summary = grouped.agg(
        prediction_method_count=("model_version", "nunique"),
    ).reset_index()

    top_method = (
        latest_pred.sort_values(["region", "risk_score"], ascending=[True, False])
        .groupby("region", as_index=False)
        .first()[["region", "model_version"]]
        .rename(columns={"model_version": "top_method"})
    )
    top_method["top_method"] = top_method["top_method"].map(_pretty_model_name)

    high_risk_models = (
        latest_pred.loc[latest_pred["predicted_label"] == "high_risk"]
        .groupby("region")["model_version"]
        .apply(lambda s: ", ".join(sorted(_pretty_model_name(v) for v in s.tolist())))
        .reset_index(name="high_risk_models")
    )

    watch = (
        latest_cons.merge(score_summary, on="region", how="left")
        .merge(top_method, on="region", how="left")
        .merge(high_risk_models, on="region", how="left")
    )

    if "methods_reporting" not in watch.columns:
        if "methods_reporting_x" in watch.columns:
            watch["methods_reporting"] = watch["methods_reporting_x"]
        elif "methods_reporting_y" in watch.columns:
            watch["methods_reporting"] = watch["methods_reporting_y"]
        elif "prediction_method_count" in watch.columns:
            watch["methods_reporting"] = watch["prediction_method_count"]
        else:
            watch["methods_reporting"] = 0

    if "methods_reporting" in watch.columns:
        watch["methods_reporting"] = watch["methods_reporting"].fillna(
            watch.get("prediction_method_count", 0)
        )

    if "avg_risk_score" not in watch.columns:
        if "avg_risk_score_x" in watch.columns:
            watch["avg_risk_score"] = watch["avg_risk_score_x"]
        elif "avg_risk_score_y" in watch.columns:
            watch["avg_risk_score"] = watch["avg_risk_score_y"]
        else:
            watch["avg_risk_score"] = None

    if "max_risk_score" not in watch.columns:
        if "max_risk_score_x" in watch.columns:
            watch["max_risk_score"] = watch["max_risk_score_x"]
        elif "max_risk_score_y" in watch.columns:
            watch["max_risk_score"] = watch["max_risk_score_y"]
        else:
            watch["max_risk_score"] = None

    if not regional_latest.empty:
        region_context = regional_latest[["region", "total_case_events", "avg_severity"]].copy()
        watch = watch.merge(region_context, on="region", how="left")
    else:
        watch["total_case_events"] = None
        watch["avg_severity"] = None

    watch["signal_status"] = watch["consensus_label"]
    watch["high_risk_models"] = watch["high_risk_models"].fillna("None")

    def narrative(row) -> str:
        methods = int(row["methods_reporting"]) if pd.notna(row["methods_reporting"]) else 0
        flagged = int(row["high_risk_method_count"]) if pd.notna(row["high_risk_method_count"]) else 0
        cases = int(row["total_case_events"]) if pd.notna(row["total_case_events"]) else 0
        severity = _format_float(row["avg_severity"])
        if flagged > 0:
            return (
                f"Based on {cases} case events, average severity {severity}, and "
                f"{flagged} of {methods} detection methods elevating this region, "
                f"this region is currently in {row['signal_status'].lower()} status."
            )
        return (
            f"Based on {cases} case events and average severity {severity}, "
            f"this region is currently showing normal detector status."
        )

    watch["narrative"] = watch.apply(narrative, axis=1)
    return watch[columns].sort_values(
        ["high_risk_method_count", "max_risk_score", "total_case_events"],
        ascending=[False, False, False],
    )


def _build_detector_briefing(
    watchlist: pd.DataFrame,
    hospital_pressure: pd.DataFrame,
    diagnosis: pd.DataFrame,
) -> list[str]:
    lines: list[str] = []

    if not watchlist.empty:
        top_region = watchlist.iloc[0]
        if int(top_region["high_risk_method_count"]) > 0:
            lines.append(
                "Based on "
                f"{top_region['region']} reaching {int(top_region['total_case_events']) if pd.notna(top_region['total_case_events']) else 0} "
                f"case events with support from {int(top_region['high_risk_method_count'])} detector methods, "
                "this region is the top current outbreak concern."
            )
        else:
            lines.append(
                "Based on the latest detector run, no region is currently showing a strong multi-method outbreak signal."
            )

    latest_hospital = _latest_bucket_rows(hospital_pressure)
    if not latest_hospital.empty:
        top_hospital = latest_hospital.sort_values(
            "capacity_pressure_index", ascending=False, na_position="last"
        ).iloc[0]
        lines.append(
            "Based on hospital "
            f"{top_hospital['hospital_id']} reaching capacity pressure {_format_float(top_hospital['capacity_pressure_index'])}, "
            "this is the first operations hotspot to review."
        )

    latest_diagnosis = _latest_bucket_rows(diagnosis)
    if not latest_diagnosis.empty:
        top_diagnosis = latest_diagnosis.sort_values(
            "diagnosis_visit_count", ascending=False, na_position="last"
        ).iloc[0]
        lines.append(
            "Based on diagnosis "
            f"{top_diagnosis['diagnosis_code']} appearing {int(top_diagnosis['diagnosis_visit_count'])} times "
            f"with average temperature {_format_float(top_diagnosis['avg_temperature_f'])}, "
            "this is the strongest clinical pattern in the latest bucket."
        )

    return lines


def _latest_total_events(event_volume: pd.DataFrame) -> int:
    latest = _latest_bucket_rows(event_volume)
    if latest.empty:
        return 0
    row = latest.sort_values("bucket_start").iloc[-1]
    return int(
        row["symptom_report_volume"] + row["hospital_admission_volume"] + row["clinic_visit_volume"]
    )


def _latest_vaccination_total(vaccination: pd.DataFrame) -> int:
    latest = _latest_bucket_rows(vaccination)
    if latest.empty:
        return 0
    return int(latest["vaccination_count"].fillna(0).sum())


def _display_table(df: pd.DataFrame, *, columns: list[str], column_labels: dict[str, str] | None = None):
    view = df.loc[:, columns].copy()
    if column_labels:
        view = view.rename(columns=column_labels)
    st.dataframe(view, use_container_width=True, hide_index=True)


with st.sidebar:
    st.header("Controls")
    page = st.radio(
        "Page",
        [
            "Situation Room",
            "Regional Pulse",
            "Detector Signals",
            "Operations Desk",
            "Clinical Context",
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
prediction_run = _coerce_prediction_timestamp(get_latest_prediction_run())
prediction_methods = [_pretty_model_name(m) for m in get_prediction_methods()]
prediction_rows = _coerce_prediction_timestamp(
    get_prediction_rows(hours=min(hours, 72))
)
detector_consensus = _coerce_prediction_timestamp(
    get_detector_consensus(hours=min(hours, 72))
)
prediction_rows_labeled = _label_prediction_methods(prediction_rows)
watchlist = _build_watchlist(prediction_rows, detector_consensus, regional)

latest_bucket = _latest_bucket_start(
    [event_volume, regional, hospital_pressure, follow_up, diagnosis, vaccination, prediction_rows]
)
latest_proc_ts, latest_batch_id = _latest_processing_metadata(
    [event_volume, regional, hospital_pressure, follow_up, diagnosis, vaccination]
)
latest_prediction_ts = None
latest_prediction_run_id = None
if not prediction_run.empty:
    latest_prediction_ts = prediction_run.iloc[0]["latest_prediction_timestamp"]
    latest_prediction_run_id = prediction_run.iloc[0]["run_id"]

banner_parts = []
if latest_proc_ts is not None:
    banner_parts.append(f"Latest processed analytics run: {_format_ts(latest_proc_ts)}")
if latest_bucket is not None:
    banner_parts.append(f"Latest event-time bucket: {_format_ts(latest_bucket)}")
if latest_prediction_ts is not None:
    banner_parts.append(f"Latest detector refresh: {_format_ts(latest_prediction_ts)}")
if latest_prediction_run_id:
    banner_parts.append(f"detector run_id: {latest_prediction_run_id}")
if latest_batch_id:
    parsed_window, parsed_run = _parse_batch_id(str(latest_batch_id))
    if parsed_window:
        banner_parts.append(f"batch window: {parsed_window}:00 UTC")
    if parsed_run:
        banner_parts.append(f"batch created: {parsed_run}")

if banner_parts:
    st.info(" | ".join(banner_parts))
else:
    st.warning("No data returned for the selected time window.")

st.caption(
    "Charts follow `bucket_start`. A newly processed batch can refresh an older event-time hour, "
    "so freshness and event time are shown separately."
)

with st.expander("How To Read This Dashboard"):
    st.markdown(
        "- `Latest Event-Time Hour` is the newest hour of outbreak data represented in the charts.\n"
        "- `Latest Processed Run` is when the analytics pipeline most recently refreshed the dashboard.\n"
        "- `Regions in High Risk` counts regions where at least three of the four outbreak methods agree.\n"
        "- `Detector Agreement` shows how many of the four methods flagged each region.\n"
        "- The four outbreak methods shown here are Random Forest, Moving Average, EWMA, and Kalman Filter."
    )


if page == "Situation Room":
    latest_events = _latest_total_events(event_volume)
    regions_high_risk = 0 if watchlist.empty else int((watchlist["signal_status"] == "High Risk").sum())
    latest_hospitals = _latest_bucket_rows(hospital_pressure)
    max_pressure = None if latest_hospitals.empty else latest_hospitals["capacity_pressure_index"].dropna().max()
    top_region_name = "None"
    top_region_signal = "No active signal"
    if not watchlist.empty:
        top_region_name = str(watchlist.iloc[0]["region"])
        top_region_signal = f"{int(watchlist.iloc[0]['high_risk_method_count'])} methods flagged"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Latest Event-Time Hour",
        _format_ts(latest_bucket).replace(" UTC", "") if latest_bucket is not None else "N/A",
    )
    col2.metric(
        "Latest Processed Run",
        _format_ts(latest_proc_ts).replace(" UTC", "") if latest_proc_ts is not None else "N/A",
    )
    col3.metric("Regions in High Risk", f"{regions_high_risk}")
    col4.metric(
        "Top Region",
        top_region_name,
        delta=top_region_signal,
    )

    st.subheader("Current Briefing")
    for line in _build_detector_briefing(watchlist, hospital_pressure, diagnosis):
        st.markdown(f"- {line}")

    st.subheader("Event Volume by Type")
    line_event_volume(event_volume)

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Detector Agreement")
        bar_detector_agreement(detector_consensus)
    with right:
        st.subheader("Regional Watchlist")
        if watchlist.empty:
            st.info("No detector rows are available for the latest run.")
        else:
            watch_view = watchlist.copy()
            watch_view["max_risk_score"] = watch_view["max_risk_score"].round(2)
            watch_view["avg_severity"] = pd.to_numeric(
                watch_view["avg_severity"], errors="coerce"
            ).round(2)
            _display_table(
                watch_view.head(5),
                columns=[
                    "region",
                    "signal_status",
                    "high_risk_method_count",
                    "methods_reporting",
                    "max_risk_score",
                    "top_method",
                    "total_case_events",
                    "avg_severity",
                ],
                column_labels={
                    "region": "Region",
                    "signal_status": "Status",
                    "high_risk_method_count": "Methods Flagging (0-4)",
                    "methods_reporting": "Methods Reporting",
                    "max_risk_score": "Max Risk",
                    "top_method": "Top Method",
                    "total_case_events": "Cases",
                    "avg_severity": "Avg Severity",
                },
            )
            with st.expander("Show full watchlist details"):
                _display_table(
                    watch_view,
                    columns=[
                        "region",
                        "signal_status",
                        "high_risk_method_count",
                        "methods_reporting",
                        "avg_risk_score",
                        "max_risk_score",
                        "top_method",
                        "high_risk_models",
                        "total_case_events",
                        "avg_severity",
                        "narrative",
                    ],
                )

elif page == "Regional Pulse":
    st.subheader("Regional Pulse")
    region_candidates = sorted(
        {
            *get_regions(),
            *(prediction_rows["region"].dropna().tolist() if not prediction_rows.empty else []),
        }
    )
    if not region_candidates:
        st.info("No regional rows are available in the selected time window.")
        st.stop()

    region = st.selectbox("Region", region_candidates, index=0)
    regional_filtered = get_regional_trends(hours=hours, region=region)
    region_predictions = _coerce_prediction_timestamp(
        get_prediction_rows(hours=min(hours, 72), region=region)
    )
    region_predictions_labeled = _label_prediction_methods(region_predictions)
    region_consensus = _coerce_prediction_timestamp(
        get_detector_consensus(hours=min(hours, 72), region=region)
    )
    region_watch = watchlist.loc[watchlist["region"] == region].head(1)
    selected_row = region_watch.iloc[0] if not region_watch.empty else None

    latest_region = _latest_bucket_rows(regional_filtered)
    latest_region_cases = None
    latest_region_severity = None
    if not latest_region.empty:
        row = latest_region.sort_values("bucket_start").iloc[-1]
        latest_region_cases = row["total_case_events"]
        latest_region_severity = row["avg_severity"]

    latest_region_vacc = vaccination.loc[vaccination["region"] == region].copy()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Region", region)
    col2.metric(
        "Signal Status",
        "N/A" if selected_row is None else selected_row["signal_status"],
    )
    col3.metric(
        "Latest Case Events",
        "N/A" if latest_region_cases is None or pd.isna(latest_region_cases) else f"{int(latest_region_cases):,}",
    )
    col4.metric(
        "Latest Avg Severity",
        _format_float(latest_region_severity),
    )

    if selected_row is not None:
        st.info(selected_row["narrative"])

    st.caption(
        "This page explains one region from three angles: disease severity, total case volume, "
        "and outbreak-method agreement."
    )

    left, right = st.columns(2)
    with left:
        line_regional_severity(regional_filtered)
    with right:
        area_total_cases(regional_filtered)

    st.markdown("#### Outbreak Signal Trend")
    left, right = st.columns(2)
    with left:
        line_detector_risk(region_predictions_labeled, title=f"Risk Score by Method: {region}")
    with right:
        line_detector_agreement(region_consensus, title=f"Detector Agreement Over Time: {region}")

    st.markdown("#### Vaccination Context")
    left, right = st.columns([3, 2])
    with left:
        line_vaccination_volume(latest_region_vacc)
    with right:
        latest_region_vacc_rows = _latest_bucket_rows(latest_region_vacc).sort_values(
            "vaccination_count", ascending=False, na_position="last"
        )
        if latest_region_vacc_rows.empty:
            st.info("No vaccination rows for this region.")
        else:
            _display_table(
                latest_region_vacc_rows.head(8),
                columns=["vaccine_type", "dose_number", "vaccination_count", "batch_id"],
                column_labels={
                    "vaccine_type": "Vaccine",
                    "dose_number": "Dose",
                    "vaccination_count": "Count",
                    "batch_id": "Batch",
                },
            )

    if not region_predictions.empty:
        with st.expander("Show detector rows for this region"):
            region_view = region_predictions_labeled.sort_values(
                ["bucket_start", "risk_score"], ascending=[False, False]
            ).copy()
            region_view["risk_score"] = region_view["risk_score"].round(3)
            _display_table(
                region_view.head(20),
                columns=[
                    "bucket_start",
                    "model_version",
                    "risk_score",
                    "predicted_label",
                    "threshold",
                ],
                column_labels={
                    "bucket_start": "Bucket",
                    "model_version": "Method",
                    "risk_score": "Risk",
                    "predicted_label": "Label",
                    "threshold": "Threshold",
                },
            )

elif page == "Detector Signals":
    st.subheader("Detector Signals")
    detector_choices = ["ALL"] + prediction_methods
    detector = st.selectbox("Detection Method", detector_choices, index=0)
    detector_raw = next(
        (raw for raw, label in MODEL_LABELS.items() if label == detector),
        detector,
    )
    filtered_predictions = _coerce_prediction_timestamp(
        get_prediction_rows(hours=min(hours, 72), model_version=detector_raw)
    )
    filtered_predictions_labeled = _label_prediction_methods(filtered_predictions)

    if filtered_predictions.empty:
        st.info("No detector output is available for the selected method and time range.")
    else:
        latest_bucket_consensus = _latest_bucket_rows(detector_consensus)

        if detector == "ALL":
            detector_text = "all methods"
        else:
            detector_text = detector

        st.caption(
            "This page compares the four outbreak methods directly. Higher scores mean a stronger outbreak signal for that region-hour."
        )

        if not latest_bucket_consensus.empty:
            top_consensus = latest_bucket_consensus.sort_values(
                ["high_risk_method_count", "max_risk_score"], ascending=[False, False]
            ).iloc[0]
            st.info(
                "Based on the latest detector run, "
                f"{top_consensus['region']} has the strongest signal under {detector_text}, "
                f"with {int(top_consensus['high_risk_method_count'])} methods flagging high risk."
            )

        left, right = st.columns(2)
        with left:
            bar_detector_scores(
                filtered_predictions_labeled,
                title="Latest Detector Scores by Region",
            )
        with right:
            bar_detector_agreement(
                detector_consensus,
                title="Detector Agreement by Region (Methods Flagging High Risk)",
            )
        if detector == "ALL":
            trend_df = (
                _coerce_bucket_start(filtered_predictions_labeled)
                .groupby(["bucket_start", "model_version"], as_index=False)["risk_score"]
                .mean()
            )
            line_detector_risk(trend_df, title="Average Detector Risk Score Trend")
        else:
            line_detector_risk(filtered_predictions_labeled, title=f"Risk Score Trend: {detector}")

        latest_signal_rows = filtered_predictions_labeled.sort_values(
            ["bucket_start", "risk_score"], ascending=[False, False]
        ).copy()
        latest_signal_rows["risk_score"] = latest_signal_rows["risk_score"].round(3)
        _display_table(
            latest_signal_rows.head(15),
            columns=[
                "bucket_start",
                "region",
                "model_version",
                "risk_score",
                "predicted_label",
                "threshold",
            ],
            column_labels={
                "bucket_start": "Bucket",
                "region": "Region",
                "model_version": "Method",
                "risk_score": "Risk",
                "predicted_label": "Label",
                "threshold": "Threshold",
            },
        )

elif page == "Operations Desk":
    st.subheader("Operations Desk")
    latest_hospital_rows = _latest_bucket_rows(hospital_pressure).sort_values(
        "capacity_pressure_index", ascending=False, na_position="last"
    )
    latest_follow_rows = _latest_bucket_rows(follow_up).sort_values(
        "follow_up_rate", ascending=False, na_position="last"
    )

    if not latest_hospital_rows.empty:
        top_hospital = latest_hospital_rows.iloc[0]
        top_follow = latest_follow_rows.iloc[0] if not latest_follow_rows.empty else None
        summary = (
            "Based on hospital "
            f"{top_hospital['hospital_id']} reaching pressure {_format_float(top_hospital['capacity_pressure_index'])}, "
        )
        if top_follow is not None:
            summary += (
                f"and clinic {top_follow['clinic_id']} posting follow-up rate "
                f"{_format_pct(top_follow['follow_up_rate'])}, "
            )
        summary += "this is where surge planning should focus first."
        st.info(summary)

    st.caption(
        "This page highlights where hospital capacity and clinic follow-up demand are most concentrated in the latest bucket."
    )

    left, right = st.columns([3, 2])
    with left:
        bar_capacity_pressure(hospital_pressure)
    with right:
        hospital_view = latest_hospital_rows.copy()
        hospital_view["capacity_pressure_index"] = hospital_view["capacity_pressure_index"].round(3)
        _display_table(
            hospital_view.head(10),
            columns=[
                "hospital_id",
                "admission_count",
                "available_beds_last",
                "capacity_pressure_index",
            ],
            column_labels={
                "hospital_id": "Hospital",
                "admission_count": "Admissions",
                "available_beds_last": "Beds Last",
                "capacity_pressure_index": "Pressure",
            },
        )

    st.markdown("#### Clinic Follow-up Load")
    left, right = st.columns([3, 2])
    with left:
        line_follow_up_rates(follow_up)
    with right:
        follow_view = latest_follow_rows.copy()
        follow_view["follow_up_rate"] = follow_view["follow_up_rate"].round(3)
        _display_table(
            follow_view.head(10),
            columns=[
                "clinic_id",
                "visit_count",
                "follow_up_true_count",
                "follow_up_rate",
            ],
            column_labels={
                "clinic_id": "Clinic",
                "visit_count": "Visits",
                "follow_up_true_count": "Follow-ups",
                "follow_up_rate": "Rate",
            },
        )

elif page == "Clinical Context":
    st.subheader("Clinical Context")
    latest_diag_rows = _latest_bucket_rows(diagnosis).sort_values(
        "diagnosis_visit_count", ascending=False, na_position="last"
    )
    latest_vaccination_rows = _latest_bucket_rows(vaccination).sort_values(
        "vaccination_count", ascending=False, na_position="last"
    )

    if not latest_diag_rows.empty:
        top_diag = latest_diag_rows.iloc[0]
        st.info(
            "Based on diagnosis "
            f"{top_diag['diagnosis_code']} appearing {int(top_diag['diagnosis_visit_count'])} times "
            f"with average temperature {_format_float(top_diag['avg_temperature_f'])}, "
            "this looks like the strongest clinical signal in the latest bucket."
        )

    st.caption(
        "This page explains what is happening clinically: which diagnoses are most active, how temperatures are trending, and how vaccination activity compares across regions."
    )

    left, right = st.columns([3, 2])
    with left:
        line_diagnosis_temperature(diagnosis)
    with right:
        bar_top_diagnoses(diagnosis)

    diag_view = latest_diag_rows.copy()
    diag_view["avg_temperature_f"] = diag_view["avg_temperature_f"].round(1)
    _display_table(
        diag_view.head(10),
        columns=[
            "clinic_id",
            "diagnosis_code",
            "diagnosis_visit_count",
            "avg_temperature_f",
        ],
        column_labels={
            "clinic_id": "Clinic",
            "diagnosis_code": "Diagnosis",
            "diagnosis_visit_count": "Visits",
            "avg_temperature_f": "Avg Temp F",
        },
    )

    st.markdown("#### Vaccination Signals")
    col1, col2 = st.columns([3, 2])
    with col1:
        line_vaccination_volume(vaccination)
    with col2:
        st.metric("Vaccinations Logged (latest hour)", f"{_latest_vaccination_total(vaccination):,}")
        bar_top_vaccination_regions(vaccination)

    vacc_view = latest_vaccination_rows.copy()
    _display_table(
        vacc_view.head(10),
        columns=["region", "vaccine_type", "dose_number", "vaccination_count"],
        column_labels={
            "region": "Region",
            "vaccine_type": "Vaccine",
            "dose_number": "Dose",
            "vaccination_count": "Count",
        },
    )
