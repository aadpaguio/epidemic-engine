import pandas as pd
import plotly.express as px
import streamlit as st


def _normalize_bucket_start(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "bucket_start" in out.columns:
        out["bucket_start"] = pd.to_datetime(out["bucket_start"], utc=True).dt.floor("h")
    return out


def _apply_time_axis(fig, df: pd.DataFrame):
    # Show only real hourly buckets on the x-axis (no interpolated ticks).
    if "bucket_start" not in df.columns or df.empty:
        return
    ts = pd.to_datetime(df["bucket_start"], utc=True).dropna().sort_values()
    if ts.empty:
        return
    tickvals = ts.drop_duplicates().tolist()
    ticktext = [t.strftime("%Y-%m-%d %H:%M") for t in tickvals]
    fig.update_xaxes(
        tickmode="array",
        tickvals=tickvals,
        ticktext=ticktext,
    )


def metric_cards(
    total_events: int,
    avg_severity: float | None,
    max_capacity_pressure: float | None,
    latest_follow_up_rate: float | None,
):
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events (latest hour)", f"{total_events:,}")
    col2.metric(
        "Average Severity (latest hour)",
        "N/A" if avg_severity is None else f"{avg_severity:.2f}",
    )
    col3.metric(
        "Max Capacity Pressure (24h)",
        "N/A" if max_capacity_pressure is None else f"{max_capacity_pressure:.2f}",
    )
    col4.metric(
        "Follow-up Rate (latest hour)",
        "N/A" if latest_follow_up_rate is None else f"{latest_follow_up_rate:.1%}",
    )


def line_event_volume(df: pd.DataFrame):
    if df.empty:
        st.info("No event volume data available in selected time range.")
        return
    df = _normalize_bucket_start(df)
    chart_df = df.melt(
        id_vars=["bucket_start"],
        value_vars=[
            "symptom_report_volume",
            "hospital_admission_volume",
            "clinic_visit_volume",
        ],
        var_name="event_type",
        value_name="volume",
    )
    fig = px.line(
        chart_df,
        x="bucket_start",
        y="volume",
        color="event_type",
        title="Event Volume by Type (Layer B)",
    )
    _apply_time_axis(fig, chart_df)
    st.plotly_chart(fig, use_container_width=True)


def line_regional_severity(df: pd.DataFrame):
    if df.empty:
        st.info("No regional severity data available.")
        return
    df = _normalize_bucket_start(df)
    fig = px.line(
        df,
        x="bucket_start",
        y="avg_severity",
        color="region",
        title="Regional Disease Severity Trend",
    )
    _apply_time_axis(fig, df)
    st.plotly_chart(fig, use_container_width=True)


def area_total_cases(df: pd.DataFrame):
    if df.empty:
        return
    df = _normalize_bucket_start(df)
    fig = px.area(
        df,
        x="bucket_start",
        y="total_case_events",
        color="region",
        title="Total Case Events by Region",
    )
    _apply_time_axis(fig, df)
    st.plotly_chart(fig, use_container_width=True)


def bar_capacity_pressure(df: pd.DataFrame):
    if df.empty:
        st.info("No hospital capacity data available.")
        return
    df = _normalize_bucket_start(df)
    latest = (
        df.sort_values("bucket_start")
        .groupby("hospital_id", as_index=False)
        .tail(1)
        .sort_values("capacity_pressure_index", ascending=False)
    )
    fig = px.bar(
        latest,
        x="hospital_id",
        y="capacity_pressure_index",
        title="Latest Capacity Pressure by Hospital",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(
        latest[
            [
                "bucket_start",
                "hospital_id",
                "admission_count",
                "available_beds_last",
                "capacity_pressure_index",
            ]
        ],
        use_container_width=True,
    )


def line_follow_up_rates(df: pd.DataFrame):
    if df.empty:
        st.info("No follow-up data available.")
        return
    df = _normalize_bucket_start(df)
    fig = px.line(
        df,
        x="bucket_start",
        y="follow_up_rate",
        color="clinic_id",
        title="Follow-up Rate by Clinic",
    )
    _apply_time_axis(fig, df)
    st.plotly_chart(fig, use_container_width=True)


def line_diagnosis_temperature(df: pd.DataFrame):
    if df.empty:
        st.info("No diagnosis temperature data available.")
        return
    df = _normalize_bucket_start(df)
    fig = px.line(
        df,
        x="bucket_start",
        y="avg_temperature_f",
        color="diagnosis_code",
        title="Average Temperature by Diagnosis",
    )
    _apply_time_axis(fig, df)
    st.plotly_chart(fig, use_container_width=True)


def line_vaccination_volume(df: pd.DataFrame):
    if df.empty:
        st.info("No vaccination volume data available.")
        return
    df = _normalize_bucket_start(df)
    plot_df = (
        df.groupby(["bucket_start", "vaccine_type"], as_index=False)["vaccination_count"]
        .sum()
        .sort_values("bucket_start")
    )
    fig = px.line(
        plot_df,
        x="bucket_start",
        y="vaccination_count",
        color="vaccine_type",
        title="Vaccination Volume by Type",
    )
    _apply_time_axis(fig, plot_df)
    st.plotly_chart(fig, use_container_width=True)


def line_detector_risk(df: pd.DataFrame, title: str = "Risk Score by Detection Method"):
    if df.empty:
        st.info("No detector output is available for the selected range.")
        return
    plot_df = _normalize_bucket_start(df)
    fig = px.line(
        plot_df,
        x="bucket_start",
        y="risk_score",
        color="model_version",
        markers=True,
        title=title,
    )
    fig.update_yaxes(range=[0, 1], title="Risk Score")
    _apply_time_axis(fig, plot_df)
    st.plotly_chart(fig, use_container_width=True)


def bar_detector_scores(df: pd.DataFrame, title: str = "Latest Detector Scores by Region"):
    if df.empty:
        st.info("No detector scores are available for the selected range.")
        return
    plot_df = df.copy()
    if "bucket_start" in plot_df.columns:
        plot_df = _normalize_bucket_start(plot_df)
        latest_bucket = plot_df["bucket_start"].max()
        plot_df = plot_df.loc[plot_df["bucket_start"] == latest_bucket].copy()
    fig = px.bar(
        plot_df,
        x="region",
        y="risk_score",
        color="model_version",
        barmode="group",
        title=title,
    )
    fig.update_yaxes(range=[0, 1], title="Risk Score")
    st.plotly_chart(fig, use_container_width=True)


def bar_detector_agreement(df: pd.DataFrame, title: str = "Detector Agreement by Region"):
    if df.empty:
        st.info("No detector agreement rows are available.")
        return
    plot_df = df.copy()
    if "bucket_start" in plot_df.columns:
        plot_df = _normalize_bucket_start(plot_df)
        latest_bucket = plot_df["bucket_start"].max()
        plot_df = plot_df.loc[plot_df["bucket_start"] == latest_bucket].copy()
    fig = px.bar(
        plot_df,
        x="region",
        y="high_risk_method_count",
        color="consensus_label",
        text="high_risk_method_count",
        title=title,
        category_orders={"consensus_label": ["High Risk", "Elevated", "Normal"]},
    )
    fig.update_yaxes(title="Methods Flagging High Risk")
    st.plotly_chart(fig, use_container_width=True)


def line_detector_agreement(df: pd.DataFrame, title: str = "Detector Agreement Over Time"):
    if df.empty:
        st.info("No detector agreement trend is available.")
        return
    plot_df = _normalize_bucket_start(df)
    fig = px.line(
        plot_df,
        x="bucket_start",
        y="high_risk_method_count",
        color="region",
        markers=True,
        title=title,
    )
    fig.update_yaxes(title="Methods Flagging High Risk")
    _apply_time_axis(fig, plot_df)
    st.plotly_chart(fig, use_container_width=True)
