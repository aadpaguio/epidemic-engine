import logging

import pandas as pd
import streamlit as st

from db import get_connection

logger = logging.getLogger(__name__)


def _query_df(sql: str, params=None) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


@st.cache_data(ttl=5)
def get_event_volume(hours: int = 72) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            symptom_report_volume,
            hospital_admission_volume,
            clinic_visit_volume,
            as_of_run_timestamp
        FROM analytic_event_volume_by_type
        WHERE bucket_start >= now() - (%s || ' hours')::interval
        ORDER BY bucket_start;
    """
    return _query_df(sql, params=(hours,))


@st.cache_data(ttl=5)
def get_regional_trends(hours: int = 72, region: str = "ALL") -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            region,
            avg_severity,
            total_case_events,
            batch_id,
            run_timestamp
        FROM analytic_regional_disease_severity_trend
        WHERE bucket_start >= now() - (%s || ' hours')::interval
          AND (%s = 'ALL' OR region = %s)
        ORDER BY bucket_start, region;
    """
    return _query_df(sql, params=(hours, region, region))


@st.cache_data(ttl=5)
def get_hospital_pressure(hours: int = 24) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            hospital_id,
            admission_count,
            available_beds_last,
            capacity_pressure_index,
            batch_id,
            run_timestamp
        FROM analytic_hospital_capacity_pressure
        WHERE bucket_start >= now() - (%s || ' hours')::interval
        ORDER BY bucket_start DESC, capacity_pressure_index DESC NULLS LAST;
    """
    return _query_df(sql, params=(hours,))


@st.cache_data(ttl=5)
def get_follow_up_rates(hours: int = 24) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            clinic_id,
            visit_count,
            follow_up_true_count,
            follow_up_rate,
            batch_id,
            run_timestamp
        FROM analytic_follow_up_rate_by_clinic
        WHERE bucket_start >= now() - (%s || ' hours')::interval
        ORDER BY bucket_start, clinic_id;
    """
    return _query_df(sql, params=(hours,))


@st.cache_data(ttl=5)
def get_diagnosis_temperature(hours: int = 48) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            clinic_id,
            diagnosis_code,
            diagnosis_visit_count,
            avg_temperature_f,
            batch_id,
            run_timestamp
        FROM analytic_avg_temperature_by_diagnosis
        WHERE bucket_start >= now() - (%s || ' hours')::interval
        ORDER BY bucket_start, diagnosis_code;
    """
    return _query_df(sql, params=(hours,))


@st.cache_data(ttl=5)
def get_vaccination_volume(hours: int = 72) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            region,
            vaccine_type,
            dose_number,
            vaccination_count,
            batch_id,
            run_timestamp
        FROM analytic_vaccination_volume_by_type
        WHERE bucket_start >= now() - (%s || ' hours')::interval
        ORDER BY bucket_start, region, vaccine_type, dose_number;
    """
    return _query_df(sql, params=(hours,))


@st.cache_data(ttl=60)
def get_alert_events(limit: int = 300) -> pd.DataFrame:
    """Operational alerts written by epidemic-alert-service into alert_events."""
    sql = """
        SELECT
            alert_key,
            created_at,
            updated_at,
            alert_type,
            severity,
            region,
            bucket_start,
            model_versions,
            max_risk_score,
            run_id,
            status,
            message,
            raw_payload
        FROM alert_events
        ORDER BY updated_at DESC NULLS LAST
        LIMIT %s;
    """
    try:
        return _query_df(sql, params=(limit,))
    except Exception as exc:
        logger.warning("get_alert_events failed: %s", exc, exc_info=True)
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_regions() -> list[str]:
    sql = """
        SELECT DISTINCT region
        FROM analytic_regional_disease_severity_trend
        ORDER BY region;
    """
    df = _query_df(sql)
    return df["region"].tolist()


@st.cache_data(ttl=5)
def get_latest_prediction_run() -> pd.DataFrame:
    sql = """
        SELECT
            run_id,
            MAX(prediction_timestamp) AS latest_prediction_timestamp
        FROM ml_outbreak_predictions
        GROUP BY run_id
        ORDER BY latest_prediction_timestamp DESC
        LIMIT 1;
    """
    return _query_df(sql)


@st.cache_data(ttl=5)
def get_prediction_methods() -> list[str]:
    sql = """
        WITH latest AS (
            SELECT run_id
            FROM ml_outbreak_predictions
            ORDER BY prediction_timestamp DESC
            LIMIT 1
        )
        SELECT DISTINCT model_version
        FROM ml_outbreak_predictions
        WHERE run_id = (SELECT run_id FROM latest)
        ORDER BY model_version;
    """
    df = _query_df(sql)
    if df.empty:
        return []
    return df["model_version"].tolist()


@st.cache_data(ttl=5)
def get_prediction_rows(
    hours: int = 24,
    region: str = "ALL",
    model_version: str = "ALL",
) -> pd.DataFrame:
    sql = """
        WITH latest AS (
            SELECT run_id
            FROM ml_outbreak_predictions
            ORDER BY prediction_timestamp DESC
            LIMIT 1
        )
        SELECT
            prediction_timestamp,
            bucket_start,
            region,
            model_version,
            risk_score,
            predicted_label,
            threshold,
            run_id
        FROM ml_outbreak_predictions
        WHERE run_id = (SELECT run_id FROM latest)
          AND bucket_start >= now() - (%s || ' hours')::interval
          AND (%s = 'ALL' OR region = %s)
          AND (%s = 'ALL' OR model_version = %s)
        ORDER BY bucket_start, region, model_version;
    """
    return _query_df(sql, params=(hours, region, region, model_version, model_version))


@st.cache_data(ttl=5)
def get_detector_consensus(hours: int = 24, region: str = "ALL") -> pd.DataFrame:
    sql = """
        WITH latest AS (
            SELECT run_id
            FROM ml_outbreak_predictions
            ORDER BY prediction_timestamp DESC
            LIMIT 1
        )
        SELECT
            bucket_start,
            region,
            COUNT(*) FILTER (WHERE predicted_label = 'high_risk') AS high_risk_method_count,
            COUNT(*) AS methods_reporting,
            AVG(risk_score) AS avg_risk_score,
            MAX(risk_score) AS max_risk_score,
            CASE
                WHEN COUNT(*) FILTER (WHERE predicted_label = 'high_risk') >= 3 THEN 'High Risk'
                WHEN COUNT(*) FILTER (WHERE predicted_label = 'high_risk') >= 1 THEN 'Elevated'
                ELSE 'Normal'
            END AS consensus_label
        FROM ml_outbreak_predictions
        WHERE run_id = (SELECT run_id FROM latest)
          AND bucket_start >= now() - (%s || ' hours')::interval
          AND (%s = 'ALL' OR region = %s)
        GROUP BY bucket_start, region
        ORDER BY bucket_start, region;
    """
    return _query_df(sql, params=(hours, region, region))
