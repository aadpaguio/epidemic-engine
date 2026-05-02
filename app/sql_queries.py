import pandas as pd
import streamlit as st

from db import get_connection


def _query_df(sql: str, params=None) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


@st.cache_data(ttl=60)
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


@st.cache_data(ttl=60)
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


@st.cache_data(ttl=60)
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


@st.cache_data(ttl=60)
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


@st.cache_data(ttl=60)
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


@st.cache_data(ttl=60)
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
    except Exception:
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
