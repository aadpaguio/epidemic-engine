"""Local Spark DataFrame tests for parse → bucket → filter → feature builders."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType

pytestmark = pytest.mark.spark

import etl  # noqa: E402

from tests.fixtures.events import (  # noqa: E402
    TOPIC_CLINIC,
    TOPIC_HOSPITAL,
    TOPIC_SYMPTOM,
    TOPIC_VACCINATION,
    clinic_visit_payload,
    historical_timestamp_iso,
    hospital_admission_payload,
    symptom_report_payload,
    vaccination_payload_v2,
)


def _kafka_schema() -> StructType:
    return StructType(
        [
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("value", BinaryType(), True),
        ]
    )


def _df_from_payloads(spark_session, rows: list[tuple[str, int, int, dict]]):
    """rows: (topic, partition, offset, payload_dict)"""
    rdd_rows = [
        Row(
            topic=t,
            partition=p,
            offset=o,
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
        for t, p, o, payload in rows
    ]
    return spark_session.createDataFrame(rdd_rows, schema=_kafka_schema())


def test_parse_bucket_filter_historical_event_hour(spark_session):
    """Event-time in the past must fall into that hour bucket, not wall-clock."""
    ts = historical_timestamp_iso()
    window_start = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
    window_end = datetime(2026, 4, 1, 9, 0, tzinfo=timezone.utc)

    kafka_df = _df_from_payloads(
        spark_session,
        [
            (TOPIC_SYMPTOM, 0, 100, symptom_report_payload(ts=ts)),
            (TOPIC_CLINIC, 0, 101, clinic_visit_payload(ts=ts)),
            (TOPIC_HOSPITAL, 0, 102, hospital_admission_payload(ts=ts)),
            (TOPIC_VACCINATION, 0, 103, vaccination_payload_v2(ts=ts)),
        ],
    )
    parsed = etl.parse_kafka_to_payload(kafka_df)
    bucketed = etl.with_event_time_and_bucket(
        parsed, TOPIC_SYMPTOM, TOPIC_CLINIC, TOPIC_HOSPITAL, TOPIC_VACCINATION
    )
    filtered = etl.filter_window(bucketed, window_start, window_end)
    stats = etl.collect_parse_stats(filtered.cache(), (TOPIC_SYMPTOM, TOPIC_CLINIC, TOPIC_HOSPITAL, TOPIC_VACCINATION))
    assert stats["parse_ok_count"] == 4
    assert stats["skip_by_reason"].get("ACCEPT", 0) == 4


def test_malformed_json_gets_skip_reason(spark_session):
    bad = spark_session.createDataFrame(
        [Row(topic=TOPIC_SYMPTOM, partition=0, offset=1, value=b"{not json")],
        schema=_kafka_schema(),
    )
    parsed = etl.parse_kafka_to_payload(bad)
    bucketed = etl.with_event_time_and_bucket(
        parsed, TOPIC_SYMPTOM, TOPIC_CLINIC, TOPIC_HOSPITAL, TOPIC_VACCINATION
    )
    rows = bucketed.select("skip_reason").distinct().collect()
    reasons = {r.skip_reason for r in rows}
    assert "JSON_PARSE" in reasons


def test_feature_region_and_vaccination_counts(spark_session):
    ts = "2026-05-01T11:30:00Z"
    window_start = datetime(2026, 5, 1, 11, 0, tzinfo=timezone.utc)
    window_end = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    kafka_df = _df_from_payloads(
        spark_session,
        [
            (TOPIC_SYMPTOM, 0, 1, symptom_report_payload(ts=ts)),
            (TOPIC_CLINIC, 0, 2, clinic_visit_payload(ts=ts)),
            (TOPIC_HOSPITAL, 0, 3, hospital_admission_payload(ts=ts)),
            (TOPIC_VACCINATION, 0, 4, vaccination_payload_v2(ts=ts)),
        ],
    )
    parsed = etl.parse_kafka_to_payload(kafka_df)
    bucketed = etl.with_event_time_and_bucket(
        parsed, TOPIC_SYMPTOM, TOPIC_CLINIC, TOPIC_HOSPITAL, TOPIC_VACCINATION
    )
    filtered = etl.filter_window(bucketed, window_start, window_end).cache()

    df_symptoms = etl.preliminary_symptoms(filtered, TOPIC_SYMPTOM)
    df_clinic = etl.preliminary_clinic_visits(filtered, TOPIC_CLINIC)
    df_hospital = etl.preliminary_hospital_admissions(filtered, TOPIC_HOSPITAL)
    df_vaccination = etl.preliminary_vaccination_records(filtered, TOPIC_VACCINATION)

    run_ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    batch_id = "pytest-batch"

    region = etl.build_feature_region_hourly(df_symptoms, df_clinic, df_hospital, batch_id, run_ts).collect()
    assert len(region) == 1
    r0 = region[0]
    assert r0["symptom_count"] == 1
    assert r0["clinic_visit_count"] == 1
    assert r0["admission_count"] == 1

    vax = etl.build_feature_vaccination_hourly(df_vaccination, batch_id, run_ts).collect()
    assert len(vax) == 1
    assert vax[0]["vaccination_count"] == 1

    from pyspark.sql import functions as F  # noqa: N812

    exploded = df_symptoms.withColumn("symptom_token", F.explode("symptoms")).filter(
        F.col("symptom_token").isNotNull() & (F.length(F.col("symptom_token")) > 0)
    )
    sym_hourly = etl.build_feature_symptom_hourly(exploded, batch_id, run_ts).collect()
    assert len(sym_hourly) == 2

    clinic_h = etl.build_feature_clinic_hourly(df_clinic, batch_id, run_ts).collect()
    assert len(clinic_h) == 1
    assert clinic_h[0]["visit_count"] == 1

    hosp_h = etl.build_feature_hospital_hourly(df_hospital, batch_id, run_ts).collect()
    assert len(hosp_h) == 1

    diag = etl.build_feature_diagnosis_hourly(
        df_clinic.filter(F.col("diagnosis_code").isNotNull()),
        batch_id,
        run_ts,
    ).collect()
    assert len(diag) == 1
