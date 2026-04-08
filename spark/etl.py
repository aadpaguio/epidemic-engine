"""
Phase 2 Spark batch ETL: read typed Kafka topics, build hourly fact tables (Layer A).

Preliminaries: parsed, hour-bucketed event DataFrames per stream. Two facts implemented
here; three more are outlined in comment blocks below.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Config (env)
# ---------------------------------------------------------------------------

DEFAULT_SYMPTOM_TOPIC = "ds551-s26.team02.symptom_reports"
DEFAULT_CLINIC_TOPIC = "ds551-s26.team02.clinic_visits"
DEFAULT_HOSPITAL_TOPIC = "ds551-s26.team02.hospital_admissions"


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, "").strip()
    if v:
        return v
    if default is not None:
        return default
    raise RuntimeError(f"Missing required environment variable: {name}")


def _optional_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


# Single schema tolerates all typed payloads (unknown fields ignored by from_json).
PAYLOAD_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("region", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("symptoms", ArrayType(StringType()), True),
        StructField("severity", IntegerType(), True),
        StructField("duration_days", DoubleType(), True),
        StructField("reported_via", StringType(), True),
        StructField("visit_id", StringType(), True),
        StructField("clinic_id", StringType(), True),
        StructField("visit_type", StringType(), True),
        StructField("primary_complaint", StringType(), True),
        StructField("temperature_f", DoubleType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("prescribed_medication", StringType(), True),
        StructField("follow_up_required", BooleanType(), True),
        StructField("admission_id", StringType(), True),
        StructField("hospital_id", StringType(), True),
        StructField("admission_reason", StringType(), True),
        StructField("oxygen_level", DoubleType(), True),
        StructField("expected_los_days", DoubleType(), True),
        StructField("available_beds", IntegerType(), True),
        StructField("team_id", StringType(), True),
        StructField("processing_timestamp", StringType(), True),
        StructField("event_source", StringType(), True),
    ]
)


def log_analytics(payload: dict[str, Any]) -> None:
    """Emit one [ANALYTICS] line; payload must include plan-required envelope keys."""
    required = ("task", "phase", "outcome", "reason_code", "run_id", "batch_id")
    for k in required:
        if k not in payload:
            raise ValueError(f"log_analytics missing key: {k}")
    line = "[ANALYTICS] " + json.dumps(payload, default=str)
    print(line, flush=True)


def build_spark(app_name: str = "epidemic-phase2-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_typed_kafka(
    spark: SparkSession,
    bootstrap: str,
    topic_symptom: str,
    topic_clinic: str,
    topic_hospital: str,
) -> DataFrame:
    topics = f"{topic_symptom},{topic_clinic},{topic_hospital}"
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def parse_kafka_to_payload(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        "topic",
        "partition",
        "offset",
        F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA).alias("data"),
    ).select(
        "topic",
        "partition",
        "offset",
        F.col("data.event_type").alias("event_type"),
        F.col("data.timestamp").alias("ts_raw"),
        F.col("data.region").alias("region"),
        F.col("data.symptoms").alias("symptoms"),
        F.col("data.severity").cast("int").alias("severity"),
        F.col("data.duration_days").cast("double").alias("duration_days"),
        F.col("data.clinic_id").alias("clinic_id"),
        F.col("data.visit_type").alias("visit_type"),
        F.col("data.temperature_f").cast("double").alias("temperature_f"),
        F.col("data.diagnosis_code").alias("diagnosis_code"),
        F.when(F.lower(F.col("data.follow_up_required").cast("string")) == "true", F.lit(True))
        .when(F.lower(F.col("data.follow_up_required").cast("string")) == "false", F.lit(False))
        .otherwise(F.col("data.follow_up_required").cast("boolean"))
        .alias("follow_up_required"),
        F.col("data.hospital_id").alias("hospital_id"),
        F.col("data.oxygen_level").cast("double").alias("oxygen_level"),
        F.col("data.expected_los_days").cast("double").alias("expected_los_days"),
        F.col("data.available_beds").cast("double").alias("available_beds"),
    )


def with_event_time_and_bucket(parsed: DataFrame) -> DataFrame:
    ts = F.to_timestamp(F.col("ts_raw"))
    return (
        parsed.withColumn("event_ts", ts)
        .withColumn("bucket_start", F.date_trunc("hour", ts))
        .withColumn(
            "parse_ok",
            F.col("event_ts").isNotNull() & F.col("bucket_start").isNotNull(),
        )
    )


def preliminary_symptoms(parsed_bucketed: DataFrame, topic_symptom: str) -> DataFrame:
    """Hour-bucketed symptom_report rows (valid parse + expected topic)."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_symptom))
            & F.col("parse_ok")
            & F.col("region").isNotNull()
        )
        .select(
            F.lit("symptom_report").alias("event_type"),
            "bucket_start",
            "region",
            "event_ts",
            "severity",
            "duration_days",
            F.lit(None).cast("double").alias("temperature_f"),
            "available_beds",
        )
    )


def preliminary_clinic_visits(parsed_bucketed: DataFrame, topic_clinic: str) -> DataFrame:
    """Hour-bucketed clinic_visit rows."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_clinic))
            & F.col("parse_ok")
            & F.col("region").isNotNull()
            & F.col("clinic_id").isNotNull()
        )
        .select(
            F.lit("clinic_visit").alias("event_type"),
            "bucket_start",
            "region",
            "clinic_id",
            "event_ts",
            "visit_type",
            "temperature_f",
            "follow_up_required",
            "available_beds",
            "diagnosis_code",
        )
    )


def preliminary_hospital_admissions(
    parsed_bucketed: DataFrame, topic_hospital: str
) -> DataFrame:
    """Hour-bucketed hospital_admission rows."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_hospital))
            & F.col("parse_ok")
            & F.col("region").isNotNull()
            & F.col("hospital_id").isNotNull()
        )
        .select(
            F.lit("hospital_admission").alias("event_type"),
            "bucket_start",
            "region",
            "hospital_id",
            "event_ts",
            "severity",
            "temperature_f",
            "oxygen_level",
            "expected_los_days",
            "available_beds",
        )
    )


def build_feature_region_hourly(
    df_symptoms: DataFrame,
    df_clinic: DataFrame,
    df_hospital: DataFrame,
    batch_id: str,
    run_ts: datetime,
) -> DataFrame:
    """
    Natural key: (bucket_start, region). Combines all three streams for regional counts
    and gauge-style available_beds stats (min / avg / last-by-time / sample count).
    """
    sym = df_symptoms.select(
        "bucket_start",
        "region",
        "event_type",
        "event_ts",
        "severity",
        "duration_days",
        "temperature_f",
        "available_beds",
    )
    cli = df_clinic.select(
        "bucket_start",
        "region",
        "event_type",
        "event_ts",
        F.lit(None).cast("int").alias("severity"),
        F.lit(None).cast("double").alias("duration_days"),
        "temperature_f",
        "available_beds",
    )
    hos = df_hospital.select(
        "bucket_start",
        "region",
        "event_type",
        "event_ts",
        "severity",
        "duration_days",
        F.lit(None).cast("double").alias("temperature_f"),
        "available_beds",
    )
    base = sym.unionByName(cli).unionByName(hos)

    beds_last = F.expr("max_by(available_beds, cast(event_ts as timestamp))")

    agg = base.groupBy("bucket_start", "region").agg(
        F.sum(F.when(F.col("event_type") == "symptom_report", 1).otherwise(0)).alias(
            "symptom_count"
        ),
        F.sum(F.when(F.col("event_type") == "clinic_visit", 1).otherwise(0)).alias(
            "clinic_visit_count"
        ),
        F.sum(F.when(F.col("event_type") == "hospital_admission", 1).otherwise(0)).alias(
            "admission_count"
        ),
        F.sum("severity").alias("severity_sum"),
        F.count(F.col("severity")).alias("severity_count"),
        F.sum("duration_days").alias("duration_days_sum"),
        F.count(F.col("duration_days")).alias("duration_days_count"),
        F.sum("temperature_f").alias("temperature_sum"),
        F.count(F.col("temperature_f")).alias("temperature_count"),
        F.min("available_beds").alias("available_beds_min"),
        F.avg("available_beds").alias("available_beds_avg"),
        beds_last.alias("available_beds_last"),
        F.sum(F.when(F.col("available_beds").isNotNull(), 1).otherwise(0)).alias(
            "available_beds_sample_count"
        ),
    )

    return agg.withColumn("batch_id", F.lit(batch_id)).withColumn(
        "run_timestamp", F.lit(run_ts).cast(TimestampType())
    ).select(
        "bucket_start",
        "region",
        "symptom_count",
        "clinic_visit_count",
        "admission_count",
        "severity_sum",
        "severity_count",
        "duration_days_sum",
        "duration_days_count",
        "temperature_sum",
        "temperature_count",
        "available_beds_min",
        "available_beds_avg",
        "available_beds_last",
        "available_beds_sample_count",
        "batch_id",
        "run_timestamp",
    )


def build_feature_clinic_hourly(
    df_clinic: DataFrame,
    batch_id: str,
    run_ts: datetime,
) -> DataFrame:
    """Natural key: (bucket_start, clinic_id)."""
    beds_last = F.expr("max_by(available_beds, cast(event_ts as timestamp))")
    agg = df_clinic.groupBy("bucket_start", "clinic_id").agg(
        F.count(F.lit(1)).alias("visit_count"),
        F.sum(F.when(F.col("follow_up_required"), 1).otherwise(0)).alias(
            "follow_up_true_count"
        ),
        F.sum("temperature_f").alias("temperature_sum"),
        F.count(F.col("temperature_f")).alias("temperature_count"),
        F.min("available_beds").alias("available_beds_min"),
        F.avg("available_beds").alias("available_beds_avg"),
        beds_last.alias("available_beds_last"),
        F.sum(F.when(F.col("available_beds").isNotNull(), 1).otherwise(0)).alias(
            "available_beds_sample_count"
        ),
    )
    return agg.withColumn("batch_id", F.lit(batch_id)).withColumn(
        "run_timestamp", F.lit(run_ts).cast(TimestampType())
    ).select(
        "bucket_start",
        "clinic_id",
        "visit_count",
        "follow_up_true_count",
        "temperature_sum",
        "temperature_count",
        "available_beds_min",
        "available_beds_avg",
        "available_beds_last",
        "available_beds_sample_count",
        "batch_id",
        "run_timestamp",
    )


def write_jdbc(df: DataFrame, table: str, jdbc_url: str, user: str, password: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .mode("append")
        .save()
    )


def main() -> int:
    run_id = _optional_env("RUN_ID") or str(uuid.uuid4())
    batch_id = _optional_env("BATCH_ID") or run_id
    run_ts = datetime.now(timezone.utc)

    kafka_bootstrap = _env("KAFKA_BOOTSTRAP")
    topic_symptom = _optional_env("TOPIC_SYMPTOM", DEFAULT_SYMPTOM_TOPIC) or DEFAULT_SYMPTOM_TOPIC
    topic_clinic = _optional_env("TOPIC_CLINIC", DEFAULT_CLINIC_TOPIC) or DEFAULT_CLINIC_TOPIC
    topic_hospital = (
        _optional_env("TOPIC_HOSPITAL", DEFAULT_HOSPITAL_TOPIC) or DEFAULT_HOSPITAL_TOPIC
    )

    jdbc_url = _env("JDBC_URL")
    jdbc_user = _env("JDBC_USER")
    jdbc_password = _env("JDBC_PASSWORD")

    log_analytics(
        {
            "task": "spark-etl",
            "phase": "preflight",
            "outcome": "valid",
            "reason_code": None,
            "run_id": run_id,
            "batch_id": batch_id,
        }
    )

    spark: SparkSession | None = None
    try:
        spark = build_spark()
        kafka_df = read_typed_kafka(
            spark, kafka_bootstrap, topic_symptom, topic_clinic, topic_hospital
        )
        parsed = parse_kafka_to_payload(kafka_df)
        parsed_bucketed = with_event_time_and_bucket(parsed)

        # --- Preliminaries: reuse these for all Layer A facts ---
        df_symptoms = preliminary_symptoms(parsed_bucketed, topic_symptom)
        df_clinic = preliminary_clinic_visits(parsed_bucketed, topic_clinic)
        df_hospital = preliminary_hospital_admissions(parsed_bucketed, topic_hospital)

        # ------------------------------------------------------------------
        # feature_hospital_hourly  (TODO)
        #   Source: preliminary_hospital_admissions (df_hospital).
        #   groupBy(bucket_start, hospital_id) → admission_count; severity_sum/count or
        #   severity histogram; temperature_sum/count; oxygen_sum/count;
        #   expected_los_sum/count; available_beds min/avg/last/sample_count (gauge).
        #   Add batch_id, run_timestamp; JDBC append to feature_hospital_hourly.
        # ------------------------------------------------------------------

        # ------------------------------------------------------------------
        # feature_diagnosis_hourly  (TODO)
        #   Source: df_clinic (drop rows with null diagnosis_code if needed).
        #   groupBy(bucket_start, clinic_id, diagnosis_code) → diagnosis_visit_count;
        #   optional temperature_sum/count; beds gauge columns; batch metadata; JDBC.
        # ------------------------------------------------------------------

        # ------------------------------------------------------------------
        # feature_symptom_hourly  (TODO)
        #   Source: parsed_bucketed on symptom topic only; explode("symptoms") to
        #   symptom_token (filter null/empty). groupBy(bucket_start, region,
        #   symptom_token) → symptom_token_count; batch metadata; JDBC.
        # ------------------------------------------------------------------

        region_hourly = build_feature_region_hourly(
            df_symptoms, df_clinic, df_hospital, batch_id, run_ts
        )
        clinic_hourly = build_feature_clinic_hourly(df_clinic, batch_id, run_ts)

        write_jdbc(region_hourly, "feature_region_hourly", jdbc_url, jdbc_user, jdbc_password)
        write_jdbc(clinic_hourly, "feature_clinic_hourly", jdbc_url, jdbc_user, jdbc_password)

        log_analytics(
            {
                "task": "spark-etl",
                "phase": "summary",
                "outcome": "valid",
                "reason_code": None,
                "run_id": run_id,
                "batch_id": batch_id,
                "status": "healthy",
                "note": "feature_region_hourly and feature_clinic_hourly written",
            }
        )
        return 0
    except Exception as e:
        log_analytics(
            {
                "task": "spark-etl",
                "phase": "error",
                "outcome": "error",
                "reason_code": "error",
                "run_id": run_id,
                "batch_id": batch_id,
                "reason_detail": str(e)[:500],
            }
        )
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
