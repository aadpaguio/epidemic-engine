"""
Phase 2 Spark batch ETL: read typed Kafka topics, build hourly fact tables (Layer A).

Preliminaries: parsed, hour-bucketed event DataFrames per stream. Two facts implemented
here; three more are outlined in comment blocks below.
"""

from __future__ import annotations

import json
import os
import sys
import time
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


def _env_truthy(name: str) -> bool:
    return _optional_env(name, "").lower() in ("1", "true", "yes", "on")


def _jar_list_env(name: str) -> list[str]:
    raw = _optional_env(name)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


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

# PERMISSIVE JSON: malformed documents populate this column (name must match option).
_CORRUPT_RECORD = "_corrupt_record"
PAYLOAD_SCHEMA_PERMISSIVE = StructType(
    list(PAYLOAD_SCHEMA.fields) + [StructField(_CORRUPT_RECORD, StringType(), True)]
)
_JSON_READ_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": _CORRUPT_RECORD,
}


def correlation_context() -> dict[str, Any]:
    """Optional K8s / run correlation (Phase 2 observability)."""
    ctx: dict[str, Any] = {}
    pod = _optional_env("K8S_POD_NAME") or _optional_env("HOSTNAME")
    if pod:
        ctx["k8s_pod"] = pod
    ns = _optional_env("K8S_NAMESPACE")
    if ns:
        ctx["k8s_namespace"] = ns
    attempt = _optional_env("K8S_JOB_COMPLETION_INDEX") or _optional_env("CLOUD_RUN_TASK_INDEX")
    if attempt:
        ctx["attempt"] = attempt
    return ctx


def log_analytics(payload: dict[str, Any]) -> None:
    """Emit one [ANALYTICS] line; payload must include plan-required envelope keys."""
    required = ("task", "phase", "outcome", "reason_code", "run_id", "batch_id")
    for k in required:
        if k not in payload:
            raise ValueError(f"log_analytics missing key: {k}")
    merged = {**correlation_context(), **payload}
    line = "[ANALYTICS] " + json.dumps(merged, default=str)
    print(line, flush=True)


def classify_exception(exc: Exception) -> str:
    """Map common failures to Phase 2 reason_code taxonomy (best-effort)."""
    msg = str(exc).lower()
    combined = f"{type(exc).__name__} {msg}".lower()
    if "kafka" in combined or "broker" in combined:
        return "KAFKA_READ_FAILED"
    if "connect" in combined and ("refused" in combined or "timed out" in combined):
        return "KAFKA_CONNECT_FAILED"
    if "jdbc" in combined or "postgresql" in combined or "psql" in combined:
        if "connect" in combined or "refused" in combined or "timeout" in combined:
            return "DB_CONNECT_FAILED"
        return "DB_WRITE_FAILED"
    return "error"


def build_spark(app_name: str = "epidemic-phase2-etl") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC")
    )
    spark_jars = []
    spark_jars.extend(_jar_list_env("SPARK_EXTRA_JARS"))
    spark_jars.extend(_jar_list_env("SPARK_JDBC_JAR"))
    if spark_jars:
        builder = builder.config("spark.jars", ",".join(spark_jars))
    return builder.getOrCreate()


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
    """Parse JSON with PERMISSIVE mode; never fail the job on a single bad document."""
    d = F.from_json(
        F.col("value").cast("string"),
        PAYLOAD_SCHEMA_PERMISSIVE,
        _JSON_READ_OPTIONS,
    )
    return kafka_df.select(
        "topic",
        "partition",
        "offset",
        F.col("value").isNull().alias("kafka_value_is_null"),
        d.alias("data"),
    ).select(
        "topic",
        "partition",
        "offset",
        "kafka_value_is_null",
        F.col(f"data.{_CORRUPT_RECORD}").isNotNull().alias("json_corrupt"),
        F.col("data.event_type").alias("event_type"),
        F.col("data.timestamp").alias("ts_raw"),
        F.col("data.region").alias("region"),
        F.col("data.age").cast("int").alias("age"),
        F.col("data.symptoms").alias("symptoms"),
        F.col("data.severity").cast("int").alias("severity"),
        F.col("data.duration_days").cast("double").alias("duration_days"),
        F.col("data.reported_via").alias("reported_via"),
        # visit_id
        F.col("data.clinic_id").alias("clinic_id"),
        F.col("data.visit_type").alias("visit_type"),
        F.col("data.primary_complaint").alias("primary_complaint"),
        F.col("data.temperature_f").cast("double").alias("temperature_f"),
        F.col("data.diagnosis_code").alias("diagnosis_code"),
        F.col("data.prescribed_medication").alias("prescribed_medication"),
        F.when(F.lower(F.col("data.follow_up_required").cast("string")) == "true", F.lit(True))
        .when(F.lower(F.col("data.follow_up_required").cast("string")) == "false", F.lit(False))
        .otherwise(F.col("data.follow_up_required").cast("boolean"))
        .alias("follow_up_required"),
        # admission_id
        F.col("data.hospital_id").alias("hospital_id"),
        F.col("data.admission_reason").alias("admission_reason"),
        F.col("data.oxygen_level").cast("double").alias("oxygen_level"),
        F.col("data.expected_los_days").cast("double").alias("expected_los_days"),
        F.col("data.available_beds").cast("double").alias("available_beds"),
    )


def with_event_time_and_bucket(
    parsed: DataFrame,
    topic_symptom: str,
    topic_clinic: str,
    topic_hospital: str,
) -> DataFrame:
    ts = F.to_timestamp(F.col("ts_raw"))
    ts_trim = F.trim(F.col("ts_raw").cast("string"))
    has_ts_text = ts_trim.isNotNull() & (F.length(ts_trim) > 0)
    time_parse_ok = ts.isNotNull() & F.date_trunc("hour", ts).isNotNull()

    base = (
        parsed.withColumn("event_ts", ts)
        .withColumn("bucket_start", F.date_trunc("hour", ts))
        .withColumn("parse_ok", time_parse_ok)
    )

    skip_reason = (
        F.when(F.col("kafka_value_is_null"), F.lit("NULL_KAFKA_VALUE"))
        .when(F.col("json_corrupt"), F.lit("JSON_PARSE"))
        .when(~has_ts_text, F.lit("MISSING_FIELD:timestamp"))
        .when(has_ts_text & ~F.col("parse_ok"), F.lit("BAD_TIMESTAMP"))
        .when(
            (F.col("topic") == F.lit(topic_symptom)) & F.col("parse_ok") & F.col("region").isNull(),
            F.lit("MISSING_FIELD:region"),
        )
        .when(
            (F.col("topic") == F.lit(topic_clinic)) & F.col("parse_ok") & F.col("region").isNull(),
            F.lit("MISSING_FIELD:region"),
        )
        .when(
            (F.col("topic") == F.lit(topic_clinic))
            & F.col("parse_ok")
            & F.col("clinic_id").isNull(),
            F.lit("MISSING_FIELD:clinic_id"),
        )
        .when(
            (F.col("topic") == F.lit(topic_hospital)) & F.col("parse_ok") & F.col("region").isNull(),
            F.lit("MISSING_FIELD:region"),
        )
        .when(
            (F.col("topic") == F.lit(topic_hospital))
            & F.col("parse_ok")
            & F.col("hospital_id").isNull(),
            F.lit("MISSING_FIELD:hospital_id"),
        )
        .otherwise(F.lit("ACCEPT"))
    )

    return base.withColumn("skip_reason", skip_reason)


def log_kafka_read_slices(
    kafka_df: DataFrame,
    run_id: str,
    batch_id: str,
    expected_topics: tuple[str, str, str],
) -> tuple[dict[str, dict[str, Any]], int]:
    """
    Per-topic partition offset slices for [ANALYTICS] kafka_read lines.
    Returns (rollup per topic that had rows, total_consumed).
    Topics with zero messages produce no Kafka rows; we still emit one line each with EMPTY_TOPIC.
    """
    slices = (
        kafka_df.groupBy("topic", "partition")
        .agg(
            F.min("offset").alias("start_offset"),
            F.max("offset").alias("end_offset"),
            F.count(F.lit(1)).alias("records_read"),
        )
        .collect()
    )

    by_topic: dict[str, dict[str, Any]] = {}
    for row in slices:
        t = row["topic"]
        if t not in by_topic:
            by_topic[t] = {"partitions": [], "records_read": 0}
        by_topic[t]["partitions"].append(
            {
                "partition": int(row["partition"]),
                "start_offset": int(row["start_offset"]),
                "end_offset": int(row["end_offset"]),
                "records_read": int(row["records_read"]),
            }
        )
        by_topic[t]["records_read"] += int(row["records_read"])

    total_consumed = sum(info["records_read"] for info in by_topic.values())

    for t in expected_topics:
        if t in by_topic:
            info = by_topic[t]
            payload: dict[str, Any] = {
                "task": "spark-etl",
                "phase": "kafka_read",
                "outcome": "valid",
                "reason_code": None,
                "run_id": run_id,
                "batch_id": batch_id,
                "topic": t,
                "records_read": info["records_read"],
                "partition_count": len(info["partitions"]),
                "partition_slices": info["partitions"],
            }
            if info["records_read"] == 0:
                payload["reason_code"] = "EMPTY_TOPIC"
            log_analytics(payload)
        else:
            log_analytics(
                {
                    "task": "spark-etl",
                    "phase": "kafka_read",
                    "outcome": "valid",
                    "reason_code": "EMPTY_TOPIC",
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "topic": t,
                    "records_read": 0,
                    "partition_count": 0,
                    "partition_slices": [],
                }
            )

    return by_topic, total_consumed


def collect_parse_stats(
    bucketed: DataFrame,
    expected_topics: tuple[str, str, str],
) -> dict[str, Any]:
    """Aggregation for skip_reason counts and event-time watermarks (bucketed must be cached)."""
    total = bucketed.count()
    agg_row = bucketed.groupBy("skip_reason").agg(F.count(F.lit(1)).alias("c")).collect()
    by_reason = {r["skip_reason"]: int(r["c"]) for r in agg_row}
    accept = int(by_reason.get("ACCEPT", 0))
    skipped = total - accept

    wm = (
        bucketed.filter(F.col("skip_reason") == F.lit("ACCEPT"))
        .groupBy("topic")
        .agg(F.min("event_ts").alias("min_event_ts"), F.max("event_ts").alias("max_event_ts"))
        .collect()
    )
    watermarks = {
        r["topic"]: {
            "min_event_ts": r["min_event_ts"],
            "max_event_ts": r["max_event_ts"],
        }
        for r in wm
    }

    topic_counts = bucketed.groupBy("topic").agg(F.count(F.lit(1)).alias("n")).collect()
    topic_n = {r["topic"]: int(r["n"]) for r in topic_counts}
    empty_topics = [t for t in expected_topics if topic_n.get(t, 0) == 0]

    return {
        "total_rows": total,
        "parse_ok_count": accept,
        "skipped_count": skipped,
        "skip_by_reason": by_reason,
        "watermarks_by_topic": watermarks,
        "empty_topics": empty_topics,
    }


def preliminary_symptoms(parsed_bucketed: DataFrame, topic_symptom: str) -> DataFrame:
    """Hour-bucketed symptom_report rows (valid parse + expected topic)."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_symptom)) & (F.col("skip_reason") == F.lit("ACCEPT"))
        )
        .select(
            F.lit("symptom_report").alias("event_type"),
            "bucket_start",
            "region",
            "event_ts",
            "age",
            # symptoms (list) TODO add count of symptoms?
            "severity",
            "duration_days",
            "reported_via",
            "available_beds"
        )
    )


def preliminary_clinic_visits(parsed_bucketed: DataFrame, topic_clinic: str) -> DataFrame:
    """Hour-bucketed clinic_visit rows."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_clinic)) & (F.col("skip_reason") == F.lit("ACCEPT"))
        )
        .select(
            F.lit("clinic_visit").alias("event_type"),
            "bucket_start",
            "region",
            "clinic_id",
            "event_ts",
            "visit_type",
            "primary_complaint",
            "temperature_f",
            "diagnosis_code",
            "prescribed_medication",
            "follow_up_required",
            "available_beds"
        )
    )


def preliminary_hospital_admissions(
    parsed_bucketed: DataFrame, topic_hospital: str
) -> DataFrame:
    """Hour-bucketed hospital_admission rows."""
    return (
        parsed_bucketed.filter(
            (F.col("topic") == F.lit(topic_hospital)) & (F.col("skip_reason") == F.lit("ACCEPT"))
        )
        .select(
            F.lit("hospital_admission").alias("event_type"),
            "bucket_start",
            "region",
            "hospital_id",
            "event_ts",
            "admission_reason",
            "severity",
            "temperature_f",
            "oxygen_level",
            "expected_los_days",
            "available_beds"
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
        F.lit(None).cast("double").alias("temperature_f"),
        "available_beds"
    )
    cli = df_clinic.select(
        "bucket_start",
        "region",
        "event_type",
        "event_ts",
        F.lit(None).cast("int").alias("severity"),
        F.lit(None).cast("double").alias("duration_days"),
        "temperature_f",
        "available_beds"
    )
    hos = df_hospital.select(
        "bucket_start",
        "region",
        "event_type",
        "event_ts",
        "severity",
        # Hospital stream uses expected_los_days, not duration_days; null for union schema.
        F.lit(None).cast("double").alias("duration_days"),
        "temperature_f",
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
        F.max("region").alias("region"),
        F.count(F.lit(1)).alias("visit_count"),
        F.sum(F.when(F.col("follow_up_required"), 1).otherwise(0)).alias(
            "follow_up_true_count" ),
        F.avg("temperature_f").alias("temperature_avg"),
        F.var("temperature_f").alias("temperature_var"),
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
        "region",
        "visit_count",
        "follow_up_true_count",
        "temperature_avg",
        "temperature_var",
        "temperature_count",
        "available_beds_min",
        "available_beds_avg",
        "available_beds_last",
        "available_beds_sample_count",
        "batch_id",
        "run_timestamp",
    )


def build_feature_hospital_hourly(
    df_hospital: DataFrame,
    batch_id: str,
    run_ts: datetime,
) -> DataFrame:
    """Natural key: (bucket_start, hospital_id)."""
    beds_last = F.expr("max_by(available_beds, cast(event_ts as timestamp))")
    agg = df_hospital.groupBy("bucket_start", "hospital_id").agg(
        F.max("region").alias("region"),
        F.count(F.lit(1)).alias("admission_count"),
        F.sum("severity").alias("severity_sum"),
        F.avg("severity").alias("severity_avg"),
        F.var("severity").alias("severity_var"),
        F.count(F.col("severity")).alias("severity_count"),
        F.avg("temperature_f").alias("temperature_avg"),
        F.var("temperature_f").alias("temperature_var"),
        F.count(F.col("temperature_f")).alias("temperature_count"),
        F.avg("oxygen_level").alias("oxygen_avg"),
        F.var("oxygen_level").alias("oxygen_var"),
        F.count(F.col("oxygen_level")).alias("oxygen_count"),
        F.sum("expected_los_days").alias("expected_los_sum"),
        F.count(F.col("expected_los_days")).alias("expected_los_count"),
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
        "hospital_id",
        "region",
        "admission_count",
        "severity_sum",
        "severity_avg",
        "severity_var",
        "severity_count",
        "temperature_avg",
        "temperature_var",
        "temperature_count",
        "oxygen_avg",
        "oxygen_var",
        "oxygen_count",
        "expected_los_sum",
        "expected_los_count",
        "available_beds_min",
        "available_beds_avg",
        "available_beds_last",
        "available_beds_sample_count",
        "batch_id",
        "run_timestamp",
    )

def build_feature_diagnosis_hourly(
    df_clinic: DataFrame,
    batch_id: str,
    run_ts: datetime,
) -> DataFrame:
    """Natural key: (bucket_start, region, diagnosis_code)."""
    agg = df_clinic.groupBy("bucket_start", "region", "diagnosis_code").agg(
        F.count(F.lit(1)).alias("diagnosis_visit_count"),
        F.sum(F.when(F.col("follow_up_required"), 1).otherwise(0)).alias(
            "follow_up_true_count" ),
        F.countDistinct("clinic_id").alias("clinic_id_unique"),
        F.countDistinct("visit_type").alias("visit_type_unique"),
        F.avg("temperature_f").alias("temperature_avg"),
        F.var("temperature_f").alias("temperature_var"),
        F.count(F.col("temperature_f")).alias("temperature_count")
    )
    return agg.withColumn("batch_id", F.lit(batch_id)).withColumn(
        "run_timestamp", F.lit(run_ts).cast(TimestampType())
    ).select(
        "bucket_start",
        "region",
        "diagnosis_code",
        "clinic_id_unique",
        "diagnosis_visit_count",
        "visit_type_unique",
        "temperature_avg",
        "temperature_var",
        "temperature_count",
        "follow_up_true_count"
    )


def build_feature_symptom_hourly(
    df_symptoms: DataFrame,
    batch_id: str,
    run_ts: datetime,    
) -> DataFrame:
    """Natural key: (bucket_start, region, symptom_token)."""
    agg = df_symptoms.groupBy("bucket_start", "region", "symptom_token").agg(
        F.count(F.lit(1)).alias("symptom_visit_count"),
        F.avg("age").alias("age_avg"),
        F.var("age").alias("age_var"),
        F.count(F.col("age")).alias("age_count"),
        F.sum("severity").alias("severity_sum"),
        F.avg("severity").alias("severity_avg"),
        F.var("severity").alias("severity_var"),
        F.count(F.col("severity")).alias("severity_count"),
        F.avg("duration_days").alias("duration_days_avg"),
        F.var("duration_days").alias("duration_days_var"),
        F.count(F.col("duration_days")).alias("duration_days_count")
    )
    return agg.withColumn("batch_id", F.lit(batch_id)).withColumn(
        "run_timestamp", F.lit(run_ts).cast(TimestampType())
    ).select(
        "bucket_start",
        "region",
        "symptom_token",
        "symptom_visit_count",
        "age_avg",
        "age_var",
        "age_count",
        "severity_sum",
        "severity_avg",
        "severity_var",
        "severity_count",
        "duration_days_avg",
        "duration_days_var",
        "duration_days_count"
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


def write_jdbc_analytic(
    df: DataFrame,
    table: str,
    analytic_name: str,
    jdbc_url: str,
    user: str,
    password: str,
    run_id: str,
    batch_id: str,
) -> int:
    """Append to JDBC and emit one [ANALYTICS] db_write line; returns rows written."""
    t0 = time.perf_counter()
    rows_computed = df.count()
    write_jdbc(df, table, jdbc_url, user, password)
    duration_ms = int((time.perf_counter() - t0) * 1000)
    log_analytics(
        {
            "task": "spark-etl",
            "phase": "db_write",
            "outcome": "valid",
            "reason_code": None,
            "run_id": run_id,
            "batch_id": batch_id,
            "analytic_name": analytic_name,
            "target_table": table,
            "rows_computed": rows_computed,
            "rows_written": rows_computed,
            "write_mode": "append",
            "duration_ms": duration_ms,
        }
    )
    return rows_computed


def log_db_write_skipped(
    df: DataFrame,
    table: str,
    analytic_name: str,
    run_id: str,
    batch_id: str,
) -> int:
    """Materialize row count only; log db_write with write_mode dry_run (no JDBC)."""
    t0 = time.perf_counter()
    rows_computed = df.count()
    duration_ms = int((time.perf_counter() - t0) * 1000)
    log_analytics(
        {
            "task": "spark-etl",
            "phase": "db_write",
            "outcome": "valid",
            "reason_code": None,
            "run_id": run_id,
            "batch_id": batch_id,
            "analytic_name": analytic_name,
            "target_table": table,
            "rows_computed": rows_computed,
            "rows_written": 0,
            "write_mode": "dry_run",
            "duration_ms": duration_ms,
        }
    )
    return rows_computed


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

    skip_jdbc = _env_truthy("SKIP_JDBC_WRITE")
    jdbc_url = jdbc_user = jdbc_password = ""
    if not skip_jdbc:
        jdbc_url = _env("JDBC_URL")
        jdbc_user = _env("JDBC_USER")
        jdbc_password = _env("JDBC_PASSWORD")

    started_wall = time.perf_counter()
    started_at = datetime.now(timezone.utc)

    log_analytics(
        {
            "task": "spark-etl",
            "phase": "preflight",
            "outcome": "valid",
            "reason_code": None,
            "run_id": run_id,
            "batch_id": batch_id,
            "started_at": started_at.isoformat(),
            "skip_jdbc_write": skip_jdbc,
        }
    )

    spark: SparkSession | None = None
    parsed_bucketed: DataFrame | None = None
    try:
        spark = build_spark()
        expected_topics = (topic_symptom, topic_clinic, topic_hospital)

        kafka_df = read_typed_kafka(
            spark, kafka_bootstrap, topic_symptom, topic_clinic, topic_hospital
        )
        _, total_consumed = log_kafka_read_slices(
            kafka_df, run_id, batch_id, expected_topics
        )

        parsed = parse_kafka_to_payload(kafka_df)
        parsed_bucketed = with_event_time_and_bucket(
            parsed, topic_symptom, topic_clinic, topic_hospital
        ).cache()
        parse_stats = collect_parse_stats(parsed_bucketed, expected_topics)

        log_analytics(
            {
                "task": "spark-etl",
                "phase": "parse",
                "outcome": "valid",
                "reason_code": None,
                "run_id": run_id,
                "batch_id": batch_id,
                "total_consumed": total_consumed,
                "parse_ok_count": parse_stats["parse_ok_count"],
                "skipped_count": parse_stats["skipped_count"],
                "skip_by_reason": parse_stats["skip_by_reason"],
                "watermarks_by_topic": {
                    k: {
                        "min_event_ts": v["min_event_ts"],
                        "max_event_ts": v["max_event_ts"],
                    }
                    for k, v in parse_stats["watermarks_by_topic"].items()
                },
            }
        )

        # --- Preliminaries: reuse these for all Layer A facts ---
        df_symptoms = preliminary_symptoms(parsed_bucketed, topic_symptom)
        df_clinic = preliminary_clinic_visits(parsed_bucketed, topic_clinic)
        df_hospital = preliminary_hospital_admissions(parsed_bucketed, topic_hospital)

        exploded_symptoms = df_symptoms.withColumn("symptom_token", F.explode("symptoms")).filter(
            F.col("symptom_token").isNotNull() & (F.length(F.col("symptom_token")) > 0))

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
        hospital_hourly = build_feature_hospital_hourly(df_hospital, batch_id, run_ts)
        diagnosis_hourly = build_feature_diagnosis_hourly(
            df_clinic.filter(F.col("diagnosis_code").isNotNull()), batch_id, run_ts
        )
        symptom_hourly = build_feature_symptom_hourly(exploded_symptoms, batch_id, run_ts)

        if skip_jdbc:
            rows_region = log_db_write_skipped(
                region_hourly, "feature_region_hourly", "feature_region_hourly", run_id, batch_id
            )
            rows_clinic = log_db_write_skipped(
                clinic_hourly, "feature_clinic_hourly", "feature_clinic_hourly", run_id, batch_id
            )
            rows_hospital = log_db_write_skipped(
                hospital_hourly, "feature_hospital_hourly", "feature_hospital_hourly", run_id, batch_id
            )
            rows_diagnosis = log_db_write_skipped(
                diagnosis_hourly, "feature_diagnosis_hourly", "feature_diagnosis_hourly", run_id, batch_id
            )
            rows_symptom = log_db_write_skipped(
                symptom_hourly, "feature_symptom_hourly", "feature_symptom_hourly", run_id, batch_id
            )
            rows_written_total = 0
        else:
            rows_region = write_jdbc_analytic(
                region_hourly,
                "feature_region_hourly",
                "feature_region_hourly",
                jdbc_url,
                jdbc_user,
                jdbc_password,
                run_id,
                batch_id,
            )
            rows_clinic = write_jdbc_analytic(
                clinic_hourly,
                "feature_clinic_hourly",
                "feature_clinic_hourly",
                jdbc_url,
                jdbc_user,
                jdbc_password,
                run_id,
                batch_id,
            )
            rows_hospital = write_jdbc_analytic(
                hospital_hourly,
                "feature_hospital_hourly",
                "feature_hospital_hourly",
                jdbc_url,
                jdbc_user,
                jdbc_password,
                run_id,
                batch_id,
            )
            rows_diagnosis = write_jdbc_analytic(
                diagnosis_hourly,
                "feature_diagnosis_hourly",
                "feature_diagnosis_hourly",
                jdbc_url,
                jdbc_user,
                jdbc_password,
                run_id,
                batch_id,
            )
            rows_symptom = write_jdbc_analytic(
                symptom_hourly,
                "feature_symptom_hourly",
                "feature_symptom_hourly",
                jdbc_url,
                jdbc_user,
                jdbc_password,
                run_id,
                batch_id,
            )
            rows_written_total = rows_region + rows_clinic + rows_hospital + rows_diagnosis + rows_symptom

        rows_computed_total = (
            rows_region + rows_clinic + rows_hospital + rows_diagnosis + rows_symptom
        )

        completed_at = datetime.now(timezone.utc)
        duration_ms = int((time.perf_counter() - started_wall) * 1000)

        skip_pct = (
            (parse_stats["skipped_count"] / parse_stats["total_rows"] * 100.0)
            if parse_stats["total_rows"] > 0
            else 0.0
        )
        degrade_pct = float(_optional_env("ANALYTICS_SKIP_DEGRADE_PCT", "25") or "25")
        skip_degraded = parse_stats["total_rows"] > 0 and skip_pct > degrade_pct
        empty_degraded = len(parse_stats["empty_topics"]) > 0
        status = "degraded" if (skip_degraded or empty_degraded) else "healthy"

        log_analytics(
            {
                "task": "spark-etl",
                "phase": "summary",
                "outcome": "valid",
                "reason_code": None,
                "run_id": run_id,
                "batch_id": batch_id,
                "status": status,
                "total_consumed": total_consumed,
                "parse_ok_count": parse_stats["parse_ok_count"],
                "skipped_count": parse_stats["skipped_count"],
                "error_count": 0,
                "rows_written_total": rows_written_total,
                "rows_computed_total": rows_computed_total,
                "jdbc_skipped": skip_jdbc,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": duration_ms,
                "empty_topics": parse_stats["empty_topics"],
                "skip_rate_pct": round(skip_pct, 4),
            }
        )
        return 0
    except Exception as e:
        rc = classify_exception(e)
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((time.perf_counter() - started_wall) * 1000)
        log_analytics(
            {
                "task": "spark-etl",
                "phase": "summary",
                "outcome": "error",
                "reason_code": rc,
                "run_id": run_id,
                "batch_id": batch_id,
                "reason_detail": str(e)[:500],
                "status": "failed",
                "error_count": 1,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": duration_ms,
            }
        )
        raise
    finally:
        if parsed_bucketed is not None:
            try:
                parsed_bucketed.unpersist()
            except Exception:
                pass
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
