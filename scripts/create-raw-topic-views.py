"""Uses the raw_typed_events table to create a dedicated view for each topic.
Different from the tables produced by the spark script: views show raw data, 
not bucketed by hour"""

import logging
from textwrap import dedent

from db import get_connection

logger = logging.getLogger(__name__)


def create_view(view_name: str, view_query: str, replace: bool = True) -> None:
    """Create or replace a SQL view using the shared database connection."""
    if not view_name:
        raise ValueError("view_name must be provided")
    if not view_query:
        raise ValueError("view_query must be provided")

    statement = (
        f"CREATE OR REPLACE VIEW {view_name} AS\n{view_query}" if replace
        else f"CREATE VIEW {view_name} AS\n{view_query}"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement)
        conn.commit()

    logger.info("Created view %s", view_name)


def main() -> None:
    views = [
        {
            "name": "public.vw_clinic_visit_raw",
            "query": """
            SELECT kafka_offset,
                event_ts,
                ingested_at,
                payload_json ->> 'region'::text AS region,
                payload_json ->> 'team_id'::text AS team_id,
                payload_json ->> 'event_id'::text AS event_id,
                payload_json ->> 'visit_id'::text AS visit_id,
                payload_json ->> 'clinic_id'::text AS clinic_id,
                payload_json ->> 'event_type'::text AS event_type,
                payload_json ->> 'patient_id'::text AS patient_id,
                payload_json ->> 'visit_type'::text AS visit_type,
                payload_json ->> 'event_source'::text AS event_source,
                (payload_json ->> 'temperature_f'::text)::real AS temperature_f,
                (payload_json ->> 'available_beds'::text)::integer AS available_beds,
                payload_json ->> 'diagnosis_code'::text AS diagnosis_code,
                payload_json ->> 'primary_complaint'::text AS primary_complaint,
                (payload_json ->> 'follow_up_required'::text)::boolean AS follow_up_required,
                (payload_json ->> 'prescribed_medication'::text)::boolean AS prescribed_medication,
                (to_timestamp(payload_json ->> 'timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS "timestamp",
                (to_timestamp(payload_json ->> 'processing_timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS processing_timestamp
            FROM raw_typed_events rte
            WHERE event_type = 'clinic_visit'::text;
            """,
        },
        {
            "name": "public.vw_hospital_admission_raw",
            "query": """
            SELECT kafka_offset,
                event_ts,
                ingested_at,
                payload_json ->> 'region'::text AS region,
                payload_json ->> 'event_id'::text AS event_id,
                payload_json ->> 'severity'::text AS severity,
                payload_json ->> 'event_type'::text AS event_type,
                payload_json ->> 'patient_id'::text AS patient_id,
                payload_json ->> 'hospital_id'::text AS hospital_id,
                payload_json ->> 'admission_id'::text AS admission_id,
                payload_json ->> 'event_source'::text AS event_source,
                (payload_json ->> 'oxygen_level'::text)::real AS oxygen_level,
                (payload_json ->> 'temperature_f'::text)::real AS temperature_f,
                (payload_json ->> 'available_beds'::text)::integer AS available_beds,
                payload_json ->> 'admission_reason'::text AS admission_reason,
                (payload_json ->> 'expected_los_days'::text)::integer AS expected_los_days,
                (to_timestamp(payload_json ->> 'timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS "timestamp",
                (to_timestamp(payload_json ->> 'processing_timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS processing_timestamp
            FROM raw_typed_events rte
            WHERE event_type = 'hospital_admission'::text;
            """,
        },
        {
            "name": "public.vw_symptom_report_raw",
            "query": """
            SELECT kafka_offset,
                event_ts,
                ingested_at,
                (payload_json ->> 'age'::text)::integer AS age,
                payload_json ->> 'region'::text AS region,
                payload_json ->> 'event_id'::text AS event_id,
                payload_json ->> 'severity'::text AS severity,
                payload_json ->> 'symptoms'::text AS symptoms,
                payload_json ->> 'event_type'::text AS event_type,
                payload_json ->> 'patient_id'::text AS patient_id,
                payload_json ->> 'event_source'::text AS event_source,
                payload_json ->> 'reported_via'::text AS reported_via,
                (payload_json ->> 'duration_days'::text)::integer AS duration_days,
                (payload_json ->> 'available_beds'::text)::integer AS available_beds,
                (to_timestamp(payload_json ->> 'timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS "timestamp",
                (to_timestamp(payload_json ->> 'processing_timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS processing_timestamp
            FROM raw_typed_events rte
            WHERE event_type = 'symptom_report'::text;
            """,
        },
        {
            "name": "public.vw_vaccination_record_raw",
            "query": """
            SELECT kafka_offset,
                event_ts,
                ingested_at,
                payload_json ->> 'region'::text AS region,
                payload_json ->> 'event_id'::text AS event_id,
                payload_json ->> 'record_id'::text AS record_id,
                (to_timestamp(payload_json ->> 'timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS "timestamp",
                payload_json ->> 'event_type'::text AS event_type,
                payload_json ->> 'patient_id'::text AS patient_id,
                (payload_json ->> 'dose_number'::text)::integer AS dose_number,
                payload_json ->> 'event_source'::text AS event_source,
                payload_json ->> 'vaccine_type'::text AS vaccine_type,
                payload_json ->> 'schema_version'::text AS schema_version,
                (to_timestamp(payload_json ->> 'administered_at'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS administered_at,
                (to_timestamp(payload_json ->> 'processing_timestamp'::text, 'YYYY-MM-DDTHH24:MI:SS'::text)::timestamp without time zone AT TIME ZONE 'EST'::text) AS processing_timestamp
            FROM raw_typed_events rte
            WHERE event_type = 'vaccination_record'::text;
            """,
        },
    ]

    for view in views:
        create_view(view["name"], dedent(view["query"]).strip())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
