"""Canonical Kafka typed-topic payloads aligned with `spark/etl.py` expectations."""

from __future__ import annotations

from typing import Any

# Default topic names (must match spark/etl.py defaults)
TOPIC_SYMPTOM = "ds551-s26.team02.symptom_reports"
TOPIC_CLINIC = "ds551-s26.team02.clinic_visits"
TOPIC_HOSPITAL = "ds551-s26.team02.hospital_admissions"
TOPIC_VACCINATION = "ds551-s26.team02.vaccination_records"


def base_timestamp_iso() -> str:
    """Fixed UTC timestamp for deterministic tests."""
    return "2026-04-30T14:30:00Z"


def symptom_report_payload(ts: str | None = None, region: str = "Boston") -> dict[str, Any]:
    return {
        "event_id": "fx-symptom-1",
        "event_type": "symptom_report",
        "timestamp": ts or base_timestamp_iso(),
        "region": region,
        "patient_id": "P1",
        "age": 40,
        "symptoms": ["fever", "cough"],
        "severity": "moderate",
        "duration_days": 2.0,
        "reported_via": "test",
        "available_beds": 5,
    }


def clinic_visit_payload(ts: str | None = None, region: str = "Boston") -> dict[str, Any]:
    return {
        "event_id": "fx-clinic-1",
        "event_type": "clinic_visit",
        "timestamp": ts or base_timestamp_iso(),
        "region": region,
        "patient_id": "P2",
        "visit_id": "V1",
        "clinic_id": "CLIN-A",
        "visit_type": "walk_in",
        "primary_complaint": "fever",
        "temperature_f": 101.0,
        "diagnosis_code": "ICD-X",
        "prescribed_medication": True,
        "follow_up_required": True,
        "available_beds": 6,
    }


def hospital_admission_payload(ts: str | None = None, region: str = "Boston") -> dict[str, Any]:
    return {
        "event_id": "fx-hosp-1",
        "event_type": "hospital_admission",
        "timestamp": ts or base_timestamp_iso(),
        "region": region,
        "patient_id": "P3",
        "admission_id": "A1",
        "hospital_id": "HOSP-1",
        "admission_reason": "observation",
        "severity": "severe",
        "temperature_f": 100.0,
        "oxygen_level": 95.0,
        "expected_los_days": 2.0,
        "available_beds": 10,
    }


def vaccination_payload_v2(ts: str | None = None, region: str = "Boston") -> dict[str, Any]:
    t = ts or base_timestamp_iso()
    return {
        "event_id": "fx-vax-1",
        "event_type": "vaccination_record",
        "schema_version": 2,
        "timestamp": t,
        "region": region,
        "patient_id": "P4",
        "record_id": "VR-1",
        "vaccine_type": "influenza",
        "dose_number": 1,
        "administered_at": t,
    }


def vaccination_payload_missing_timestamp_administered(region: str = "Boston") -> dict[str, Any]:
    """Only administered_at — raw ingester still reads `timestamp` for event_ts column."""
    return {
        "event_id": "fx-vax-no-ts",
        "event_type": "vaccination_record",
        "schema_version": 2,
        "region": region,
        "patient_id": "P5",
        "record_id": "VR-2",
        "vaccine_type": "influenza",
        "dose_number": 2,
        "administered_at": base_timestamp_iso(),
    }


def historical_timestamp_iso() -> str:
    """Older event hour than wall clock — tests event-time vs ingest-time."""
    return "2026-04-01T08:15:00Z"


malformed_json_string = '{"broken": '
