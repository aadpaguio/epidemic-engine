#!/usr/bin/env python3
"""
Controlled-rate Kafka producer for the Phase 1 raw topic.

Produces JSON events compatible with validate-events (event_type, timestamp, region)
plus realistic optional fields for routable types. Intended to run in-cluster
(see docs/load-test-raw-kafka.md).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError as e:  # pragma: no cover
    print("Install kafka-python-ng: pip install kafka-python-ng", file=sys.stderr)
    raise SystemExit(1) from e

REGIONS = (
    "Boston",
    "Cambridge",
    "Worcester",
    "Springfield",
    "Providence",
    "Hartford",
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, "").strip()
    if v:
        return v
    if default is not None:
        return default
    raise RuntimeError(f"Missing required value for {name} (env or CLI)")


def _build_symptom(rng: random.Random) -> dict[str, Any]:
    return {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_type": "symptom_report",
        "timestamp": _iso_now(),
        "patient_id": f"P{rng.randint(10000, 99999)}",
        "region": rng.choice(REGIONS),
        "age": rng.randint(18, 95),
        "symptoms": rng.sample(
            ["cough", "fever", "fatigue", "nausea", "headache", "chills"],
            k=rng.randint(1, 3),
        ),
        "severity": rng.randint(1, 10),
        "duration_days": round(rng.uniform(0.5, 14.0), 1),
        "reported_via": rng.choice(["app", "phone", "clinic", "web"]),
        "available_beds": rng.randint(0, 50),
    }


def _build_clinic(rng: random.Random) -> dict[str, Any]:
    return {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_type": "clinic_visit",
        "timestamp": _iso_now(),
        "patient_id": f"P{rng.randint(10000, 99999)}",
        "region": rng.choice(REGIONS),
        "visit_id": f"V-{uuid.uuid4().hex[:12]}",
        "clinic_id": f"CL-{rng.randint(1, 200)}",
        "visit_type": rng.choice(["walk_in", "scheduled", "follow_up"]),
        "primary_complaint": rng.choice(
            ["respiratory", "fever", "injury", "checkup", "other"]
        ),
        "temperature_f": round(rng.uniform(96.5, 103.0), 1),
        "diagnosis_code": f"ICD-{rng.randint(100, 999)}",
        # Keep clinic payload aligned with Spark schema (BooleanType).
        "prescribed_medication": rng.choice([True, False]),
        "follow_up_required": rng.choice([True, False]),
        "available_beds": rng.randint(0, 50),
    }


def _build_hospital(rng: random.Random) -> dict[str, Any]:
    return {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_type": "hospital_admission",
        "timestamp": _iso_now(),
        "patient_id": f"P{rng.randint(10000, 99999)}",
        "region": rng.choice(REGIONS),
        "admission_id": f"A-{uuid.uuid4().hex[:12]}",
        "hospital_id": f"H-{rng.randint(1, 50)}",
        "admission_reason": rng.choice(
            ["respiratory_distress", "dehydration", "injury", "observation"]
        ),
        "severity": rng.randint(1, 10),
        "temperature_f": round(rng.uniform(97.0, 104.0), 1),
        "oxygen_level": round(rng.uniform(88.0, 99.0), 1),
        "expected_los_days": round(rng.uniform(1.0, 10.0), 1),
        "available_beds": rng.randint(0, 50),
    }


def _build_unroutable(rng: random.Random) -> dict[str, Any]:
    return {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_type": "general_health_report",
        "timestamp": _iso_now(),
        "patient_id": f"P{rng.randint(10000, 99999)}",
        "region": rng.choice(REGIONS),
        "data": {"note": "loadgen-unroutable"},
    }


def _pick_builder(rng: random.Random, unroutable_weight: float):
    if rng.random() < unroutable_weight:
        return _build_unroutable(rng)
    r = rng.random()
    if r < 0.45:
        return _build_symptom(rng)
    if r < 0.45 + 0.30:
        return _build_clinic(rng)
    return _build_hospital(rng)


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Produce synthetic epidemic events to the raw Kafka topic at a fixed rate."
    )
    p.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "").strip() or None,
        help="Kafka bootstrap servers (or env KAFKA_BOOTSTRAP)",
    )
    p.add_argument(
        "--topic",
        default=os.environ.get("LOADGEN_TOPIC", "").strip()
        or os.environ.get("TOPIC_RAW", "ds551-s26.team02.raw"),
        help="Raw topic name (or env LOADGEN_TOPIC / TOPIC_RAW)",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("LOADGEN_RATE", "10")),
        help="Target events per second (default 10, or LOADGEN_RATE)",
    )
    p.add_argument(
        "--count",
        type=int,
        default=None,
        help="Stop after this many events (or env LOADGEN_COUNT)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Run for this many seconds (or env LOADGEN_DURATION_SEC)",
    )
    p.add_argument(
        "--unroutable-pct",
        type=float,
        default=float(os.environ.get("LOADGEN_UNROUTABLE_PCT", "5")),
        help="Percent of events with unroutable event_type (default 5)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=(
            int(os.environ["LOADGEN_SEED"])
            if os.environ.get("LOADGEN_SEED", "").strip()
            else None
        ),
        help="RNG seed for reproducibility",
    )
    args = p.parse_args()

    count = args.count
    duration = args.duration
    if count is None and os.environ.get("LOADGEN_COUNT", "").strip():
        count = int(os.environ["LOADGEN_COUNT"])
    if duration is None and os.environ.get("LOADGEN_DURATION_SEC", "").strip():
        duration = float(os.environ["LOADGEN_DURATION_SEC"])
    if count is not None and duration is not None:
        p.error("Use only one of --count / LOADGEN_COUNT and --duration / LOADGEN_DURATION_SEC")
    if count is None and duration is None:
        p.error(
            "Set --count or --duration (or env LOADGEN_COUNT / LOADGEN_DURATION_SEC)"
        )

    bootstrap = args.bootstrap or _env("KAFKA_BOOTSTRAP", None)
    if args.rate <= 0:
        print("--rate must be positive", file=sys.stderr)
        return 2

    unr_w = max(0.0, min(100.0, args.unroutable_pct)) / 100.0

    if count is not None and count < 1:
        print("--count must be >= 1", file=sys.stderr)
        return 2
    if duration is not None and duration <= 0:
        print("--duration must be positive", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",
        compression_type=None,
    )

    interval = 1.0 / args.rate
    next_slot = time.perf_counter()
    t_start = time.perf_counter()
    wall_deadline = t_start + float(duration) if duration is not None else None

    futures: list[tuple[float, Any]] = []
    send_errors = 0
    n = 0

    try:
        while True:
            if count is not None and n >= count:
                break
            now = time.perf_counter()
            if wall_deadline is not None and now >= wall_deadline:
                break

            sleep_for = next_slot - now
            if sleep_for > 0:
                time.sleep(sleep_for)

            payload = _pick_builder(rng, unr_w)
            key = str(payload.get("event_id", ""))
            t_send = time.perf_counter()
            try:
                fut = producer.send(args.topic, key=key, value=payload)
                futures.append((t_send, fut))
            except KafkaError:
                send_errors += 1
            n += 1
            next_slot += interval
            # Avoid unbounded drift if the OS sleeps a long time
            if next_slot < time.perf_counter() - interval:
                next_slot = time.perf_counter()
    finally:
        producer.flush()

    lat_ms: list[float] = []
    ack_errors = 0
    for t_send, fut in futures:
        try:
            fut.get(timeout=120)
            lat_ms.append((time.perf_counter() - t_send) * 1000.0)
        except Exception:
            ack_errors += 1

    producer.close()

    t_end = time.perf_counter()
    elapsed = max(t_end - t_start, 1e-9)
    actual_rate = n / elapsed

    lat_ms.sort()
    summary = {
        "tag": "LOADGEN",
        "topic": args.topic,
        "bootstrap": bootstrap,
        "target_rate": args.rate,
        "events_sent": n,
        "send_errors": send_errors,
        "ack_errors": ack_errors,
        "ack_samples": len(lat_ms),
        "duration_wall_s": round(elapsed, 3),
        "actual_rate_per_s": round(actual_rate, 3),
        "ack_latency_ms_p50": round(_percentile(lat_ms, 50), 3),
        "ack_latency_ms_p95": round(_percentile(lat_ms, 95), 3),
        "ack_latency_ms_p99": round(_percentile(lat_ms, 99), 3),
        "unroutable_pct": args.unroutable_pct,
    }
    print(json.dumps(summary), flush=True)
    return 0 if send_errors == 0 and ack_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
