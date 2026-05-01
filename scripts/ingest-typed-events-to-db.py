#!/usr/bin/env python3
"""
Continuously ingest typed Kafka events into raw_typed_events.

This process is intentionally decoupled from feature aggregation so Spark can read
from Postgres/TimescaleDB instead of repeatedly scanning Kafka for each window.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from collections import Counter
from typing import Any

import psycopg2
from kafka import KafkaConsumer


DEFAULT_TOPICS = (
    "ds551-s26.team02.symptom_reports",
    "ds551-s26.team02.clinic_visits",
    "ds551-s26.team02.hospital_admissions",
    "ds551-s26.team02.vaccination_records",
)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if default is not None:
        return default
    raise RuntimeError(f"Missing required environment variable: {name}")


def parse_event_ts(payload: dict[str, Any]) -> datetime | None:
    raw = payload.get("timestamp")
    if not isinstance(raw, str) or not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def main() -> int:
    bootstrap = env("KAFKA_BOOTSTRAP")
    group_id = env("RAW_INGEST_GROUP_ID", "team02-raw-typed-ingest")
    db_host = env("DB_HOST", "timescaledb-0")
    db_port = int(env("DB_PORT", "5432"))
    db_name = env("DB_NAME", "epidemic")
    db_user = env("DB_USER", "spark_writer")
    db_password = env("DB_PASSWORD")
    poll_timeout_ms = int(env("POLL_TIMEOUT_MS", "1000"))
    flush_every = max(1, int(env("DB_FLUSH_EVERY", "200")))
    run_seconds = int(env("RUN_SECONDS", "0"))
    max_messages = int(env("MAX_MESSAGES", "0"))
    auto_offset_reset = env("AUTO_OFFSET_RESET", "latest")
    topics = [t.strip() for t in env("TOPICS", ",".join(DEFAULT_TOPICS)).split(",") if t.strip()]
    if not topics:
        raise RuntimeError("TOPICS must include at least one topic")

    print(
        "[raw-ingest] starting "
        f"bootstrap={bootstrap} group_id={group_id} topics={','.join(topics)} "
        f"auto_offset_reset={auto_offset_reset} run_seconds={run_seconds} "
        f"max_messages={max_messages} flush_every={flush_every}",
        flush=True,
    )

    running = True

    def stop_handler(_signum: int, _frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
        consumer_timeout_ms=0,
    )

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    conn.autocommit = False

    upsert_sql = """
    INSERT INTO raw_typed_events (
      topic,
      kafka_partition,
      kafka_offset,
      event_type,
      event_ts,
      payload_json,
      run_id,
      batch_id
    )
    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
    ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
    """

    processed = 0
    inserted = 0
    empty_polls = 0
    started = time.time()

    try:
        with conn.cursor() as cur:
            while running:
                if run_seconds > 0 and (time.time() - started) >= run_seconds:
                    break
                if max_messages > 0 and processed >= max_messages:
                    break
                records = consumer.poll(timeout_ms=poll_timeout_ms)
                if not records:
                    empty_polls += 1
                    if empty_polls == 1 or empty_polls % 10 == 0:
                        elapsed = int(time.time() - started)
                        print(
                            f"[raw-ingest] idle empty_polls={empty_polls} elapsed_sec={elapsed}",
                            flush=True,
                        )
                    continue

                empty_polls = 0
                batch_id = datetime.now(UTC).strftime("raw-ingest-%Y%m%dT%H%M%SZ")
                run_id = batch_id
                poll_processed = 0
                poll_inserted = 0
                poll_topics: Counter[str] = Counter()
                poll_event_hours: Counter[str] = Counter()
                for _tp, messages in records.items():
                    for msg in messages:
                        processed += 1
                        poll_processed += 1
                        payload = msg.value if isinstance(msg.value, dict) else {}
                        event_ts = parse_event_ts(payload)
                        poll_topics[msg.topic] += 1
                        if event_ts is not None:
                            poll_event_hours[event_ts.replace(minute=0, second=0, microsecond=0).isoformat()] += 1
                        cur.execute(
                            upsert_sql,
                            (
                                msg.topic,
                                msg.partition,
                                msg.offset,
                                payload.get("event_type"),
                                event_ts,
                                json.dumps(payload, separators=(",", ":")),
                                run_id,
                                batch_id,
                            ),
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                            poll_inserted += 1

                conn.commit()
                consumer.commit()
                if poll_processed > 0:
                    elapsed = max(1.0, time.time() - started)
                    print(
                        f"[raw-ingest] committed poll_processed={poll_processed} "
                        f"poll_inserted={poll_inserted} processed={processed} inserted={inserted} "
                        f"rate={processed/elapsed:.1f}/s topics={dict(poll_topics)} "
                        f"event_hours={dict(poll_event_hours)}",
                        flush=True,
                    )

            conn.commit()
            consumer.commit()
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    elapsed = max(1.0, time.time() - started)
    print(
        f"[raw-ingest] stopped processed={processed} inserted={inserted} rate={processed/elapsed:.1f}/s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[raw-ingest] fatal: {exc}", file=sys.stderr)
        raise
