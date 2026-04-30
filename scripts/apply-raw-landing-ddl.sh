#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
DB_POD="${DB_POD:-timescaledb-0}"
DB_NAME="${DB_NAME:-epidemic}"
DB_USER="${DB_USER:-spark_writer}"

oc exec -i "$DB_POD" -n "$NAMESPACE" -- psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS raw_typed_events (
    topic             TEXT        NOT NULL,
    kafka_partition   INT         NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    event_type        TEXT,
    event_ts          TIMESTAMPTZ,
    payload_json      JSONB       NOT NULL,
    run_id            TEXT,
    batch_id          TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_raw_typed_events_event_ts
    ON raw_typed_events (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_raw_typed_events_ingested_at
    ON raw_typed_events (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_typed_events_event_type
    ON raw_typed_events (event_type, event_ts DESC);
SQL

echo "raw_typed_events DDL applied on ${DB_NAME} (${DB_POD})"
