-- Idempotent: safe to re-run. Matches infra/database-init.sql (keep in sync).
-- Lightweight DDL only — no table rewrites, suitable for tight OpenShift quotas.

CREATE OR REPLACE VIEW vw_data_quality_hourly AS
SELECT
    bucket_start,
    total_events,
    missing_event_type,
    missing_event_ts,
    missing_region,
    missing_severity,
    invalid_severity,
    quality_issue_count,
    CASE
        WHEN total_events > 0 THEN
            ROUND((100.0 * quality_issue_count::double precision / total_events::double precision)::numeric, 2)
    END AS quality_issue_pct,
    CASE
        WHEN total_events > 0 THEN
            ROUND((100.0 * missing_severity::double precision / total_events::double precision)::numeric, 2)
    END AS missing_severity_pct
FROM (
    SELECT
        date_trunc('hour', COALESCE(r.event_ts, r.ingested_at)) AS bucket_start,
        COUNT(*)::bigint AS total_events,
        COUNT(*) FILTER (
            WHERE r.event_type IS NULL OR trim(r.event_type) = ''
        )::bigint AS missing_event_type,
        COUNT(*) FILTER (WHERE r.event_ts IS NULL)::bigint AS missing_event_ts,
        COUNT(*) FILTER (
            WHERE NOT (r.payload_json ? 'region')
               OR trim(COALESCE(r.payload_json->>'region', '')) = ''
        )::bigint AS missing_region,
        COUNT(*) FILTER (
            WHERE NOT (r.payload_json ? 'severity')
               OR trim(COALESCE(r.payload_json->>'severity', '')) = ''
        )::bigint AS missing_severity,
        COUNT(*) FILTER (
            WHERE r.payload_json ? 'severity'
              AND trim(COALESCE(r.payload_json->>'severity', '')) <> ''
              AND lower(trim(r.payload_json->>'severity')) NOT IN (
                  'mild', 'moderate', 'severe', 'critical'
              )
              AND NOT (r.payload_json->>'severity' ~ '^[0-9]+(\.[0-9]+)?$')
        )::bigint AS invalid_severity,
        COUNT(*) FILTER (
            WHERE (r.event_type IS NULL OR trim(r.event_type) = '')
               OR r.event_ts IS NULL
               OR NOT (r.payload_json ? 'region')
               OR trim(COALESCE(r.payload_json->>'region', '')) = ''
               OR NOT (r.payload_json ? 'severity')
               OR trim(COALESCE(r.payload_json->>'severity', '')) = ''
               OR (
                   r.payload_json ? 'severity'
                   AND trim(COALESCE(r.payload_json->>'severity', '')) <> ''
                   AND lower(trim(r.payload_json->>'severity')) NOT IN (
                       'mild', 'moderate', 'severe', 'critical'
                   )
                   AND NOT (r.payload_json->>'severity' ~ '^[0-9]+(\.[0-9]+)?$')
               )
        )::bigint AS quality_issue_count
    FROM raw_typed_events r
    GROUP BY 1
) q;

COMMENT ON VIEW vw_data_quality_hourly IS
    'Per-hour completeness and severity-parse rates on landed typed events (raw_typed_events).';
