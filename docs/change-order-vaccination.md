# Change Order CO-2026-01 — Vaccination records

This documents team02 implementation of `[CHANGE-ORDER-CO-2026-01.md](../CHANGE-ORDER-CO-2026-01.md)`.

## Routing and storage decision

**Choice:** Route valid `vaccination_record` events to a **dedicated typed Kafka topic**  
`ds551-s26.team02.vaccination_records`, then **Spark batch ETL** materializes hourly facts into  
`feature_vaccination_hourly` and serving view `analytic_vaccination_volume_by_type`.

**Why:** Matches the existing Phase 1 → typed topics → Phase 2 Spark → Timescale architecture without overloading clinic/symptom streams or bypassing Kafka for raw ingest. Other teams can replay or re-consume the vaccination topic independently of other types.

## Validator (`validate-events`)

- **Base fields** (unchanged for existing types): `event_type`, `timestamp`, `region` (non-empty), timestamp must be ISO-8601.
- `**schema_version` (all event types):** If the field is **absent** or **empty**, the record is accepted (legacy payloads). If present, it must be integer `**2`**; otherwise validation fails with reason code `**BAD_SCHEMA_VERSION**` and the record goes to the **validate DLQ** when configured.
- `**vaccination_record`:** Additionally requires `schema_version == 2`, `record_id`, `patient_id`, `vaccine_type`, `dose_number` (integer-coercible), and `administered_at` (parseable ISO-8601). Failures use codes such as `MISSING_FIELD:*`, `BAD_FIELD:dose_number`, `BAD_TIMESTAMP`, or `BAD_SCHEMA_VERSION`.

## Router (`route-and-enrich`)

- `vaccination_record` → `**ds551-s26.team02.vaccination_records`** (param `vaccination-topic` on the PipelineRun).
- Unknown `event_type` values remain **silently dropped** (same as before); only parse/produce errors use the routing DLQ.

## Downstream analytic

- **Spark:** `build_feature_vaccination_hourly` aggregates counts by `bucket_start`, `region`, `vaccine_type`, and `dose_number`.
- **Timescale:** `analytic_vaccination_volume_by_type` surfaces those counts from `v_latest_feature_vaccination_hourly` (latest successful batch per bucket key).

## Operational notes

- Create the vaccination typed topic in Kafka before running Phase 1 against live traffic (same as other typed topics).
- After changing `infra/database-init.sql`, refresh the embedded ConfigMap in `[infra/database-deployment.yaml](../infra/database-deployment.yaml)` per the comment block at the top of that file.
- **Existing Timescale volumes:** init SQL runs only on first empty data volume. For an already-initialized DB, apply the new DDL manually or reset the PVC (data loss) before relying on `feature_vaccination_hourly`.