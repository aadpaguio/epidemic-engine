# Q10 — Change Order CO-2026-01 (`vaccination_record`)

---

## Validator — how `vaccination_record` is handled

**Decision:** Valid `vaccination_record` events **pass** the same validate-events stage as other types and are **published to the validated Kafka topic** (`output-topic`). They are **not** sent to a separate “pre-validation” path: routing to the vaccination-specific **typed** topic happens only in the **next** Tekton task (`route-and-enrich`).

**Failure behavior:** A `vaccination_record` that fails validation is treated like any other invalid record: it is **not** written to the validated topic; it is logged with a `reason_code`, and when `validate-dlq-topic` is configured, the failure is **wrapped and sent to the validate DLQ**.

**Pass vs. fail summary**


| Outcome                            | Meaning                                                                                                                                                                                                                                                   |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pass**                           | Base fields + vaccination-specific checks succeed → record goes to **validated** topic.                                                                                                                                                                   |
| **Fail**                           | Missing/bad fields, bad timestamps, or wrong `schema_version` → **validate DLQ** (if configured), not validated output.                                                                                                                                   |
| **Routed separately (downstream)** | After passing validation, **route-and-enrich** maps `event_type == "vaccination_record"` to the **dedicated typed topic** `vaccination_records` (see below). That is separate storage from `symptom_reports`, `clinic_visits`, and `hospital_admissions`. |


### Updated validation logic (excerpt)

Source: `tekton/tasks.yaml`, embedded Python in the `validate-events` task (`validate_obj`).

```python
        def validate_obj(obj) -> tuple[bool, str | None, str | None]:
            """Returns (ok, reason_code, event_id_str)."""
            if not isinstance(obj, dict):
                return False, "JSON_PARSE", None

            eid = obj.get("event_id")
            eid_str = str(eid) if eid is not None else None

            for field in REQUIRED_FIELDS:
                if field not in obj or obj[field] in (None, ""):
                    return False, f"MISSING_FIELD:{field}", eid_str

            ts = obj["timestamp"]
            if not check_iso8601_string(ts):
                return False, "BAD_TIMESTAMP", eid_str

            # CO-2026-01: events may include schema_version; legacy payloads omit it.
            # If present, only schema_version 2 is accepted.
            if "schema_version" in obj and obj["schema_version"] not in (None, ""):
                try:
                    sv_int = int(obj["schema_version"])
                except (TypeError, ValueError):
                    return False, "BAD_SCHEMA_VERSION", eid_str
                if sv_int != 2:
                    return False, "BAD_SCHEMA_VERSION", eid_str

            et = obj.get("event_type")
            if et == "vaccination_record":
                vac_required = (
                    "schema_version",
                    "record_id",
                    "patient_id",
                    "vaccine_type",
                    "dose_number",
                    "administered_at",
                )
                for field in vac_required:
                    if field not in obj or obj[field] in (None, ""):
                        return False, f"MISSING_FIELD:{field}", eid_str
                try:
                    if int(obj["schema_version"]) != 2:
                        return False, "BAD_SCHEMA_VERSION", eid_str
                except (TypeError, ValueError):
                    return False, "BAD_SCHEMA_VERSION", eid_str
                dn = obj["dose_number"]
                if isinstance(dn, bool):
                    return False, "BAD_FIELD:dose_number", eid_str
                try:
                    int(dn)
                except (TypeError, ValueError):
                    return False, "BAD_FIELD:dose_number", eid_str
                if not check_iso8601_string(obj["administered_at"]):
                    return False, "BAD_TIMESTAMP", eid_str

            return True, None, eid_str
```

---

## Routing and storage — where a `vaccination_record` ends up

### Kafka routing

After validation, `**route-and-enrich**` reads from the **validated** topic, enriches the payload (`team_id`, `processing_timestamp`, `event_source`), and produces to a topic chosen by `event_type`.

**Routing map** (excerpt)

Source: `tekton/tasks.yaml`, `route-and-enrich` task.

```python
        TYPE_TO_TOPIC = {
            "symptom_report": SYMPTOM_TOPIC,
            "clinic_visit": CLINIC_TOPIC,
            "hospital_admission": HOSPITAL_TOPIC,
            "vaccination_record": VACCINATION_TOPIC,
        }
```

**Topic resolution**

```python
                        et = obj.get("event_type")
                        target = TYPE_TO_TOPIC.get(et) if et is not None else None

                        # INSTRUCTIONS.md Stage 2 — unroutable types: silent drop (no topic, no DLQ).
                        if target is None:
                            silent_dropped += 1
                            continue

                        enriched = enrich(obj)
                        try:
                            producer.send(target, key=kid, value=enriched)
```

**Configured destination for team02** (`tekton/pipelinerun.yaml`):

```yaml
    - name: vaccination-topic
      value: ds551-s26.team02.vaccination_records
```

So each valid, routed `**vaccination_record**` is written to the Kafka topic `**ds551-s26.team02.vaccination_records**` (parameter `vaccination-topic`).

### Downstream storage (database)

The Spark batch ETL consumes the typed vaccination topic and writes aggregated facts to **TimescaleDB**:

- **Table:** `feature_vaccination_hourly` — hourly counts by region, `vaccine_type`, and `dose_number` (see `spark/etl.py` functions `preliminary_vaccination_records` and `build_feature_vaccination_hourly`, and DDL in `infra/database-init.sql` / `infra/database-deployment.yaml`).

**End-to-end path (concise):**  
`raw` → **validate-events** → `validated` → **route-and-enrich** → `**ds551-s26.team02.vaccination_records`** → Spark ETL → `**feature_vaccination_hourly`** (and serving views such as `v_latest_feature_vaccination_hourly` / `analytic_vaccination_volume_by_type`).

---

## Downstream integration (analytic using `vaccine_type` / `dose_number`)

**Requirement:** At least one analytic or feature must use vaccination-specific fields.

The Spark pipeline builds `**feature_vaccination_hourly`**, grouping by time bucket, region, `**vaccine_type`**, and `**dose_number**`, with record counts sourced from the typed Kafka topic. Implementation: `spark/etl.py` (`preliminary_vaccination_records`, `build_feature_vaccination_hourly`). Serving SQL includes `**analytic_vaccination_volume_by_type**` (see `infra/database-init.sql`).

---

## Documentation

Runbook / architecture coverage includes `**docs/change-order-vaccination.md**`, `**docs/phase3-dataflow.md**`, `**tekton/README.md**`, and `**spark/README.md**` (validation rules, topic names, `schema_version` behavior, and failure paths).

---

## File references


| Concern                   | Location                                                    |
| ------------------------- | ----------------------------------------------------------- |
| Validation + validate DLQ | `tekton/tasks.yaml` — task `validate-events`                |
| Routing + routing DLQ     | `tekton/tasks.yaml` — task `route-and-enrich`               |
| Topic parameter           | `tekton/pipelinerun.yaml` — `vaccination-topic`             |
| Spark read/write          | `spark/etl.py`                                              |
| Timescale table           | `infra/database-init.sql`, `infra/database-deployment.yaml` |


