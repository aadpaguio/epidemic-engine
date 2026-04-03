---
name: Phase 1 Reliability Additions
overview: Lightweight SPOF and health-signal additions that fit into the existing two-task pipeline without adding new Tasks or complex wiring.
todos:
  - id: dlq-fallback
    content: Wrap DLQ produces in try/except with DLQ_UNAVAILABLE log-and-discard fallback in both task scripts
    status: already exists, implementation slightly different in each task
  - id: health-log-line
    content: Add a final structured [PIPELINE] health summary log line at the end of each task (status, counters, timing)
    status: done
  - id: readme-health-and-spof
    content: Add health interpretation and SPOF mitigation sections to tekton/README.md
    status: pending
isProject: false
---

# Phase 1 Reliability Additions: SPOF + Health Signals

## Guiding Principle

Only add items that are straightforward to implement inside the existing `validate-events` and `route-and-enrich` Tasks (no extra Tekton Tasks, no pipeline structure changes, no admin-level cluster checks).

---

## 1) SPOF Mitigation (code-level, in `tekton/tasks.yaml`)

Items that already help but should be called out explicitly:

- **Independent consumer groups per task.** Each task uses its own Kafka consumer group, so one task's failure or restart does not block or corrupt the other's offsets. (Already in place.)
- **PipelineTask retries.** Retries on both pipeline tasks mean a single pod scheduling failure or transient Kafka disconnect does not fail the whole run. (Already in `pipeline.yaml`.)
- **DLQ fallback to log-and-discard.** In both task scripts, wrap every DLQ produce call in a `try/except`. If the DLQ topic is unreachable or the param is empty, log the record with reason code `DLQ_UNAVAILABLE` and continue processing. One broken sink should not stop the pipeline.

```python
# Example pattern (inside the message loop):
try:
    producer.send(dlq_topic, key=key, value=envelope)
except Exception as dlq_err:
    log_pipeline(task="validate", event_id=eid,
                 outcome="error", reason_code="DLQ_UNAVAILABLE",
                 detail=str(dlq_err))
```

---

## 2) Health Signal (logging only, in `tekton/tasks.yaml`)

At the end of each task, after flushing the producer, emit one final structured log line summarizing the run health. This reuses the existing `[PIPELINE]` log format and adds no new dependencies.

**Fields:**

| Field | Source |
|-------|--------|
| `status` | `healthy` (no errors), `degraded` (DLQ fallbacks triggered), or `failed` (infra error like no Kafka connection) |
| `total_consumed` | Counter already tracked |
| `valid_count` | Counter already tracked |
| `dlq_count` | Counter already tracked |
| `error_count` | Counter already tracked |
| `started_at` | Capture `datetime.utcnow()` at task start |
| `completed_at` | Capture `datetime.utcnow()` before final log |
| `duration_ms` | Difference of the above |

**Status logic:**

- `healthy` — `error_count == 0` and no `DLQ_UNAVAILABLE` events.
- `degraded` — processing completed but one or more `DLQ_UNAVAILABLE` fallbacks occurred.
- `failed` — task could not connect to Kafka or encountered an unrecoverable error.

This is just one extra `print()` call per task, using data already available.

---

## 3) README Documentation (in `tekton/README.md`)

Add two short sections:

- **Health interpretation** — explain the three statuses (`healthy` / `degraded` / `failed`), where to find the summary line in `oc logs`, and what operator action each status implies.
- **SPOF notes** — document that independent consumer groups, retries, and DLQ fallback together prevent a single component failure from halting the pipeline end-to-end.

---

## What Was Removed (and Why)

These items from the original plan are deferred because they add complexity or risk rubric friction:

| Removed Item | Reason |
|-------------|--------|
| Preflight health-check Task | Adds a third Task; course deliverables specify two Tasks |
| Pipeline health-summary/aggregator Task | Adds a fourth Task; requires Tekton `results` wiring |
| Tekton Task `results` fields | Requires YAML changes in tasks + pipeline; benefit is marginal when structured logs already capture the same data |
| Kafka min-ISR / replication checks | Likely no RBAC to query cluster metadata from student pods |
| Consumer lag snapshot (`end_offsets`) | Nice-to-have but adds API calls that can fail; offset logging already covers this |
