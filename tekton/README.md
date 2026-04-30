# Phase 1 — Tekton pipeline (team02)

This folder implements the DS-551 Phase 1 flow described in `INSTRUCTIONS.md`: validate raw Kafka events, then enrich and route them to typed topics.

## Pipeline design

1. **`validate-events`** — Reads `ds551-s26.team02.raw`. Assigns `event_id` when missing, then validates required base fields **`event_type`**, **`timestamp`**, **`region`**, and ISO-8601 timestamps. If **`schema_version`** is present it must be **`2`** (CO-2026-01); **`vaccination_record`** additionally requires `schema_version`, `record_id`, `patient_id`, `vaccine_type`, `dose_number`, and **`administered_at`**. Valid records go to `ds551-s26.team02.validated`. Invalid records are logged; when a validate DLQ topic is configured, failures are wrapped and sent there. **Typed routing happens in the next task.**

2. **`route-and-enrich`** — Runs after validate (`runAfter: [validate]`). Reads `validated`. Adds `team_id`, `processing_timestamp` (UTC), and `event_source: "ds551-event-generator"`. Routes by `event_type`:

   | `event_type`          | Topic                                      |
   | --------------------- | ------------------------------------------ |
   | `symptom_report`      | `ds551-s26.team02.symptom_reports`         |
   | `clinic_visit`        | `ds551-s26.team02.clinic_visits`           |
   | `hospital_admission`  | `ds551-s26.team02.hospital_admissions`     |
   | `vaccination_record`  | `ds551-s26.team02.vaccination_records`     |

   Other types (e.g. `general_health_report`) are **silently dropped** (no produce to typed topics; optional routing DLQ remains for parse/produce errors only).

Both tasks use **`kafka-python-ng`** in `python:3.11-slim`, consume with **`auto_offset_reset='latest'`** for new consumer groups, and stop after **`batch-size`** messages (default 100) per run. Offsets are committed manually after producer flush, and each PipelineTask has retries configured in `tekton/pipeline.yaml`.

**CO-2026-01** details (routing choice, schema rules, failure codes): [`docs/change-order-vaccination.md`](../docs/change-order-vaccination.md).

For unattended hourly operation, avoid overlapping runs with the same consumer-group IDs.

## Files

| File | Purpose |
| ---- | ------- |
| `tasks.yaml` | Tasks `validate-events` and `route-and-enrich` |
| `pipeline.yaml` | Pipeline `phase1-pipeline` chaining the two tasks |
| `pipelinerun.yaml` | Example `PipelineRun` params for team02 (`generateName: phase1-run-`) |
| `pipeline-test-commands.sh` | Optional helper: `deploy`, `run`, `status` |

## Deploy and run

Prerequisites: `oc login`, project set to your team namespace, Tekton available there. Use the **per-team** bootstrap from `INSTRUCTIONS.md`: `kafka-teamXX:9092` (this repo uses **team02**).

From the **repository root**, with `NAMESPACE` set to your OpenShift project:

```bash
export NAMESPACE=your-namespace

oc apply -f tekton/tasks.yaml -n "$NAMESPACE"
oc apply -f tekton/pipeline.yaml -n "$NAMESPACE"
```

`pipelinerun.yaml` uses **`metadata.generateName`**, so create a **new** run each time (not `apply`):

```bash
oc create -f tekton/pipelinerun.yaml -n "$NAMESPACE"
```

Equivalent helper (same repo):

```bash
export NAMESPACE=your-namespace
./tekton/pipeline-test-commands.sh deploy
./tekton/pipeline-test-commands.sh run
./tekton/pipeline-test-commands.sh status
```

Watch execution:

```bash
oc get pipelinerun -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp
oc get taskrun -n "$NAMESPACE" -l tekton.dev/pipelineRun=<pipelinerun-name>
oc logs -f <task-pod-name> -n "$NAMESPACE" --all-containers=true
```

## Verification

- **Tekton** — PipelineRun and both TaskRuns succeed; step logs show `[PIPELINE]` JSON lines and a final `SUMMARY` line with counts.
- **Kafka** — From a pod that can reach the broker (e.g. exec into the `kafka-team02` pod), consume a typed topic and confirm enrichment:

```bash
POD=$(oc get pods -l app=kafka-team02 -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
oc exec -it "$POD" -n "$NAMESPACE" -- kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ds551-s26.team02.symptom_reports \
  --from-beginning \
  --max-messages 3
```

Each message should include **`team_id`**, **`processing_timestamp`**, and **`event_source`**.

Team log snapshots for grading are under `logs/phase_1/` (`validate-logs.txt`, `route-enrich-logs.txt`).

## Challenges and solutions

1. **`event_id` missing on the wire** — `INSTRUCTIONS.md` requires `event_id`, but the class event generator often omits it. **`validate-events`** assigns `evt-<uuid>` when `event_id` is missing or blank before validation, and logs `event_id_source: synthetic` on success when applicable.

2. **`patient_id` / `region` gaps** — Some generated types omit `patient_id` (or other required base fields). Those records correctly fail validation and go to the validate DLQ when configured.

3. **Bootstrap address** — Use **`kafka-team02:9092`** from inside the team namespace, not the shared A03 broker listed only for that lab.

4. **Tekton CLI vs image paths** — Kafka CLI binary may be `kafka-console-consumer` (no `.sh`) depending on the broker image; use `oc exec` and adjust if `not found`.

5. **Merge / PR conflicts** — `tasks.yaml` was merged with reliability additions (`summary_status` on validate and route) while keeping `silent_dropped` on the route task.

For full requirements and grading rubric, see **`INSTRUCTIONS.md`** (Phase 1 section).
