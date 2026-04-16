# Load test: raw Kafka topic (`loadgen/produce.py`)

This test drives **controlled-rate** synthetic traffic into your **raw** topic so you can observe backlog growth, Phase 1 drain time, and Spark batch behavior. It does **not** by itself measure “streaming” end-to-end latency; see [What you are measuring](#what-you-are-measuring) below.

## Prerequisites

- `oc login` and project set to your team namespace (same as Kafka / Tekton).
- Kafka bootstrap hostname that **resolves inside the cluster** (same value as `kafka-bootstrap` in `tekton/pipelinerun.yaml`).
- Raw topic name, e.g. `ds551-s26.team02.raw` (adjust for your team).

## What the script sends

`loadgen/produce.py` emits JSON objects that satisfy Phase 1 **validate-events** required fields (`event_type`, `timestamp`, `region`). By default:

- **~95%** routable events: `symptom_report`, `clinic_visit`, `hospital_admission` (with fields aligned to `INSTRUCTIONS.md`).
- **~5%** `general_health_report` (unroutable; **silent drop** in `route-and-enrich`, same as the course generator).

Override the unroutable share with `--unroutable-pct` or `LOADGEN_UNROUTABLE_PCT`.

## Run in the cluster (recommended)

From the **repository root**:

```bash
export NAMESPACE=<your-namespace>

oc create configmap loadgen-produce-script \
  --from-file=produce.py=loadgen/produce.py \
  --dry-run=client -o yaml | oc apply -n "$NAMESPACE" -f -
```

Edit `infra/loadgen-job.yaml` if your bootstrap or topic differ from the examples, or patch env after apply. Then:

```bash
oc apply -n "$NAMESPACE" -f infra/loadgen-job.yaml
oc logs -f job/loadgen-raw-kafka -n "$NAMESPACE"
```

The job prints **one JSON summary line** (prefix implied by content: `"tag":"LOADGEN"`) with:

| Field | Meaning |
| ----- | ------- |
| `target_rate` | Requested events/sec |
| `actual_rate_per_s` | `events_sent / duration_wall_s` |
| `duration_wall_s` | Wall time for the run |
| `ack_latency_ms_p50` / `p95` / `p99` | Broker ack latency from send to future completion |
| `send_errors` / `ack_errors` | Producer failures |

**Re-run:** Kubernetes Jobs are immutable; replace the job before another run:

```bash
oc delete job loadgen-raw-kafka -n "$NAMESPACE" --ignore-not-found
oc apply -n "$NAMESPACE" -f infra/loadgen-job.yaml
```

### Environment variables (Job or shell)

| Variable | Required | Description |
| -------- | -------- | ----------- |
| `KAFKA_BOOTSTRAP` | yes | Broker list, e.g. `kafka....svc.cluster.local:9092` |
| `LOADGEN_TOPIC` | no | Raw topic (default `ds551-s26.team02.raw` or `TOPIC_RAW`) |
| `LOADGEN_RATE` | no | Target events/sec (default `10`) |
| `LOADGEN_COUNT` | one of count/duration | Stop after N events |
| `LOADGEN_DURATION_SEC` | one of count/duration | Run for N seconds |
| `LOADGEN_UNROUTABLE_PCT` | no | Percent unroutable events (default `5`) |
| `LOADGEN_SEED` | no | RNG seed for reproducible mixes |

Use **either** `LOADGEN_COUNT` **or** `LOADGEN_DURATION_SEC`, not both.

## Run locally (only if Kafka is reachable)

If your laptop can reach the same bootstrap host and port (unusual for in-cluster DNS):

```bash
pip install kafka-python-ng
export KAFKA_BOOTSTRAP=<host:9092>
python loadgen/produce.py --topic ds551-s26.team02.raw --rate 10 --duration 30
```

## Suggested scenarios

Use these as a ladder; record logs and `oc adm top pod` (or OpenShift metrics) for each step.

1. **Smoke:** `--rate 10 --duration 60` — confirm summary JSON and no ack errors.
2. **Sustained moderate:** `--rate 100 --duration 600` (10 min) — watch whether `actual_rate_per_s` tracks `target_rate` and whether ack percentiles grow.
3. **Short burst:** `--rate 1000 --duration 60` — often where producer or broker limits show up (high p99, `actual_rate` below target).

After loading raw, **drain with Phase 1**: increase `batch-size` in a `PipelineRun` or run multiple `PipelineRun`s until the backlog you care about is processed; capture TaskRun duration and logs. Then run the **Spark** job and capture `[ANALYTICS]` timings and resource use.

## What you are measuring

- **Ingest path:** Can you sustain the target rate? How do Kafka **ack latencies** behave?
- **Backlog / batch pipeline:** With Tekton capped at `batch-size` per run, a fast generator creates **lag on raw**. Stress testing here answers “how long to drain N messages” and “what breaks first (timeouts, OOM, DLQ volume)?”, not “50 ms event latency.”
- **Downstream:** Typed topic size and Spark job wall time scale with how much you produced and how much Phase 1 routed.

Document those layers separately in `docs/phase2-stress-test.md` so graders can follow cause and effect.
