# Phase 1 Lab: Getting Started with the Epidemic Engine

**Lab Session:** 50 minutes total (15–20 min walkthrough + 30–35 min Q&A)

**For:** Students beginning Phase 1 of the DS-551 Epidemic Engine project

---

## Welcome to Phase 1!

This guide walks you through what Phase 1 is, how to get started, and what you need to do by April 3.

---

## Part 1: What is the Epidemic Engine Project? (2–3 minutes)

### The Big Picture

The **Epidemic Engine** is a **cloud-native data platform** that processes synthetic, funny epidemiological health events (like "patient sneezed in the Bean," "Boston tap water smells weird," etc.).

Your job over three phases is to:
1. **Phase 1 (Due Apr 3):** Ingest raw events, validate them, enrich them with metadata, and route them to topic-specific output topics using **Tekton Pipelines**
2. **Phase 2 (Due Apr 16):** Add analytics on top (compute insights using **Spark**; store in a database)
3. **Phase 3 (Due Apr 30):** Extend with advanced features (alerting, ML models, dashboards, testing)

### Why This Stack?

- **Kafka** — Event streaming platform (like a firehose for events)
- **Tekton** — Kubernetes-native pipeline orchestration (runs workflows in containers)
- **Spark** — Distributed analytics engine (for Phase 2)
- **OpenShift** — Container orchestration (your deployment platform)

### Why This Matters

You're building a **real data engineering stack** that companies use in production. By the end, you'll have:
- Event-driven architecture ✓
- Container-based workflows ✓
- Cloud-native operations ✓
- Production-grade thinking ✓

---

## Part 2: Your Infrastructure (3–4 minutes)

### What's Provided

Your course staff has already set up:

1. **OpenShift Cluster** — A Kubernetes cluster running at BU; your team gets one namespace
2. **Event Generator** (in infra namespace) — Continuously produces synthetic epidemiological events
3. **Kafka Broker** (in your namespace) — One per team; your team's data stays isolated
4. **Tekton** — Pre-installed; you define Tasks and Pipelines
5. **Team Credentials Doc** — Shared with you; contains:
   - Your namespace name (e.g., `ds551-2026-spring-team-03`)
   - Kafka bootstrap server (internal DNS)
   - Raw input topic name
   - Your role/credentials

### What You Need to Access It

```bash
# You have OpenShift CLI (oc) installed locally
# You have GitHub access
# You have your team credentials saved

# From your laptop, connect to the classroom cluster:
oc login https://api.crc.testing:6443 --token=YOUR_TOKEN
oc project ds551-2026-spring-team-03

# Verify your Kafka pod is running:
kubectl get pods | grep kafka
```

**If you don't have credentials yet:** Ask in #help on Piazza; staff will share the team credentials doc.

---

## Part 3: What Happens in Phase 1? (5–7 minutes)

### The Data Flow You're Building

```
[Event Generator in infra namespace]
         ↓ (produces events to)
[Kafka raw topic: ds551-s26.teamXX.raw]
         ↓ (your Tekton pipeline consumes)
[Tekton Task 1: validate-events]
    Checks: schema is valid, event_type is one of {symptom_report, clinic_visit, environmental_conditions}
    Discards invalid; passes valid to output topic
         ↓ (produces to)
[Kafka validated topic: ds551-s26.teamXX.validated]
         ↓ (your Tekton pipeline continues)
[Tekton Task 2: route-and-enrich]
    Enriches: adds team_id, processing_timestamp, event_source
    Routes by event_type to three typed output topics
         ↓ (produces to)
Three output topics:
    - ds551-s26.teamXX.symptom_reports
    - ds551-s26.teamXX.clinic_visits
    - ds551-s26.teamXX.environmental_conditions
```

### Your Two Deliverables

**Deliverable 1: Architecture Plan (300 points, due Apr 3)**

This is a planning doc:
- Which database will you use for Phase 2? Why?
- What three+ analytics will you compute later?
- How does your full system connect across all three phases?
- Who is responsible for what on your team?

**File:** `docs/ARCHITECTURE.md` in your repo

**Deliverable 2: Tekton Pipeline (600 points, due Apr 3)**

This is your working code:
- **Task 1 (validate-events):** Read from raw Kafka topic, check schema, write valid events to validated topic
- **Task 2 (route-and-enrich):** Read from validated topic, enrich with metadata, route by event_type to three output topics
- **Pipeline:** Orchestrate both tasks
- **PipelineRun:** Instantiate and run your pipeline
- **README:** Document how to deploy, run, verify results

**Files:** Deployed in your namespace as Kubernetes manifests

---

## Part 4: How to Get Started (Step-by-Step) (5–8 minutes)

### Step 1: Clone Your Repository

```bash
# Your team has a GitHub repo at:
# https://github.com/langd0n-classes/ds551-2026-teamXX

git clone https://github.com/langd0n-classes/ds551-2026-teamXX
cd ds551-2026-teamXX
```

Your repo has a structure (provided by course staff):

```
repo/
├── docs/
│   └── ARCHITECTURE.md          (you write this)
├── tekton/
│   ├── tasks/
│   │   ├── validate-events.yaml
│   │   └── route-and-enrich.yaml
│   ├── pipeline.yaml
│   └── pipelinerun.yaml
└── README.md                     (update this as you go)
```

### Step 2: Read Your Team Credentials

Course staff have shared a document with:
- Kafka bootstrap server (internal DNS) — use this to connect from your pipeline
- Raw input topic name
- Expected output topic names
- Your namespace name for kubectl commands

**Example:**
```
Team: 03
Namespace: ds551-2026-spring-team-03
Kafka Bootstrap: my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092
Raw Topic: ds551-s26.team03.raw
```

### Step 3: Understand Event Structure

The Event Generator produces events like this:

```json
{
  "event_type": "symptom_report",
  "patient_id": "P12345",
  "timestamp": "2026-03-24T14:30:00Z",
  "region": "Boston",
  "severity": "moderate",
  "symptoms": ["sneezing", "fatigue"]
}
```

**Key field:** `event_type` is always one of:
- `symptom_report` → goes to `ds551-s26.teamXX.symptom_reports` output topic
- `clinic_visit` → goes to `ds551-s26.teamXX.clinic_visits`
- `hospital_admission` → goes to `ds551-s26.teamXX.hospital_admissions`

The generator also produces `general_health_report`, `vaccination`, and `emergency_incident` events — these are noise events with only `event_type`, `timestamp`, and `region`. Your pipeline should silently drop them after validation.

### Step 4: Write Your Tekton Tasks

**Task 1: validate-events**

You need to create a Tekton Task that:

1. **Connects to Kafka** — use `KafkaConsumer` or equivalent to read from the input topic
2. **Defines validation rules:**
   - Required fields: `event_type`, `timestamp`, `region`
   - Valid event_types: accept all six types; the three routable ones (`symptom_report`, `clinic_visit`, `hospital_admission`) will be forwarded; the others will be dropped at routing
3. **Processes each event:**
   - If validation passes → send to the output topic
   - If validation fails → log the reason (print to stdout) and skip the event (don't send it)
4. **Produces to output topic** — use `KafkaProducer` to write valid events

**Pseudocode:**
```
for each event in input_topic:
    if event has all required fields AND event_type is valid:
        send event to output_topic
    else:
        log "Invalid: <reason>"
        skip (don't send)
```

**Implementation guidance:**
- Language: Python is recommended (Tekton runs containers; `pip install kafka-python`)
- Framework: Use `kafka-python` or equivalent Kafka client library
- Check docs: https://github.com/dpkp/kafka-python for how to consume and produce

---

**Task 2: route-and-enrich**

You need to create a Tekton Task that:

1. **Connects to Kafka** — read from the validated input topic
2. **Enriches each event** — add three new fields:
   - `team_id` — your team number (passed as a parameter)
   - `processing_timestamp` — current timestamp when this task processes the event
   - `event_source` — literal string "epidemic-engine"
3. **Routes by event_type:**
   - If `event_type == "symptom_report"` → send to `ds551-s26.{team_id}.symptom_reports` topic
   - If `event_type == "clinic_visit"` → send to `ds551-s26.{team_id}.clinic_visits` topic
   - If `event_type == "environmental_conditions"` → send to `ds551-s26.{team_id}.environmental_conditions` topic
4. **Preserves original fields** — don't lose any data from the original event; just add new fields

**Pseudocode:**
```
for each event in input_topic:
    event["team_id"] = team_id
    event["processing_timestamp"] = now()
    event["event_source"] = "epidemic-engine"
    
    output_topic = determine_output_topic(event["event_type"], team_id)
    send event to output_topic
    log "Routed {event_type} to {output_topic}"
```

**Implementation guidance:**
- Similar to Task 1: uses Python, `kafka-python`, consumer/producer
- Be careful with routing logic: use `if/elif/else` or a dict to map event types to topics
- Preserve all original event fields; you're only adding new ones

### Step 5: Create Your Pipeline

You need to create a Tekton Pipeline that:

1. **Defines parameters** passed to tasks:
   - `kafka-bootstrap` — the bootstrap server (e.g., `my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092`)
   - `team-id` — your team number (e.g., `team03`)

2. **References your two tasks** in order:
   - First task: `validate-events` (reads raw topic, outputs validated topic)
   - Second task: `route-and-enrich` (runs **after** validate-events completes; uses `runAfter`)

3. **Passes parameters to each task:**
   - Both tasks need the bootstrap server
   - Route task also needs the team-id
   - Topic names are constructed using parameters (e.g., `ds551-s26.$(params.team-id).raw`)

**Structure to follow:**
```
Pipeline
  ├─ params: kafka-bootstrap, team-id
  ├─ Task 1 (validate-events)
  │   └─ gets kafka-bootstrap, input-topic, output-topic
  └─ Task 2 (route-and-enrich)
      ├─ runAfter: Task 1
      └─ gets kafka-bootstrap, input-topic, team-id
```

**Learning resources:**
- Tekton Pipeline docs: https://tekton.dev/docs/pipelines/
- Check examples of `runAfter` to understand task ordering

### Step 6: Deploy and Run

To get your pipeline running on OpenShift:

1. **Apply your Tekton resources to the cluster:**
   - Use `kubectl apply -f` to deploy your Task YAML files
   - Use `kubectl apply -f` to deploy your Pipeline YAML
   - Verify they're registered: `kubectl get tasks`, `kubectl get pipelines`

2. **Create a PipelineRun to execute:**
   - Define a PipelineRun YAML that references your Pipeline
   - Pass values for parameters (kafka-bootstrap, team-id)
   - Apply it with `kubectl apply -f`

3. **Monitor execution:**
   - Watch the pods created by your tasks: `kubectl get pods`
   - View task logs: `kubectl logs <pod-name>` or use Tekton CLI

**Resources:**
- Tekton PipelineRun docs: https://tekton.dev/docs/pipelines/pipelineruns/
- Check how to pass parameter values in PipelineRun

### Step 7: Verify Output Topics

Once your pipeline completes:

1. **Check that output topics exist:**
   - Your three typed topics should now exist (symptom_reports, clinic_visits, environmental_conditions)
   - You can check the list of topics using Kafka CLI tools

2. **Verify events are flowing:**
   - Read a sample of messages from each output topic
   - Confirm events have the enrichment fields (team_id, processing_timestamp, event_source)
   - Trace one event from raw topic → validated → typed output to verify the full flow

3. **Troubleshooting:**
   - If output topics are empty: check task logs for errors or dropped events
   - If enrichment fields are missing: verify your route-and-enrich task logic
   - If routing is wrong: double-check your event_type → output topic mapping

---

## Part 5: Key Concepts & Common Pitfalls (3–4 minutes)

### Kafka Bootstrap Server

**Important:** When connecting from inside the cluster (from within a Tekton Task or Pod), use:
```
my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092
```

This is the **internal Kubernetes DNS name**. It only works from within the cluster.

### Event Routing Logic

Your pipeline must:
1. Read from **one input topic** (raw, then validated)
2. Write to **three output topics** (one per event_type)

This is the "typed topic" pattern: each event_type gets its own dedicated topic for cleaner downstream processing.

### Enrichment

Add these fields to every event passing through route-and-enrich:
```json
{
  "team_id": "team03",
  "processing_timestamp": "2026-03-24T14:30:15Z",
  "event_source": "epidemic-engine"
}
```

Don't lose the original fields! Python dicts in your code should preserve everything.

### Invalid Events

If an event fails validation (missing required field, unknown event_type):
- **Log it** (print to stdout; it goes to Tekton logs)
- **Drop it** (don't send to output topics)
- **Never silently ignore** — always log why it was dropped

---

## Part 6: Your Checklist by April 3 (2–3 minutes)

### Architecture Plan (docs/ARCHITECTURE.md)
- [ ] Database choice + justification
- [ ] Three+ planned analytics with business value
- [ ] Future feature direction
- [ ] System architecture diagram (all three phases)
- [ ] Team roles and responsibilities

### Tekton Pipeline Code
- [ ] validate-events task written and tested
- [ ] route-and-enrich task written and tested
- [ ] Pipeline YAML connects both tasks
- [ ] PipelineRun YAML shows how to instantiate

### README.md
- [ ] How to deploy (kubectl apply commands)
- [ ] How to run (PipelineRun creation)
- [ ] How to verify (check output topics)
- [ ] Team member names and roles
- [ ] Links to architecture doc

### Testing & Verification
- [ ] Pipeline runs end-to-end without crashing
- [ ] Events flow from raw → validated → typed topics
- [ ] Invalid events are logged (not silently dropped)
- [ ] Output events have enrichment fields
- [ ] You can trace one event from start to finish

---

## Part 7: Troubleshooting Tips (2–3 minutes)

### "My Tasks Won't Connect to Kafka"

**Problem:** Connection timeout or "host not found"

**Solution:**
- Verify bootstrap server: `my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092`
- Check Kafka pod is running: `kubectl get pods | grep kafka`
- Verify you're using the **internal DNS**, not external IP

### "Events Aren't Flowing"

**Problem:** Raw topic has events, but validated topic is empty

**Solution:**
- Check Task logs: `kubectl logs <task-pod>`
- Does your code have validation logic? (Check if events are being dropped)
- Are you actually calling `producer.send()`?
- Verify output topic names match your code

### "I'm Getting Permission Errors"

**Problem:** `kubectl: permission denied`

**Solution:**
- Verify you're logged in: `oc whoami`
- Verify correct project: `oc project`
- Ask staff to re-share your credentials token

### "My Code Works Locally But Fails in Tekton"

**Problem:** Script runs on laptop but fails in container

**Solution:**
- Kafka connection: use internal cluster DNS, not localhost
- Python imports: are you installing packages? (`pip install kafka-python`)
- File paths: absolute paths in containers, not relative paths

### "Where Do I See Logs?"

```bash
# Show all Tekton TaskRuns:
tkn taskrun list

# Show logs from a specific taskrun:
tkn taskrun logs <taskrun-name>

# Or use kubectl:
kubectl logs <pod-name>
```

---

## Summary: Your Next Steps

1. **Today (or this week):**
   - Clone repo, get credentials
   - Read INSTRUCTIONS.md fully
   - Understand the three event_types and output topics

2. **Next few days:**
   - Write Tekton tasks (start with validate-events)
   - Deploy and test locally
   - Add route-and-enrich task

3. **Two weeks out:**
   - Full pipeline running end-to-end
   - Writing architecture doc
   - Testing edge cases

4. **By April 3:**
   - Submit both deliverables (architecture + pipeline code)
   - Q&A with graders

---

## Questions?

**In this lab:** Ask now! (Next 30–35 minutes is Q&A for you)

**After lab:** 
- Post on Piazza (#phase1 tag)
- Attend next lab (Wed, Mar 31, Lab 04)
- Office hours (check calendar)

---

## Resources

- **Full Phase 1 Spec:** `INSTRUCTIONS.md` in your repo
- **Project Overview:** `PROJECT-PHASES-SUMMARY.md`
- **Tekton Docs:** https://tekton.dev/docs/
- **Kafka Python Client:** https://github.com/dpkp/kafka-python
- **Course Piazza:** #phase1, #help tags

---

Good luck! You've got this. 🚀
