# Team [XX] — Epidemic Engine

**DS-551 Data Engineering at Scale — Spring 2026**

---

## Project Overview

[Brief description of what your Epidemic Engine does]

---

## Team Members

- [Name 1] — [Role/Contributions]
- [Name 2] — [Role/Contributions]
- [Name 3] — [Role/Contributions]
- [Name 4] — [Role/Contributions]

---

## Architecture

[High-level description of your system]

See `docs/architecture.md` for detailed architecture documentation.

---

## Quick Start

Use `INSTRUCTIONS.md` as the course-staff requirements document.

### Current Active Scope

- Architecture Plan
- Phase 1 — Tekton Pipeline

Later project phases will be added to this same repository by course staff.

### Phase 1 Deployment

```bash
oc apply -f tekton/tasks.yaml -n <your-namespace>
oc apply -f tekton/pipeline.yaml -n <your-namespace>
oc apply -f tekton/pipelinerun.yaml -n <your-namespace>
oc get pipelinerun -n <your-namespace>
```

### Verification

[How to verify your current project work is functioning]

---

## Repository Structure

```text
├── README.md
├── INSTRUCTIONS.md
├── docs/
│   ├── architecture.md
│   ├── genai-usage.md
│   ├── kafka-tips.md
│   ├── spark-tips.md
│   └── tekton-tips.md
├── tekton/
│   └── [your Phase 1 files go here]
├── spark/
│   └── [used later]
└── k8s/
    └── [used later]
```

Additional folders and files will be added later as the project grows.

---

## Phase Status

### Architecture Plan ⏳
- [ ] Database choice documented
- [ ] Planned analytics described
- [ ] Team roles assigned

### Phase 1: Tekton Pipeline ⏳
- [ ] `validate-events` task implemented
- [ ] `route-and-enrich` task implemented
- [ ] Pipeline runs successfully
- [ ] Typed topics receiving events
- [ ] `tekton/README.md` complete

---

## Documentation

- **Requirements:** `INSTRUCTIONS.md`
- **Architecture:** `docs/architecture.md`
- **GenAI Usage:** `docs/genai-usage.md`
- **Kafka Tips:** `docs/kafka-tips.md`
- **Spark Tips:** `docs/spark-tips.md`
- **Tekton Tips:** `docs/tekton-tips.md`
- **Phase 1 Pipeline Notes:** create `tekton/README.md` as part of your submission
