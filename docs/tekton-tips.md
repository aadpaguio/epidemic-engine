# Tekton Pipelines Reference

Quick reference for working with Tekton Pipelines in DS-551.

---

## Your Starter Resources

```bash
oc get task -n <your-namespace>
oc get pipeline -n <your-namespace>
oc get pipelinerun -n <your-namespace>
oc logs -f <taskrun-pod-name> -n <your-namespace>
```

---

## Key Concepts

- **Task:** one unit of work that runs in a container
- **Step:** one container within a Task
- **Pipeline:** a DAG of Tasks connected with `runAfter`
- **PipelineRun:** an execution instance of a Pipeline
- **TaskRun:** an execution instance of a Task
- **Params:** input values passed to Tasks and Pipelines

---

## Task Template (Python)

```yaml
apiVersion: tekton.dev/v1
kind: Task
metadata:
  name: my-task
spec:
  params:
  - name: kafka-bootstrap
    type: string
  - name: input-topic
    type: string
  steps:
  - name: process
    image: python:3.11-slim
    script: |
      #!/usr/bin/env sh
      pip install kafka-python-ng --target=/tmp/pkgs --quiet
      PYTHONPATH=/tmp/pkgs python3 - <<'PYEOF'
      import sys; sys.path.insert(0, '/tmp/pkgs')
      from kafka import KafkaConsumer, KafkaProducer
      import json
      import os
      PYEOF
```

**Important:** The `pip install` must be in the same `script:` block as your Python code. Each step starts in a fresh container.

---

## Pipeline Template

```yaml
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: phase1-pipeline
spec:
  params:
  - name: kafka-bootstrap
    type: string
  - name: input-topic
    type: string
  - name: validated-topic
    type: string
  tasks:
  - name: validate
    taskRef:
      name: validate-events
  - name: enrich
    runAfter: [validate]
    taskRef:
      name: route-and-enrich
```

---

## Debugging

```bash
oc describe pipelinerun <name> -n <namespace>
oc get pods -n <namespace>
oc logs <pod-name> -n <namespace>
```

---

## Official Documentation

- Tekton Docs: https://tekton.dev/docs/pipelines/
- Task API: https://tekton.dev/docs/pipelines/tasks/
- Pipeline API: https://tekton.dev/docs/pipelines/pipelines/

