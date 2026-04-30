# Log Analytics Dashboard for Pipeline Health

## What is currently broken or missing

The current system lacks a centralized operational view of pipeline health. When something fails, it is difficult to know whether the problem is caused by Kafka, Tekton task failures, downstream database writes, or a combination of issues. This makes debugging slow and increases the risk of outages going unnoticed until they start impacting data quality.

## What this feature does

This feature provides an operational dashboard that surfaces key health metrics for the pipeline. It monitors Kafka consumption, pipeline task success rates, downstream write performance, and system liveness. The dashboard is designed to detect early signs of failure, alert on degradation, and make the current state of the pipeline visible to engineers.

## How it connects to your existing pipeline

- What it reads from
  - Kafka consumer lag and read status for input topics such as `ds551-s26.team02.raw`.
  - Tekton pipeline/task run outcomes from the existing Tekton pipeline control plane.
  - Database write metrics and latency for the TimescaleDB or any downstream storage.
  - Logs or metrics emitted by the ingestion and validation tasks.

- What it writes to or triggers
  - It writes dashboard state to a visualization layer, such as Grafana or a simple web dashboard.
  - It triggers alerts or notifications when critical thresholds are crossed, such as task failure rates above a configured limit.

- Which existing components it touches
  - The Tekton pipeline, by reading taskrun status and metadata.
  - Kafka, by reading consumer group and topic lag metrics.
  - The database ingestion path, by monitoring write success and latency.
  - The observability layer, by representing metrics in an interface useful for operations.

## What analytics are important for this

- Task failure rate: percentage of failed Tekton tasks across the ingress pipeline.
- Task duration and trends: how long validation/routing tasks take over time.
- Kafka read health: whether Kafka consumers are still reading and how far behind they are.
- Topic lag: consumer group lag on raw and typed topics to detect stalls or slow consumption.
- DB write latency and error rate: how quickly writes complete and whether write failures are rising.
- Event throughput: messages processed per minute/second to detect drops in pipeline volume.
- Backpressure indicators: if lag is growing while consumer reads remain constant.
- System uptime and restart frequency: whether the ingestion components are restarting frequently.

## What would be needed to make this production-ready

1. Collect and centralize metrics
   - Add instrumentation for task success/failure, latency, and throughput.
   - Push metrics to a central monitoring system such as Prometheus.

2. Build a real dashboard
   - Create Grafana dashboards or an equivalent operational UI with the key charts and alerts.
   - Include drill-down links from failed task metrics to logs.

3. Add alerting and incident detection
   - Define alert rules for task failures, growing Kafka lag, and DB write degradation.
   - Integrate notifications with email, Slack, or PagerDuty.

4. Ensure observability for Tekton and Kafka
   - Enable Tekton metrics export and Kafka consumer/producer metrics collection.
   - Standardize on labels/tags so pipeline metrics are easy to filter.

5. Validate with real failure scenarios
   - Test dashboard behavior during task failures, Kafka outages, and DB latency spikes.
   - Confirm alerts are actionable and not noisy.

6. Document operational runbooks
   - Create a runbook describing what each metric means and what remediation steps to take.
   - Make the dashboard part of the team’s incident response flow.
