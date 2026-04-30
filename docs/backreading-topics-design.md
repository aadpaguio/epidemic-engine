# Backreading Kafka Topics for Offline Recovery

## What is currently broken or missing

Today the pipeline assumes it is continuously online and processing incoming Kafka events in real time. If the system is down for an hour, the consumer group may lag behind, but there is no explicit logic to decide whether to backfill that gap or to continue from the latest offsets. That means the pipeline either:

- silently resumes from the last committed offset and risks missing a larger window of data if the application crashed before committing offsets, or
- requires manual intervention to rewind Kafka offsets after downtime.

This becomes especially important when downtime extends beyond a few minutes or when we want the system to be trustworthy after outages.

## What this feature does

This feature adds a controlled backreading mode for topic consumers so that when the pipeline recovers, it can replay events from the period it was offline instead of only reading new messages. It uses a time-based threshold to decide whether to perform a backfill: small gaps are ignored as acceptable data loss, while longer outages trigger replay from the offline start time up to the current stream.

## How it connects to your existing pipeline

- What it reads from
  - The raw Kafka input topic(s), such as `ds551-s26.team02.raw`.
  - Optionally the typed output topics if a downstream component needs to replay later stage processing.

- What it writes to or triggers
  - It writes recovered events through the same validation/enrichment pipeline as live events.
  - It may update consumer offset metadata in a durable store when backfill completes.

- Which existing components it touches
  - Kafka consumer group logic in the Tekton ingestion / consumer stage.
  - Any replay or backfill wrapper around the current pipeline entrypoint.
  - Monitoring/alerting for consumer lag and topic retention.

## Key design decisions

- Threshold for trigger
  - If the consumer detects a downtime gap of a few seconds or a single missed event, it does not trigger backreading. That is considered acceptable for the system.
  - If the gap exceeds a configurable threshold, such as 15–60 minutes, the system enters backfill mode.
  - The threshold should be configurable because tolerance may vary by deployment and use case.

- How the system knows it was offline
  - Track the last successfully processed event timestamp or the last committed Kafka offset timestamp.
  - Compare against the current latest available timestamp in the Kafka topic.
  - If the lag window exceeds the threshold, start replay from the offline start time rather than consuming only new records.

- Recoverability for extended downtime
  - Kafka can recover from downtime if topic retention still contains the missing data.
  - If downtime extends beyond Kafka retention, recovery is impossible for that portion of the stream.
  - If the backlog is large, the system must decide whether to catch up as fast as possible or process a bounded window and then switch to live consumption.

- Avoiding endless backlog loops
  - Use a bounded catch-up policy: read backlog until within a small lag tolerance, then resume live processing.
  - If the backlog cannot be drained before current time because event production outpaces recovery, the system should optionally skip to a recent timestamp and mark the gap as unrecoverable.
  - This prevents the consumer from spending forever on stale data.

## What would be needed to make this production-ready

1. Durable offset and timestamp tracking
   - Store last processed offsets and offline recovery metadata in a persistent store outside the consumer process.
   - Avoid relying only on in-memory state or local checkpoint files.

2. Configurable catch-up threshold and policy
   - Parameterize the backfill trigger window (e.g. 15 min, 30 min, 60 min).
   - Add a policy for how to behave when backlog is too large to drain.

3. Idempotent downstream processing
   - Ensure replayed events do not create duplicate records or side effects in downstream storage.
   - Add deduplication keys or idempotent write semantics if the same event may be reprocessed.

4. Topic retention and lag monitoring
   - Monitor Kafka topic retention and consumer lag to detect whether missed data is still available.
   - Alert if downtime exceeds retention or if the backlog is growing faster than consumption.

5. Integration into the pipeline orchestration
   - Wire the backreading feature into Tekton/OpenShift startup and failure recovery flow.
   - Add a recovery mode for pipeline tasks or consumers that runs automatically after outages.

6. Testing and validation
   - Add end-to-end tests for short outages, hour-long outages, and outages longer than retention.
   - Validate that the system behaves correctly under both replay and live catch-up.
