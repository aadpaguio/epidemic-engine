# DS-551 Epidemic Engine — Phase 2 Instructions: Analytics & Storage

**Due:** April 16, 2026 at 8:00 AM  
**Late Due:** April 18, 2026 at 8:00 AM (10% penalty)

**Total Points:** 400

---

## Phase 2 Overview

Phase 2 builds on your Phase 1 pipeline by adding a **batch analytics layer**. Your Spark job will consume the typed Kafka topics from Phase 1 (symptom_reports, clinic_visits, hospital_admissions), compute meaningful business insights, and persist results to a database for operational use.

**Data Flow:**
```
Phase 1 Output Topics (Kafka)
├─ ds551-s26.teamXX.symptom_reports
├─ ds551-s26.teamXX.clinic_visits
└─ ds551-s26.teamXX.hospital_admissions
    ↓
Spark Batch Job (runs on schedule via CronJob or Tekton)
├─ Read all three typed topics
├─ Compute 3+ distinct analytics
├─ Aggregate and transform for decision-making
└─ Write results to database
    ↓
Database (your choice: PostgreSQL, MongoDB, TimescaleDB, InfluxDB, etc.)
├─ Analytics results tables/collections
├─ Aggregations queryable for Phase 3 features
└─ Ready for dashboards, alerts, ML models
```

---

## Infrastructure Provided

- **Kafka topics from Phase 1:** Your code reads these (if Phase 1 is working correctly)
- **OpenShift cluster:** Deploy your database and Spark job here
- **Docker registry:** Internal to OpenShift; use for pushing Spark container image
- **Team credentials:** Same namespace as Phase 1; credentials from onboarding doc

---

## Grading Breakdown

| Component | Points | Details |
|-----------|--------|---------|
| **Database Deployment** | 50–75 | Base 50 pts; up to +25 bonus for InfluxDB or other advanced DB choices |
| **Spark Analytics Job** | 100 | Consumes typed Kafka topics; 3+ distinct analytics; produces valid output |
| **Containerization** | 50 | Dockerfile; image builds and pushes to registry |
| **Kubernetes Deployment** | 50 | Job/CronJob manifest; runs without errors; writes to database |
| **Documentation** | 150 | README, stress-test report, runbook, schema docs |
| **Total** | **400–425** | Base 400 pts; up to 25 bonus points available |

---

## R1: Database Deployment (50 points + bonus)

### Requirement

Deploy one database system to OpenShift in your team namespace. It must:
- Run successfully as a Pod or StatefulSet
- Be accessible from within the cluster (internal hostname)
- Support the schema you'll use for analytics results
- Persist data (local storage is OK for this course; no external cloud databases required)

### Database Options & Bonus Points

Choose **one** of:

| Database | Base Points | Bonus |
|----------|-------------|-------|
| **PostgreSQL** | 50 | — |
| **MongoDB** | 50 | +10 if you add secondary indexes and explain query performance |
| **TimescaleDB** | 50 | +20 (designed for time-series, more sophisticated) |
| **InfluxDB** | 50 | +25 (time-series TSDB, retention policies, downsampling) |
| **Redis** | 30 | +20 if you add persistence config and benchmark memory usage |

**Example:**
- Basic PostgreSQL: 50 pts
- MongoDB with indexes + performance analysis: 50 + 10 = **60 pts**
- TimescaleDB with hypertables + continuous aggregates: 50 + 20 = **70 pts**
- InfluxDB with retention policies + downsampling: 50 + 25 = **75 pts**

### Deployment

Use a Kubernetes manifest (YAML file submitted to your repo):
- StatefulSet for data durability
- PersistentVolumeClaim for storage
- Service with internal DNS for Spark job connectivity
- Example bootstrap SQL scripts (if relational) to set up schema

Example service connectivity from Spark:
```
# For PostgreSQL
jdbc:postgresql://postgres-service.team03.svc.cluster.local:5432/analytics

# For MongoDB
mongodb://mongo-service.team03.svc.cluster.local:27017/analytics

# For InfluxDB
http://influxdb-service.team03.svc.cluster.local:8086
```

### Deliverables

- `infra/database-deployment.yaml` — complete manifest (StatefulSet, PVC, Service, secrets if needed)
- `infra/database-init.sql` (or equivalent bootstrap script) — initial schema setup
- Verification that the pod is running: `kubectl get statefulsets` output in your README

---

## R2: Spark Analytics Job (100 points)

### Requirement

Implement a Spark batch job that:
1. Connects to Kafka and reads the three typed topics from Phase 1
2. Computes **3 or more distinct analytics**
3. Writes results to your database

### Reading from Kafka in Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("epidemic-analytics").getOrCreate()

# Read from Kafka topics
symptom_reports = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-team03:9092") \
    .option("subscribe", "ds551-s26.team03.symptom_reports") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON, compute analytics, write to database
```

### 3+ Required Analytics

Choose analytics that are **distinct** (not just renaming the same aggregation). Examples:

**✅ Good Examples:**
- **Trend analysis:** Event counts per hour; plot over time
- **Aggregations:** Average severity by location; percentiles of response time
- **Joins:** Correlate clinic visits with hospital admissions in same region/time window
- **Windowing:** 6-hour moving average of event rate; detect spikes
- **Anomaly detection:** Z-score or IQR on event volume; flag unusual hours

**❌ Bad Examples:**
- Three separate count-by-location queries (too similar)
- Just renaming the same aggregation three times
- Not actually computing anything distinct

### Examples with Real Data

```python
# Analytics 1: Daily event counts by event type
daily_counts = df.groupBy(
    date_trunc("day", col("timestamp")).alias("date"),
    col("event_type")
).count()

# Analytics 2: Average severity by region
avg_severity = df.groupBy("region").agg(avg("severity").alias("avg_severity"))

# Analytics 3: Join clinic visits with hospital admissions in same region/window
clinic_admissions = clinic_visits.join(
    hospital_admissions,
    on=["region", window("timestamp", "1 hour")],
    how="inner"
)
```

### Writing to Database

```python
# Write to PostgreSQL
results.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/analytics") \
    .option("dbtable", "analytics_results") \
    .option("user", "postgres") \
    .option("password", "password") \
    .mode("append") \
    .save()

# or write to MongoDB
results.write \
    .format("mongo") \
    .option("spark.mongodb.output.uri", "mongodb://mongo:27017/analytics.results") \
    .mode("append") \
    .save()
```

### Deliverables

- `spark/etl.py` (or `etl.scala` if using Scala) — complete Spark job code
- Comments explaining each of the 3+ analytics
- Clear variable names; code is readable
- Job runs end-to-end with no errors

---

## R3: Containerization (50 points)

### Requirement

Package your Spark job in a container so it runs on OpenShift.

### Dockerfile

```dockerfile
FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Install PySpark and Kafka client
RUN pip install pyspark==3.4.0 kafka-python

# Copy your Spark job
COPY spark/etl.py /app/etl.py

# Set working directory
WORKDIR /app

# Run the job
CMD ["python", "etl.py"]
```

### Build & Push

```bash
# Build
docker build -t <registry>/team03/epidemic-analytics:latest .

# Push to OpenShift internal registry
docker push <registry>/team03/epidemic-analytics:latest
```

Get the registry URL from your team credentials or ask course staff.

### Deliverables

- `Dockerfile` — located at repo root or in `spark/` subdirectory
- Image builds without errors
- Image successfully pushed to registry

---

## R4: Kubernetes Deployment (50 points)

### Requirement

Deploy your Spark job on OpenShift using **one of:**
- **Kubernetes Job** — runs once, completes
- **Kubernetes CronJob** — runs on schedule (e.g., every 6 hours)
- **Tekton Task** — runs via Tekton (Phase 1 pattern)

### Example: CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: epidemic-analytics
  namespace: team03
spec:
  schedule: "0 */6 * * *"  # Every 6 hours
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: spark-job
            image: <registry>/team03/epidemic-analytics:latest
            env:
            - name: KAFKA_BOOTSTRAP
              value: "my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092"
            - name: DATABASE_HOST
              value: "postgres-service.team03.svc.cluster.local"
            - name: DATABASE_PORT
              value: "5432"
            volumeMounts:
            - name: job-config
              mountPath: /app/config
          volumes:
          - name: job-config
            configMap:
              name: spark-config
          restartPolicy: OnFailure
          serviceAccountName: spark-sa
```

### Deliverables

- `infra/spark-job.yaml` or `infra/spark-cronjob.yaml` — complete manifest
- ServiceAccount with necessary RBAC permissions
- Job/CronJob runs successfully; verify with:
  ```bash
  kubectl get jobs
  kubectl logs -l job-name=epsilon-analytics
  ```

---

## R5: Documentation (150 points)

### Requirement

Provide documentation so an on-call engineer can understand, run, and troubleshoot your system.

### R5a: `spark/README.md` (80 points)

Document the following:

1. **Analytics descriptions** (1 sentence each)
   > Example: "Analytics 1 computes hourly event rate; used to detect traffic spikes"

2. **Data flow and Kafka connectivity**
   - Which topics are read
   - Expected schema from Phase 1
   - Example event structure

3. **Manual run command**
   ```bash
   spark-submit --master k8s://https://<master>:6443 \
     --deploy-mode cluster \
     --conf spark.kubernetes.container.image=<image> \
     spark/etl.py
   ```

4. **Monitoring and health checks**
   - How to verify the job is running
   - Expected log messages
   - Database query to verify results were written

5. **Schema documentation**
   - Database schema (tables/collections created)
   - Sample output rows
   - Data types for each analytics result

### R5b: Stress-Test Report (`docs/phase2-stress-test.md`, 40 points)

Test your system's performance limits. Report:

1. **Test setup**
   - Kafka bootstrap configuration
   - Database specs
   - Spark job resource limits (CPU, memory)

2. **Load scenarios**
   - 100 events/sec for 1 hour
   - 1000 events/sec for 10 minutes
   - Measure: latency, throughput, memory usage, database write time

3. **Results**
   - Table: events/sec vs. latency (ms)
   - Bottlenecks identified (Kafka? Database? Spark?)
   - Back-pressure handling (what happens if Kafka gets ahead of Spark?)

4. **Failure modes**
   - What happens if database goes down? (Job hangs? Crashes? Retries?)
   - What happens if Kafka topic gets too large? (Memory pressure?)
   - Recovery procedure

### R5c: Runbook (`docs/phase2-runbook.md`, 30 points)

Operational procedures for on-call engineers:

**Startup:**
- Deploy database
- Deploy Spark job
- Verify job is running
- Verify analytics results appear in database

**Shutdown:**
- Gracefully stop Spark job (allow in-flight data to complete)
- Verify no orphaned processes
- Clean up PVCs if needed

**Routine troubleshooting:**
- Job is not running (check logs, pod status)
- Database is full (clean old analytics, expand PVC)
- Kafka topics are lagging (check Spark parallelism, increase partitions)
- Database writes are slow (check indexes, query plan)

---

## Submission on Gradescope

Submit **one repository** with:

```
project-repo/
├── spark/
│   ├── etl.py (or etl.scala)
│   └── README.md
├── Dockerfile
├── infra/
│   ├── database-deployment.yaml
│   ├── database-init.sql
│   ├── spark-job.yaml (or spark-cronjob.yaml)
│   └── spark-rbac.yaml
├── docs/
│   ├── phase2-stress-test.md
│   └── phase2-runbook.md
└── README.md (top-level; links to above)
```

All files must be in your team's GitHub repo, which is the official submission source.

---

## Common Issues & Tips

**Q: My Spark job says it can't connect to Kafka**
- Verify you're using the correct bootstrap server (internal DNS, not external)
- Check that Kafka pods are running: `kubectl get pods | grep kafka`
- Verify your Spark job is in the same namespace or can see cross-namespace DNS

**Q: Writes to database are very slow**
- Add indexes on your analytics results tables
- Use batch writes, not row-by-row
- Monitor database CPU/memory; may need to scale

**Q: Job runs locally but fails on OpenShift**
- Check RBAC permissions; add ServiceAccount role bindings
- Verify image is pushed to correct registry
- Check container logs: `kubectl logs <pod>`

**Q: I don't know how to partition my analytics optimally**
- Start with 4 partitions per topic; tune based on stress test
- Use `.repartition(N)` in Spark if you need to adjust
- Monitor parallelism: Spark UI shows if tasks are under-utilized

---

## Success Criteria

- ✅ Spark job runs end-to-end without crashing
- ✅ 3+ distinct analytics compute correctly
- ✅ Results persist to database and are queryable
- ✅ Documentation is clear and complete
- ✅ Stress test identifies and addresses bottlenecks
- ✅ Job can be run multiple times; results are appended or upserted correctly

---

## Next Steps

Once Phase 2 is complete, you'll use your analytics results in Phase 3 to build advanced features (alerting, ML models, dashboards). Design your analytics outputs so Phase 3 can easily consume them.

Good luck! Ask on Piazza or in lab if you have questions.
