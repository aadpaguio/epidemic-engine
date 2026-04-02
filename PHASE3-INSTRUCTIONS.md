# DS-551 Epidemic Engine — Phase 3 Instructions: Full Integration & Features

**Due:** April 30, 2026 at 8:00 PM (soft due, resubmissions allowed until May 4)  
**Hard Due:** May 4, 2026 at 8:00 AM for final code/docs  
**Video Due:** May 4, 2026 at 1:00 PM  
**Team Interviews:** May 6-7, 2026 during final exam window

**Total Points:** 1500

---

## Phase 3 Overview

Phase 3 is the capstone: an **end-to-end production Epidemic Engine** with advanced features of your choosing. You'll integrate Phase 1 (Tekton pipeline) and Phase 2 (batch analytics) into a complete system, add advanced features from a menu, and demonstrate operational readiness.

**Full System Data Flow:**
```
Event Generator (infra namespace)
  ↓
Per-team Kafka (raw topic)
  ↓
Tekton Pipeline (Phase 1: validate + enrich + route)
  ↓
Kafka typed topics (symptom_reports, clinic_visits, hospital_admissions)
  ↓
Spark Batch Analytics (Phase 2: compute 3+ analytics, write to DB)
  ↓
Database (PostgreSQL, MongoDB, etc.)
  ↓
Phase 3 Features (student-chosen):
  ├─ Alerting (real-time thresholds, notifications)
  ├─ ML (anomaly detection, outbreak prediction)
  ├─ Visualization (MANDATORY: Jupyter notebooks or dashboard)
  └─ Testing & Reliability (automated tests, data quality)
    ↓
Production System (pods running, data flowing 1+ hours, features operational)
```

---

## Grading Breakdown

| Category | Points | What You Do |
|----------|--------|-------------|
| **Required Baseline** | 400 | End-to-end integration + documentation + mandatory visualization |
| **Category A: Alerting** | 100–300 | Threshold-based alerts (basic 200 pts, advanced 300 pts) |
| **Category B: ML & Analytics** | 250–350 | Anomaly detection (250 pts) or outbreak prediction (350 pts) |
| **Category C: Visualization (MANDATORY)** | 150 | Jupyter notebooks or dashboard with analytics |
| **Category D: Testing & Reliability** | Up to 300 | Comprehensive test suite, data quality, monitoring |
| **Stretch Goals** | Varies | Additional features beyond the menu; negotiate with staff |
| **Total Available** | **1500+** | — |

### Points Strategy

- **400 pts baseline** — earn these by integrating Phase 1 + Phase 2 and documenting well
- **Spend 1100+ pts** — choose features strategically; you don't need all to reach 1500
- **Example combinations:**
  - Basic alerting (200) + anomaly detection (250) + visualization mandatory (150) + comprehensive testing (300) = **900 pts**
  - Advanced alerting (300) + outbreak prediction (350) + visualization mandatory (150) + testing (300) = **1100 pts**

---

## Requirement R1: System Integration Baseline (400 points)

### R1a: End-to-End Integration Evidence (100 points)

Your system must run for at least **1 continuous hour** with all components working:

1. **Event Generator producing events** — confirm raw topic has new events every few seconds
2. **Tekton pipeline running** — validates, enriches, routes to typed topics
3. **Typed output topics receiving events** — confirm message counts in symptom_reports, clinic_visits, hospital_admissions
4. **Spark analytics running** — job completes, computes analytics
5. **Database storing results** — query analytics results from database

**Deliverable:** Screenshot or log evidence showing all components working. Include timestamps proving 1+ hour of continuous flow.

### R1b: Data Flow Documentation (100 points)

Create `docs/PHASE3-DATAFLOW.md` documenting:

1. **End-to-end flow with specific component names**
   ```
   Event Generator (infra namespace)
     ↓
   Kafka topic: ds551-s26.team03.raw
     ↓
   Tekton Task: validate-events (image: image-registry.openshift-image-registry.svc:5000/team03/validate:latest)
     ↓
   Kafka topic: ds551-s26.team03.validated
     ↓
   Tekton Task: route-and-enrich (image: ...)
     ↓
   Kafka topics: (3 outputs)
     ↓
   Spark Job: CronJob (epidemic-analytics, runs every 6 hours)
     ↓
   PostgreSQL (postgres-service.team03:5432, database=analytics)
     ↓
   Phase 3 Features: Alerting Service (deployment), ML Model (batch job), Dashboard (Jupyter)
   ```

2. **Data schema at each step**
   - Raw topic: example JSON event from event generator
   - Validated topic: added fields (add_timestamp, added_by field, etc.)
   - Typed topics: structured schema with semantics
   - Database: analytics result tables and their schemas

3. **Latency and throughput**
   - Event generator rate: X events/sec
   - Tekton pipeline processing time: ~Y seconds per batch
   - Spark analytics frequency: every Z hours
   - Total end-to-end latency: raw event → database result = ~W minutes

4. **Failure modes and recovery**
   - If Kafka pod crashes: recovery time, data loss risk
   - If Spark job fails: retry behavior, backfill strategy
   - If database goes down: detection, alert, recovery

### R1c: System Architecture Diagram (100 points)

Create `docs/PHASE3-ARCHITECTURE.md` with:

1. **Kubernetes architecture diagram** showing:
   - Namespaces (infra, team03)
   - Pods (Kafka, Tekton controller, Spark driver/executors, database, alerting, ML, dashboard)
   - Services (internal DNS names)
   - PersistentVolumes (data storage)
   - Ingress/Routes (if exposing dashboard externally)

2. **Technology stack**
   - Container orchestration: OpenShift Kubernetes
   - Event streaming: Apache Kafka
   - Workflow: Tekton Pipelines
   - Batch analytics: Apache Spark
   - Database: [PostgreSQL/MongoDB/etc]
   - Phase 3 feature tech (alerting: Kafka Streams? Flink? Email service?; ML: scikit-learn? TensorFlow?; Viz: Jupyter? Streamlit? nginx?)

3. **Decisions and tradeoffs**
   - Why Spark vs. Kafka Streams for Phase 2? (batch analytics better than streaming?)
   - Why PostgreSQL vs. MongoDB? (structured relational better than flexible document?)
   - Why alerting via [X] vs. [Y]?
   - What would you change with more time?

### R1d: Mandatory Visualization (150 points)

You **must** implement at least one of:

**Option A: Jupyter Notebooks** (recommended; 150 pts)
- 2–3 notebooks analyzing Phase 2 analytics results
- Plots: time series of event counts, severity distributions, location heatmaps, anomalies
- Queries: read directly from your database, group/aggregate, visualize
- Live queries: show that they run against current database state
- Example:
  ```python
  import pandas as pd
  import matplotlib.pyplot as plt
  
  df = pd.read_sql("SELECT COUNT(*) as cnt FROM analytics_results GROUP BY hour", db_connection)
  df.plot(kind='line')
  plt.title("Events per Hour")
  plt.show()
  ```

**Option B: Web Dashboard** (if approved; 150 pts)
- Lightweight web app (Flask/FastAPI + Bootstrap, or embedded Streamlit/Grafana)
- Displays real-time or near-real-time analytics results
- At least 3 distinct visualizations
- No heavy frontend framework (React/Angular) required; keep it simple
- Example: Flask app with matplotlib/Plotly, served via k8s Ingress

**Option C: Combination** (recommended; 150 pts)
- Jupyter notebooks + REST API endpoint serving analytics (FastAPI)
- Notebooks for ad-hoc analysis; API for real-time dashboards

**Deliverables:**
- `notebooks/` directory with `.ipynb` files (Jupyter) or `app/` directory with Flask/FastAPI code
- `docs/visualization-guide.md` — how to run, what to expect
- Screenshot or video of visualization working with real data from your database

---

## Requirement R2: Feature Categories (1100+ points available)

Choose from one or more categories below. You can mix and match; you don't need to do all.

---

### Feature Category A: Real-Time Alerting (100–300 points)

Implement alerts that monitor your analytics results or event stream in real-time and notify on anomalies/thresholds.

**Basic Alerting (200 points):**
- Monitor **≥2 metrics** from your database or Kafka topics (e.g., events/hour > 100, avg_severity > 7)
- Threshold-based triggers (hardcoded or configurable)
- **≥1 notification method:**
  - Email integration (send via SMTP)
  - Slack webhook (send to team Slack channel)
  - SMS (Twilio or equivalent)
  - Syslog/log file (write to file, parsed by Kubernetes logging)
- Service deployment (Pod running continuously, checking thresholds)
- Per-team alerts (alerts specific to your data, not cross-team)

**Example Implementation:**
```python
# Python alerting service (runs in a pod)
import psycopg2, smtplib, time

conn = psycopg2.connect("dbname=analytics user=postgres host=postgres-service")
while True:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM analytics_results WHERE timestamp > now() - interval '1 hour'")
    count = cur.fetchone()[0]
    if count > 100:
        send_email("alert@bu.edu", f"High event volume: {count} in last hour")
    time.sleep(300)  # Check every 5 minutes
```

**Advanced Alerting (300 points):**
- Everything in basic, PLUS:
  - **Severity levels** (critical, warning, info) with escalation
  - **Suppression/deduplication** (don't spam same alert)
  - **≥2 aggregation windows** (check over 5-min, 1-hour, and daily windows; alert if consistent)
  - **Configurable thresholds** (read from ConfigMap, no code changes needed to update)

**Example:**
```yaml
---
# ConfigMap: alerting-config
apiVersion: v1
kind: ConfigMap
metadata:
  name: alerting-config
  namespace: team03
data:
  thresholds.json: |
    {
      "high_event_rate": {
        "window_5m": 50,
        "window_1h": 100,
        "window_1d": 2000,
        "severity": "warning",
        "cooldown_seconds": 300
      },
      "high_severity": {
        "threshold": 8,
        "severity": "critical"
      }
    }
```

---

### Feature Category B: ML & Analytics (250–350 points)

Implement machine learning or advanced analytics on your event stream.

**Anomaly Detection (250 points):**
- Detect unusual patterns in your event stream
- Algorithms: Z-score, IQR (Interquartile Range), or Isolation Forest on key metrics (event count, severity, location distribution)
- Input: Phase 2 analytics results or raw event stream
- Output: flagged records marked as anomalies, stored in database
- Example: "Event count in Boston at 2 AM is 5σ above historical mean → anomaly"
- Batch job: runs periodically, updates database with anomaly scores

**Implementation:**
```python
# Anomaly detection in Spark
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import IsolationForest

df = spark.read.jdbc(...)  # Read analytics from DB
scaler = StandardScaler(inputCol="event_count", outputCol="scaled_count")
df_scaled = scaler.fit(df).transform(df)

iso_forest = IsolationForest(contamination=0.1)
df_anomalies = iso_forest.transform(df_scaled)
df_anomalies.filter("anomaly == 1").write.jdbc(...)  # Write anomalies back to DB
```

**Outbreak Prediction (350 points):**
- Supervised ML: predict outbreak severity (binary: outbreak or not; or multi-class: severity level)
- Feature engineering: use Phase 2 analytics to create features (e.g., event rate trend, severity distribution, location clusters)
- Training data: use historical events; label synthetic "outbreak" scenarios
- Model: scikit-learn (RandomForest, LogisticRegression) or Spark ML
- Batch predictions: run job daily/weekly, score against latest analytics, store predictions
- Example: Given event metrics from past 7 days, predict "outbreak likely" with 85% confidence

**Implementation:**
```python
# Train model
from sklearn.ensemble import RandomForestClassifier
X = df[['event_count_trend', 'avg_severity', 'location_spread']]
y = df['has_outbreak']  # binary labels
model = RandomForestClassifier(n_estimators=100)
model.fit(X, y)

# Predict on latest data
latest_analytics = pd.read_sql("SELECT * FROM analytics_results ORDER BY time DESC LIMIT 1", db)
prediction = model.predict(latest_analytics[['event_count_trend', ...]])
# Store prediction in DB for Phase 3 dashboard
```

---

### Feature Category C: Visualization & Reporting (150 points — MANDATORY)

*You must implement at least one viz option; counts toward your baseline.*

See **R1d: Mandatory Visualization** above for details. Choose Jupyter notebooks, web dashboard, or combination.

Additional options:
- **Grafana dashboard** (if you deploy Grafana pod): query database, build time-series visualizations
- **Kibana/ELK** (if logs are indexed): visualize event flow, errors, latency

---

### Feature Category D: Testing & Reliability (up to 300 points)

**Comprehensive Test Suite (150–200 points):**
- Unit tests for analytics functions (test each Spark aggregation)
- Integration tests (Kafka → Spark → database happy path)
- End-to-end tests (Phase 1 full pipeline → Phase 2 job → verify database)
- Test framework: pytest (Python), Selenium (for dashboard), or Scala test frameworks
- Coverage: aim for ≥70% code coverage on critical paths
- CI/CD: GitHub Actions or OpenShift Tekton to run tests on every commit

**Example Unit Test (150 pts for solid coverage):**
```python
# test_analytics.py
import pytest
from analytics import compute_hourly_counts

def test_compute_hourly_counts():
    input_data = [...events with timestamps...]
    result = compute_hourly_counts(input_data)
    assert result['2026-03-24-10:00'].count == 50
    assert result['2026-03-24-11:00'].count == 45
```

**Data Quality & Monitoring (100–150 points):**
- Schema validation on incoming Kafka events (detect missing fields, wrong types)
- Data profiling: compute statistics on each field (nulls, distributions, outliers)
- Anomaly detection on data: flag if >20% of events are missing a field
- Dashboards showing data quality metrics (% valid, % complete, schema violations over time)
- Alerts if data quality drops below threshold

**Example Data Quality Check:**
```python
# In Spark job
def validate_schema(df):
    required_fields = ['event_id', 'event_type', 'timestamp', 'severity']
    missing_fields = set(required_fields) - set(df.columns)
    if missing_fields:
        raise ValueError(f"Missing fields: {missing_fields}")
    
    null_counts = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in required_fields])
    null_pct = null_counts.collect()[0].asDict()
    if any(pct > 0.05 for pct in null_pct.values()):  # >5% nulls
        log_alert("High null rate in required fields")
```

---

## Requirement R3: Deliverables & Submission

### Repository Structure

```
project-repo/
├── docs/
│   ├── PHASE3-DATAFLOW.md          (100 pts: data flow + schemas + latency)
│   ├── PHASE3-ARCHITECTURE.md      (100 pts: k8s arch diagram, tech stack, decisions)
│   ├── visualization-guide.md      (mandatory viz: how to run)
│   ├── alert-config.md             (if doing alerting)
│   ├── ml-model-guide.md           (if doing ML)
│   └── phase3-runbook.md           (operations procedures)
│
├── notebooks/ (or app/)
│   ├── analytics-dashboard.ipynb   (Jupyter notebooks, or Flask app for web dashboard)
│   └── README.md                   (how to run)
│
├── alert/ (if doing alerting)
│   ├── alert-service.py
│   ├── Dockerfile
│   └── alert-deployment.yaml
│
├── ml/ (if doing ML)
│   ├── model.pkl                   (trained model, or training script)
│   ├── predict.py                  (scoring job)
│   ├── Dockerfile
│   └── ml-job.yaml
│
├── tests/ (if doing testing)
│   ├── test_analytics.py
│   ├── test_integration.py
│   └── test_e2e.py
│
├── infra/
│   ├── phase1-tekton-pipeline.yaml (Phase 1 pipeline)
│   ├── phase2-spark-job.yaml       (Phase 2 Spark)
│   ├── database-deployment.yaml
│   └── [other infra files from Phases 1-2]
│
├── PHASE3-FEATURES-SCORECARD.md    (map your features to grading categories)
└── README.md                        (top-level; links to all above)
```

### PHASE3-FEATURES-SCORECARD.md

Create a scorecard mapping what you implemented to our grading categories:

```markdown
# Phase 3 Features Scorecard

## Baseline (400 pts)
- ✅ End-to-end integration (100 pts): System ran for 1+ hour with all components working
- ✅ Data flow documentation (100 pts): docs/PHASE3-DATAFLOW.md
- ✅ Architecture diagram (100 pts): docs/PHASE3-ARCHITECTURE.md
- ✅ Mandatory visualization (100 pts): Jupyter notebooks in notebooks/

**Subtotal: 400 pts**

## Features (1100+ pts available)

### Category A: Alerting
- ✅ Basic Alerting (200 pts): Monitors event volume, severity thresholds; alerts via Slack
  - Deployment: alert-deployment.yaml
  - Config: alert-service.py monitors PostgreSQL every 5 mins
  - Notifications: Slack webhook to #epidemic-alerts

### Category B: ML
- ✅ Anomaly Detection (250 pts): Z-score anomaly detection on event counts
  - Spark job: ml/predict.py
  - Batch frequency: daily
  - Output: anomaly_scores table in database

### Category C: Visualization
- ✅ Jupyter Notebooks (150 pts)
  - notebooks/analytics-dashboard.ipynb: time series, heatmaps, anomalies
  - Queries live database; reproducible

### Category D: Testing
- ✅ Comprehensive Test Suite (200 pts)
  - Unit tests: test_analytics.py (≥70% coverage)
  - Integration tests: test_integration.py (Kafka → Spark → DB)
  - CI: GitHub Actions on every commit

**Subtotal: 800 pts**

## Total Score
400 + 800 = **1200 pts**

(Max available is 1500+; your team earned 1200 for strategic feature choices)
```

---

## Requirement R4: Video Presentation (100 pts, not part of the 1500 but required for demo)

Record a **6-minute maximum video** demonstrating your system.

**Required sections:**
1. **System overview** (1–2 min) — show Kubernetes pods running, describe what each does
2. **Live data flow** (2–3 min) — show events flowing through the system
   - Event generator producing to raw topic
   - Tekton pipeline processing
   - Analytics results in database
   - At least one Phase 3 feature in action (alert firing, anomaly highlighted, ML prediction, visualization dashboard)
3. **Feature deep-dive** (1–2 min) — explain one feature well:
   - "Our anomaly detection flagged this 2-hour spike in symptom reports; here's why it's important"
   - "Our outbreak prediction model says 87% confidence of high severity this week; trained on this historical data"

**Format:**
- Screen recording (not required to show your face)
- Sound is clear
- Natural pacing (no rushing)
- Show real system output (logs, database queries, dashboards)

**Submission:**
- Upload to Gradescope: Phase 3 Video Verification assignment
- OR link to YouTube/Panopto in your GitHub README

---

## Requirement R5: Team Interview (100 pts, graded during final exam window)

**When:** May 6-7, 2026, during final exam window  
**Duration:** 15-20 minutes per team

Course staff will ask:
1. **Architecture**: "Why did you choose PostgreSQL vs. MongoDB? What are the tradeoffs?"
2. **Design decisions**: "Walk us through one design choice and the alternative you rejected."
3. **Failure recovery**: "Your Spark job crashed last week. How did you detect it and recover?"
4. **Individual contributions**: (asked of each team member) "What part did you build? Explain the code."
5. **Pivot/learning**: "If you had to restart in 2 weeks, what would you do differently?"

**Rubric:**
- Understands full system architecture
- Can defend design choices
- Shows operational maturity (handled failures, monitored system)
- Individual contributes meaningfully to codebase

**Outcome:** Interview scores can adjust final project grade by ±10% (multiplier: 0.9–1.1)

---

## Success Criteria

- ✅ **Integrated system runs end-to-end** for 1+ hour with no manual intervention
- ✅ **All components are operational:**
  - Event Generator → Kafka raw → Tekton → Kafka typed → Spark → Database → Features
- ✅ **At least one Phase 3 feature is live** (alerting, ML, visualization, tests)
- ✅ **Documentation is clear** (data flow, architecture, runbook, feature guide)
- ✅ **Team can explain and defend every design choice** during interview
- ✅ **Code is reproducible** (instructors can follow your docs and recreate the system)

---

## Common Issues & Tips

**Q: My Kafka topics have no new events after 1 hour**
- Check if Event Generator pod is still running: `kubectl -n infra get pods | grep event-generator`
- Check if your Tekton pipeline is consuming and producing: `kubectl logs <tekton-pod>`
- Verify topics exist: `kafka-console-consumer.sh --bootstrap-server ... --topic ds551-s26.teamXX.raw`

**Q: Spark job runs but writes nothing to database**
- Check Spark driver logs: `kubectl logs <spark-driver>`
- Verify database service is accessible: `kubectl run -it debug --image=postgres -- psql -h postgres-service -U postgres`
- Check Spark code for exceptions; add logging before the write

**Q: Alerting not firing even when thresholds are crossed**
- Verify alert service pod is running: `kubectl get pod -l app=alerting`
- Check alert service logs: `kubectl logs -l app=alerting`
- Manually query the database to confirm alert condition is true
- Test alert manually: `python alert-service.py --test`

**Q: Our architecture is too ambitious; can we simplify?**
- Yes! Focus on doing a few things well. Basic alerting + visualization + solid testing = 700+ pts and very defensible
- Talk to course staff if you want to pivot; we'd rather see a smaller, well-executed system than half-finished features

---

## Next Steps

1. **Start with Phase 1 + 2 working** — confirm end-to-end integration this week
2. **Pick your features** — choose one or two features that align with your team's strength
3. **Build incrementally** — get visualization working first (easiest), then alerting or ML
4. **Document as you go** — write runbooks and architecture docs during development, not at the end
5. **Test continuously** — don't wait until deadline; test with real data frequently
6. **Prepare for interview** — practice explaining your system to a friend; time it to ensure clarity

---

## Questions?

Ask on Piazza (#phase3 tag) or attend lab sessions:
- **Lab Tue, May 1** — Phase 3 final prep and Q&A
- **Lab Thu, May 3** — Last-minute troubleshooting

Good luck! This is a substantial project; celebrate your accomplishments. 🎉
