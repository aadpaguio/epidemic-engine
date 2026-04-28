# Lec 24 — AI Architecture Comparison

**Assignment:** One submission per team. Submit by opening a PR on this repo with your completed answers.
**Due:** Monday, April 28, 2026 at 11:59 PM
**Late due:** Wednesday, April 30, 2026 at 11:59 PM (grace period)

**Tool requirement:** You must use **VS Code Copilot** (GitHub Copilot in Visual Studio Code) for this activity. Responses using a different AI tool will not receive full credit.

---

## What to do

1. Pull up your team's original architecture plan submission and summarize it in Q1 below.
2. Open VS Code Copilot and prompt it to generate a full architecture plan for your Epidemic Engine system. Give it context: your DB choice, your analytics goals, your Phase 3 features, and your team's topic namespace.
3. Compare the two plans and answer Q2–Q6 below.
4. Open a PR on this repo with your completed file. **One PR per team.**

---

## Q1 — Your Team's Architecture Plan Summary (15 pts)

Paste the key elements of your team's original architecture plan. Include at minimum:
1. Your database choice and justification
2. The analytics you planned (name + what each computes)
3. Your Phase 3 capabilities
4. Your data flow (Kafka topics → Spark → DB)

1. Database choice and justification: TimescaleDB was selected as the feature store for its hypertable support, enabling efficient time-series queries and continuous aggregates. This choice qualifies for the +20 bonus in the Phase 2 rubric by leveraging PostgreSQL extensions for scalable analytics, avoiding simple timestamp columns and supporting partitioned storage for historical data.

2. The analytics planned (name + what each computes):

- Regional disease severity trend: Computes average severity and case counts per region over time to identify worsening trends.
- Hospital capacity pressure index: Calculates a stress signal per hospital combining admission rates and available bed statistics.
- Symptom-to-admission escalation rate: Derives the ratio of admissions to symptoms per region and time bucket as an indicator of escalation.
- Event volume by type over time: Tracks throughput and activity mix per event type across time periods.
- Average patient temperature by diagnosis code: Aggregates temperature trends grouped by diagnosis code, optionally by clinic or time.
- Follow-up requirement rate by clinic: Computes the rate of follow-up requirements per clinic, retaining numerator and denominator counts.

3. Phase 3 capabilities: Phase 3 includes a dashboard for visualization, an alerting service for notifications, and an ML scoring/outbreak prediction model that consumes the database for real-time analytics.

4. Data flow (Kafka topics → Spark → DB): Event Generator → Kafka raw topic (ds551-s26.team02.raw) → Tekton validate-events → validated topic (ds551-s26.team02.validated) → Tekton route-and-enrich → typed topics (ds551-s26.team02.symptom_reports, ds551-s26.team02.clinic_visits, ds551-s26.team02.hospital_admissions) → Spark batch ETL job → TimescaleDB Layer A fact tables (canonical hourly facts) → Layer B SQL views (derived analytics) → Phase 3 consumers.

---

## Q2 — VS Code Copilot Architecture Plan Output (20 pts)

Paste the architecture plan that VS Code Copilot generated for your Epidemic Engine. Include the full output or a clearly representative excerpt. **Also paste at least one prompt you used** to generate the plan.

**Prompt(s) you used:**

```
using the given instructions and requirements, design an architecture plan
```
(INSTRUCTIONS.md, PHASE2-INSTRUCTIONS.md, PHASE3-INSTRUCTIONS.md provided as context). 
First output was not useful, had to additionally prompt for 
```
give more detail on the previous plan you gave me. What steps do I need to take to implement this?
```

**Copilot's output:**

## Plan: Implement Epidemic Engine Architecture

Build the full Epidemic Engine system as summarized, integrating Phases 1-3 with TimescaleDB, 6 distinct analytics, Phase 3 features (visualization, alerting, ML), end-to-end Kafka-to-database flow, and assigned team roles, ensuring operational readiness by May 4, 2026.

### Steps
1. Implement Phase 1 Tekton pipeline: Create tasks.yaml, pipeline.yaml, and pipelinerun.yaml for validation, enrichment, and routing to typed topics.
2. Deploy TimescaleDB: Use database-deployment.yaml and database-init.sql with hypertables for time-series storage, achieving +20 bonus.
3. Develop Spark ETL job: Write etl.py to compute 6 analytics (e.g., regional trends via `groupBy` and `avg`), containerize with Dockerfile, and deploy via spark-job.yaml.
4. Build Phase 3 features: Create Jupyter notebooks in notebooks/ for visualization, alerting service in alert/, and ML model in ml for outbreak prediction.
5. Integrate and test: Run end-to-end flow for 1+ hour, document in PHASE3-DATAFLOW.md and PHASE3-ARCHITECTURE.md, assign roles, and create runbook.
6. Finalize deliverables: Update README.md, PHASE3-FEATURES-SCORECARD.md, record video, and prepare for interview.

### Further Considerations
1. Prioritize Phase 1 completion first, as Phases 2-3 depend on typed topics; test with live Kafka events.
2. Ensure analytics are distinct and queryable (e.g., use `window` functions for time buckets); align with PHASE2-INSTRUCTIONS.md for stress testing.
3. Leverage existing plans in phase-1-plan.md and phase-2-plan.md; assign roles based on strengths (e.g., one for Tekton, one for Spark).

## Q3 — Where the AI Plan Matched Your Design (20 pts)

Identify **at least two specific areas** where the AI-generated plan aligned with or resembled your team's actual architecture. Quote or paraphrase both the AI output and your original plan. Explain *why* you think they matched — was it because the context you provided was specific enough, or because certain patterns are standard regardless of context?

**Match 1:**

Both plans specifically call out using TimescaleDB as the feature store

**Why they matched:**

While this was an intentional decision that we made, based on evaluating options provided in the 'database options' table in PHASE2-INSTRUCTIONS, I think that the AI planner has somehow just pulled this decision in as context, even though I tried not to include these files.

**Match 2:**

Both call for the same yaml files to be created for the phase 1 tekton pipeline

AI-generated has step 2 "Implement Phase 1 Tekton pipeline: Create tasks.yaml, pipeline.yaml, and pipelinerun.yaml for validation, enrichment, and routing to typed topics."

Our plan has these files listed as well, with specific requirements, see phase-1-plan.md starting from line 135.

**Why they matched:**

These file were explicitly asked for by the instructions. It makes perfect sense that they would be included in both design plans.

---

## Q4 — Where the AI Plan Diverged from Your Design (20 pts)

Identify **at least two specific areas** where the AI-generated plan differed from, missed, or got wrong relative to your team's actual architecture. Quote or paraphrase both sides. Was the AI's suggestion worse, different-but-reasonable, or actually better than what you built?

**Divergence 1:**

AI plan 

**Judgment (worse / different-but-reasonable / better) and why:**

> [your explanation]

**Divergence 2:**

> [describe the divergence; quote/paraphrase both plans]

**Judgment (worse / different-but-reasonable / better) and why:**

> [your explanation]

---

## Q5 — Likes, Dislikes, and Takeaways (15 pts)

Reflect on the experience of using VS Code Copilot to generate an architecture plan.

**What did you like** about the AI-generated plan? Was anything surprisingly useful, well-structured, or worth adopting?

> [your answer]

**What did you not like** or found lacking? What would you never trust an AI to get right without deep system context?

> [your answer]

**One concrete takeaway:** Going forward, what is one specific way you would or would not use AI assistance for architecture decisions?

> [your answer]

---

## Q6 — Comparison Presentation (10 pts)

Record a short video (3–5 minutes) or prepare a 3-minute in-class presentation walking through the comparison. Show the two plans side-by-side (or in sequence) and narrate: what matched, what differed, and your key takeaway.

Paste your video link below (Google Drive or Panopto; sharing must be set to "anyone with link can view"), or write "in-class presentation" if your team is presenting live.

**Video link or presentation note:**

> [paste link or write "in-class presentation"]
