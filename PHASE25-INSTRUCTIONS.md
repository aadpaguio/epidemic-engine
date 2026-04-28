# DS-551 Epidemic Engine — Phase 2.5: Unlocking Phase 3

**Due:** Thursday, April 30, 2026 at 8:00 AM
**Submission:** Gradescope (one per team)

---

## The Story

You have a working analytics pipeline. Events flow in, Spark crunches them, results
land in your database. Phase 2 is done.

Now imagine you hand this system off to an on-call engineer at 2 AM. The Spark job
silently failed four hours ago. Nobody noticed. The database has stale data. Phase 3
features — alerting, ML, dashboards — are all reading from that stale data and
producing confident-looking garbage.

**Phase 2.5 is about closing that gap.** Before you build more features on top of
Phase 2, you need to ask: *what has to be true about my pipeline for Phase 3 to work
reliably?* What happens when something breaks? Can I tell? Can I recover?

In this exercise you will identify **two things your Phase 2 pipeline is missing**
that would need to exist before Phase 3 could be trusted in production — and build a
POC (Proof of Concept — a working prototype that demonstrates the core idea, not a
finished feature) for each one.

---

## What Makes a Good Phase 2.5 Feature

A Phase 2.5 feature sits *between* your batch analytics layer and your Phase 3
capabilities. It is not a Phase 3 feature itself — it is what makes Phase 3 possible.

Ask yourself:

- If this breaks, does Phase 3 silently produce wrong results?
- Is there currently no way to detect or recover from this failure?
- Would fixing this unblock or improve something specific in Phase 3?

If yes to those, it is probably a good Phase 2.5 feature.

**Examples of the right kind of thinking** (one per category — do not use these
directly; come up with your own):

- **Observability:** Right now you cannot tell if your Spark job ran successfully or
  how fresh your database results are. A health check that exposes job status and
  last-write timestamp would let Phase 3 features know whether to trust the data
  they are reading.

- **Resilience:** Your pipeline has no way to recover data if the Spark job crashes
  mid-run. A checkpoint or idempotent write strategy would let you re-run the job
  safely without double-counting or gaps.

- **Data integrity:** Nothing currently validates that the data Spark is reading from
  Kafka matches the schema your analytics expect. A schema check at job startup would
  catch upstream changes before they silently corrupt your results.

These are examples of the *type* of thinking we want. **You must come up with your own
features** based on what is actually missing in your system.

---

## What a POC Looks Like

A POC is **not** a finished, deployed feature. It is the core idea working well enough
to demonstrate that the approach is sound.

Every POC has two parts:

**1. A design doc (`design.md`)** that answers:
- What is broken or missing right now, specifically in your pipeline?
- What does this feature do, and how does it plug into your existing components?
- What would need to happen to turn this POC into something production-ready?

**2. Working code (`poc/`)** that demonstrates the core logic — runnable locally even
if not deployed to OpenShift.

A design doc with no code is not a POC. Code with no design doc is not a POC.

---

## Deliverables

Commit to your team repo under `phase25/`:

```text
phase25/
├── feature-1/
│   ├── design.md
│   └── poc/
├── feature-2/
│   ├── design.md
│   └── poc/
└── README.md
```

`README.md`: one paragraph explaining what gap each feature addresses and why you
chose it over other options.

---

## Design Doc Template

```markdown
# Feature Name

## What is currently broken or missing
Be specific. What happens today when this goes wrong?
What does your Phase 3 not have that it needs?

## What this feature does
One paragraph. What does the POC demonstrate?

## How it connects to your existing pipeline
- What it reads from
- What it writes to or triggers
- Which existing components it touches

## What would be needed to make this production-ready
3 to 5 specific gaps (e.g., "containerize the script",
"add retry on DB write failure", "wire into Tekton pipeline as a pre-check").
```

---

## Grading

See Gradescope for the full rubric. The core question for every feature:
*does this make Phase 3 more trustworthy, and did you actually build the thing?*
