# Production Change Order — CO-2026-01

**Effective:** Immediately
**Applies to:** All teams — Phase 3 requirement
**Points:** 100 pts added to Phase 3 (grand total is now 1600 pts)

---

## What Is Happening

A new upstream event type — `vaccination_record` — is now being published into your
team's raw Kafka topic alongside the existing event types. Your pipeline does not
currently know about it.

This is not a surprise or a trick. This is how real systems work. Source contracts
evolve while pipelines stay live. The event generator has already been updated;
the new events are flowing now.

---

## The New Event

`vaccination_record` events look like this:

```json
{
  "event_type": "vaccination_record",
  "schema_version": 2,
  "timestamp": "2026-04-27T14:32:00Z",
  "record_id": "VR123456",
  "patient_id": "P54321",
  "region": "Boston",
  "vaccine_type": "influenza",
  "dose_number": 1,
  "administered_at": "2026-04-27T14:30:00Z"
}
```

**Required fields:** `event_type`, `schema_version`, `timestamp`, `record_id`,
`patient_id`, `region`, `vaccine_type`, `dose_number`, `administered_at`

Note: all events now include `schema_version: 2`. Existing event types are otherwise
unchanged.

---

## What You Need to Do

**1. Update your validator**
Decide how `vaccination_record` is handled: does it pass validation, fail, or get
routed to a new path? Document the decision. Existing event types must continue
to work without regression.

**2. Route and store vaccination records**
Add a handling path in your Tekton pipeline. Where does a `vaccination_record` go
after validation — a new typed topic, an existing one, straight to the database?
Any choice is defensible; just make it intentional and document it.

**3. Use vaccination data in at least one analytic or feature**
Integrate `vaccination_record` into at least one downstream output — a Spark
analytic, an alert condition, an ML feature, a dashboard view. It must produce a
queryable or observable output using vaccination-specific fields
(`vaccine_type`, `dose_number`).

**4. Update your docs**
Update your runbook and architecture docs to cover the new event type, how you
handle `schema_version`, and what happens if a `vaccination_record` fails
validation.

---

## What Good Looks Like

A localized change: one new routing branch, one schema validator update, one analytic
updated or added, one doc section updated. The rest of your system is untouched.

---

## Grading (Phase 3 — 100 pts)

| Component | Points |
|-----------|--------|
| Validator updated | 25 |
| Routing and storage | 25 |
| Downstream integration (uses vaccine_type or dose_number) | 35 |
| Documentation updated | 15 |

---

## Questions?

Post on Piazza with the tag `#change-order`.
