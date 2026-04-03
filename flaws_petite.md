# Flaws i found
Q2 Flaw 1
H retry-missing


Q3 Flaw 2
D dead-letter-missing

Q4 Flaw 3
B dead-letter-missing

Q5 Flaw 4
D idempotency-violation



# Petit Agilé Bistro — Order Processing Pipeline

```mermaid
flowchart TD
    POS["POS Terminal"]
    POS -->|"produces JSON order events"| A

    A["[A] raw-orders topic · 1 partition · no message key"]
    A -->|"consumer group: bistro-consumer"| B

    B["[B] validate-order Task · checks: item_id · table_number · timestamp · invalid → logged + discarded · valid → published to valid-orders"]
    B --> C

    C["[C] valid-orders topic"]
    C -->|"consumer group: bistro-consumer"| D

    D["[D] route-and-enrich Task · adds: processed_at · restaurant_id · routes order_type: food → kitchen, drink → bar, other → nothing · write failure → caught + skipped"]
    D --> E
    D --> F
    D --> G

    E["[E] kitchen topic"]
    F["[F] bar topic"]
    G["[G] manager-alerts topic"]

    H["[H] PipelineRun · no retries: field on any Task"]
    H -. orchestrates .-> B
    H -. orchestrates .-> D
```

---

## Issue Types

| Type | Meaning |
|---|---|
| `dead-letter-missing` | Events that fail or can't be routed have nowhere to go — lost permanently |
| `idempotency-violation` | Reprocessing (restart, replay) causes duplicates downstream |
| `spof` | Single point of failure with no redundancy or isolation |
| `schema-evolution-risk` | Hardcoded assumptions break silently when a new event type or field arrives |
| `back-pressure-unchecked` | No mechanism to detect or limit lag when consumption falls behind production |
| `health-signal-absent` | No observable metric, log count, or heartbeat at this point in the pipeline |
| `retry-missing` | Transient failures cause permanent message loss — no retry policy |

# Flaws i found
Q2 Flaw 1
H retry-missing


Q3 Flaw 2
D dead-letter-missing

Q4 Flaw 3
B dead-letter-missing

Q5 Flaw 4
D idempotency-violation