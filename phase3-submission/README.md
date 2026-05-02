# Phase 3 — Visualization and database evidence (cover sheet)

**Course:** DS-551 Epidemic Engine — Phase 3  
**Artifact:** Mandatory visualization implemented as a **Streamlit** application in this repository (`app/`).

## Contents of this folder

| File | Description |
|------|-------------|
| [db-query-evidence.md](db-query-evidence.md) | Primary evidence that the dashboard executes **live SQL** against the team **TimescaleDB** (Phase 2 analytic views, ML prediction tables, alert tables). Intended for grading without screenshots or video. |

## Note on deployment location

The visualization runs as a **local process** (developer laptop), not as a pod in the team namespace. The database remains **hosted in OpenShift** (TimescaleDB). Connectivity is established via **`oc port-forward`** to the cluster service and credentials supplied through environment variables. Section 1 of `db-query-evidence.md` explains why this still satisfies “queries against the current database state” as described in the Phase 3 instructions.

## Source code reference

| Path | Role |
|------|------|
| `app/db.py` | PostgreSQL connection (`psycopg2`), connectivity check |
| `app/sql_queries.py` | Parameterized SQL and `pandas.read_sql_query` |
| `app/dashboard.py` | Application entry; loads query results before rendering charts |
| `app/components.py` | Plotly visualizations from query-backed DataFrames |
