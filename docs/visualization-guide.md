# Visualization Guide (Streamlit Mockup)

This guide runs the Phase 3 dashboard mockup locally while querying in-cluster TimescaleDB through a local port-forward.

## 1) Prerequisites

- `oc` CLI logged in to your OpenShift cluster
- Python 3.10+ on your laptop
- TimescaleDB service available as `timescaledb-service`

## 2) Start local connection to TimescaleDB

In terminal A:

```bash
oc port-forward svc/timescaledb-service 5432:5432
```

Keep this process running while the dashboard is open.

## 3) Configure local environment

From repo root:

```bash
cp .env.example .env
```

Then edit `.env` with the correct DB credentials for your cluster.

## 4) Install dashboard dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
```

## 5) Run the dashboard

```bash
streamlit run app/dashboard.py
```

Expected behavior:

- App shows "Connected to TimescaleDB" if port-forward and credentials are correct.
- App displays multi-page navigation:
  - Overview
  - Regional Trends
  - Hospital & Clinic Operations
  - Clinical Signals

## 6) Visualizations included

The mockup queries Layer B analytics views and renders:

1. Event volume by type over time (`analytic_event_volume_by_type`)
2. Regional severity and total case events (`analytic_regional_disease_severity_trend`)
3. Hospital capacity pressure (`analytic_hospital_capacity_pressure`)
4. Follow-up rate by clinic (`analytic_follow_up_rate_by_clinic`)
5. Avg temperature by diagnosis (`analytic_avg_temperature_by_diagnosis`)
6. Vaccination volume by type (`analytic_vaccination_volume_by_type`, CO-2026-01)

## 7) Screenshot checklist (Phase 3 evidence)

Capture at least:

- Overview page showing KPI cards and event-volume chart
- Regional Trends page with region filter and two charts
- Hospital & Clinic Operations page with capacity pressure + follow-up trend
- Clinical Signals page with diagnosis temperature chart
- Terminal showing active `oc port-forward`

## 8) Troubleshooting

- **Connection refused / timeout**
  - Confirm `oc port-forward` is running.
  - Confirm `DB_HOST=localhost` and `DB_PORT=5432`.
- **Authentication failed**
  - Re-check `DB_USER` and `DB_PASSWORD` in `.env`.
- **No data in charts**
  - Verify Spark ETL has written recent Layer A facts and Layer B views are populated.
  - Run DB checks with `psql`:
    ```sql
    SELECT count(*) FROM analytic_event_volume_by_type;
    SELECT count(*) FROM analytic_regional_disease_severity_trend;
    ```

