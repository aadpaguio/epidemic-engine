# Evidence: Mandatory visualization queries TimescaleDB

## Purpose

This memo documents how the Phase 3 satisfies the requirement for **live database queries** against operational analytics.

**What is being demonstrated**

1. The dashboard opens a **PostgreSQL/TimescaleDB** connection using **`psycopg2`** (not mock data or static JSON).
2. Every chart and table is fed by **`pandas.read_sql_query`** executing SQL against named relations in the **`epidemic`** database.
3. Those relations include **Layer B analytic views** (`analytic_*`) built on Phase 2 hourly facts, plus **Phase 3 tables** (`ml_outbreak_predictions`, `alert_events`) where applicable.

---

## 1. Architecture: local UI, cluster database

The Streamlit application runs **outside** the OpenShift cluster (e.g., on a developer machine). The TimescaleDB instance runs **inside** the cluster. This separation is common when shared namespaces enforce resource quotas or when visualization is developed interactively.

**Database reachability:** operators typically run in a separate terminal:

```bash
oc port-forward svc/timescaledb-service 5432:5432
```

The application is configured with standard environment variables (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) so that `DB_HOST` points at the forwarded port (e.g., `127.0.0.1`). This yields the same query semantics as a pod co-located in the cluster: SQL is executed **on the server**; only the client runs locally.

---

## 2. Evidence: explicit database connection

Connection construction is implemented in `app/db.py`. Host, port, database name, user, and password are read from the environment and passed to **`psycopg2.connect`**.

```21:40:app/db.py
def get_db_config() -> DbConfig:
    return DbConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "epidemic"),
        user=os.getenv("DB_USER", "spark_writer"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def get_connection():
    cfg = get_db_config()
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.name,
        user=cfg.user,
        password=cfg.password,
        connect_timeout=5,
    )
```

Before any analytical content loads, the UI verifies connectivity with **`SELECT 1`**. If the check fails, execution stops and the user is prompted with a sample port-forward command.

```43:64:app/db.py
def check_connection():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
        return True, ""
    except Exception as exc:
        return False, str(exc)


def render_connection_status() -> bool:
    ok, err = check_connection()
    if ok:
        st.success("Connected to TimescaleDB.")
        return True

    st.error("Could not connect to TimescaleDB.")
    st.code("oc port-forward svc/timescaledb-service 5432:5432", language="bash")
```

---

## 3. Evidence: unified query path (`read_sql_query`)

All dashboard datasets flow through a single helper that opens a connection and executes **`pandas.read_sql_query`**. There is no parallel code path that injects fabricated rows into charts.

```11:14:app/sql_queries.py
def _query_df(sql: str, params=None) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)
```

---

## 4. Catalog of relations queried

The following table maps **`app/sql_queries.py`** entry points to **database relations**. Graders can cross-check definitions in `infra/database-init.sql` (views and tables).

| Query function | Relation(s) |
|----------------|-------------|
| `get_event_volume` | `analytic_event_volume_by_type` |
| `get_regional_trends` | `analytic_regional_disease_severity_trend` |
| `get_hospital_pressure` | `analytic_hospital_capacity_pressure` |
| `get_follow_up_rates` | `analytic_follow_up_rate_by_clinic` |
| `get_diagnosis_temperature` | `analytic_avg_temperature_by_diagnosis` |
| `get_vaccination_volume` | `analytic_vaccination_volume_by_type` |
| `get_regions` | `analytic_regional_disease_severity_trend` |
| `get_latest_prediction_run`, `get_prediction_methods`, `get_prediction_rows`, `get_detector_consensus` | `ml_outbreak_predictions` |
| `get_alert_events` | `alert_events` |

The **`analytic_*`** names refer to **SQL views** (Layer B) defined in repository DDL; they read from hourly fact tables produced by Phase 2 Spark and from **`v_latest_feature_*`** “latest batch per key” helpers as documented in `infra/database-init.sql`.

---

## 5. Representative SQL: Phase 2 analytics view

The regional severity trend is a representative example: it selects from **`analytic_regional_disease_severity_trend`** with a sliding time window and optional region filter.

```33:47:app/sql_queries.py
def get_regional_trends(hours: int = 72, region: str = "ALL") -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start,
            region,
            avg_severity,
            total_case_events,
            batch_id,
            run_timestamp
        FROM analytic_regional_disease_severity_trend
        WHERE bucket_start >= now() - (%s || ' hours')::interval
          AND (%s = 'ALL' OR region = %s)
        ORDER BY bucket_start, region;
    """
    return _query_df(sql, params=(hours, region, region))
```

---

## 6. Representative SQL: Phase 3 prediction rows

Outbreak detector outputs are read from **`ml_outbreak_predictions`**, restricted to the latest **`run_id`** and a time window.

```197:225:app/sql_queries.py
def get_prediction_rows(
    hours: int = 24,
    region: str = "ALL",
    model_version: str = "ALL",
) -> pd.DataFrame:
    sql = """
        WITH latest AS (
            SELECT run_id
            FROM ml_outbreak_predictions
            ORDER BY prediction_timestamp DESC
            LIMIT 1
        )
        SELECT
            prediction_timestamp,
            bucket_start,
            region,
            model_version,
            risk_score,
            predicted_label,
            threshold,
            run_id
        FROM ml_outbreak_predictions
        WHERE run_id = (SELECT run_id FROM latest)
          AND bucket_start >= now() - (%s || ' hours')::interval
          AND (%s = 'ALL' OR region = %s)
          AND (%s = 'ALL' OR model_version = %s)
        ORDER BY bucket_start, region, model_version;
    """
    return _query_df(sql, params=(hours, region, region, model_version, model_version))
```

---

## 7. Application flow: queries precede charts

`app/dashboard.py` invokes **`render_connection_status()`** first; if connectivity fails, **`st.stop()`** prevents rendering. Immediately afterward it loads each dataset by calling the query functions listed above. Plotting code in `app/components.py` consumes only those DataFrames.

```470:487:app/dashboard.py
if not render_connection_status():
    st.stop()


event_volume = get_event_volume(hours=hours)
regional = get_regional_trends(hours=hours)
hospital_pressure = get_hospital_pressure(hours=min(hours, 48))
follow_up = get_follow_up_rates(hours=min(hours, 48))
diagnosis = get_diagnosis_temperature(hours=min(hours, 72))
vaccination = get_vaccination_volume(hours=hours)
prediction_run = _coerce_prediction_timestamp(get_latest_prediction_run())
prediction_methods = [_pretty_model_name(m) for m in get_prediction_methods()]
prediction_rows = _coerce_prediction_timestamp(
    get_prediction_rows(hours=min(hours, 72))
)
detector_consensus = _coerce_prediction_timestamp(
    get_detector_consensus(hours=min(hours, 72))
)
```

The UI caption states that analytics are backed by TimescaleDB:

```463:467:app/dashboard.py
st.title(f"Epidemic Engine: {page}")
st.caption(
    "Final operations dashboard over TimescaleDB analytics and outbreak signals. "
    "Trend visuals are ordered by event time."
)
```

---

## 8. Optional independent verification (same catalog objects)

You can confirm row visibility with **`psql`** inside the cluster without running Streamlit. Replace `<namespace>` with the team namespace:

```bash
oc exec timescaledb-0 -- psql -U spark_writer -d epidemic -c \
  "SELECT COUNT(*) FROM analytic_regional_disease_severity_trend WHERE bucket_start >= now() - interval '72 hours';"
```

Output:

count 
-------
    40
(1 row)


