import os
from dataclasses import dataclass

import psycopg2
import streamlit as st
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


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
    if err:
        st.caption(f"Connection error: {err}")
    return False
