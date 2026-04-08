# Phase 2: batch PySpark ETL (local Spark in one process). See spark/README.md for build/push.
FROM python:3.11-slim-bookworm

ARG POSTGRES_JDBC_VERSION=42.7.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/jars
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /app/jars/postgresql.jar

ENV SPARK_JDBC_JAR=/app/jars/postgresql.jar

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/etl.py /app/etl.py
WORKDIR /app

CMD ["python", "etl.py"]
