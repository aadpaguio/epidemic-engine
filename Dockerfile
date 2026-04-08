# Phase 2: batch PySpark ETL (local Spark in one process). See spark/README.md.
#
# UBI 9 avoids apt/debian (deb.debian.org) — many clusters block that egress from
# build pods while allowing Red Hat and PyPI.
FROM registry.access.redhat.com/ubi9/python-311:latest

USER root

ARG POSTGRES_JDBC_VERSION=42.7.3

RUN microdnf install -y java-17-openjdk-headless \
    && microdnf clean all \
    && rm -rf /var/cache/dnf /var/cache/yum

RUN mkdir -p /app/jars
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /app/jars/postgresql.jar

ENV SPARK_JDBC_JAR=/app/jars/postgresql.jar

COPY spark/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/etl.py /app/etl.py
WORKDIR /app

# UBI image runs well as non-root; OpenShift arbitrary UID also works with /app readability.
RUN chmod -R a+rX /app
USER 1001

CMD ["python3", "etl.py"]
