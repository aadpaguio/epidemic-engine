# Phase 2: batch PySpark ETL (local Spark in one process). See spark/README.md.
#
# UBI 9 avoids apt/debian (deb.debian.org) — many clusters block that egress from
# build pods while allowing Red Hat and PyPI.
FROM registry.access.redhat.com/ubi9/python-311:latest

USER root

ARG POSTGRES_JDBC_VERSION=42.7.3
ARG SPARK_VERSION=3.5.4
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG JSR305_VERSION=3.0.0

# Full UBI Python image has dnf; ubi-minimal uses microdnf instead.
RUN dnf install -y java-17-openjdk-headless \
    && dnf clean all \
    && rm -rf /var/cache/dnf /var/cache/yum

RUN mkdir -p /app/jars
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /app/jars/postgresql.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /app/jars/spark-sql-kafka.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /app/jars/spark-token-provider-kafka.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /app/jars/kafka-clients.jar
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /app/jars/commons-pool2.jar
ADD https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/${JSR305_VERSION}/jsr305-${JSR305_VERSION}.jar /app/jars/jsr305.jar

ENV SPARK_JDBC_JAR=/app/jars/postgresql.jar
ENV SPARK_EXTRA_JARS=/app/jars/spark-sql-kafka.jar,/app/jars/spark-token-provider-kafka.jar,/app/jars/kafka-clients.jar,/app/jars/commons-pool2.jar,/app/jars/jsr305.jar

COPY spark/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/etl.py /app/etl.py
WORKDIR /app

# UBI image runs well as non-root; OpenShift arbitrary UID also works with /app readability.
RUN chmod -R a+rX /app
USER 1001

CMD ["python3", "etl.py"]
