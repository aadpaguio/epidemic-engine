# Apache Kafka Reference

Quick reference for working with Apache Kafka in DS-551.

---

## Your Kafka Connection (Spring 2026)

**Bootstrap server (internal, use from within OpenShift):**

```text
my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092
```

**Your raw input topic:** `ds551-s26.teamXX.raw`

**Your typed output topics:**

- `ds551-s26.teamXX.symptom_reports`
- `ds551-s26.teamXX.clinic_visits`
- `ds551-s26.teamXX.environmental`

---

## Checking Topic Contents

```bash
oc run kafka-check --image=confluentinc/cp-kafka:7.4.0 -it --rm \
  --restart=Never -n <your-namespace> -- \
  kafka-console-consumer \
  --bootstrap-server my-cluster-kafka-bootstrap.ds551-2026-spring-9ab13b.svc.cluster.local:9092 \
  --topic ds551-s26.teamXX.raw \
  --max-messages 5
```

---

## Key Concepts

- **Topics:** named streams of records
- **Producers:** write data to topics
- **Consumers:** read data from topics
- **Consumer Groups:** track read position per group
- **Bootstrap Server:** entry point to the Kafka cluster

---

## Official Documentation

- Kafka Docs: https://kafka.apache.org/documentation/
- `kafka-python-ng`: https://pypi.org/project/kafka-python-ng/

