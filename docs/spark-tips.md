# Apache Spark Reference

This file is included for later phases of the project. **It is not required for Phase 1.**

---

## Official Documentation

- Spark Documentation: https://spark.apache.org/docs/latest/
- Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- PySpark API: https://spark.apache.org/docs/latest/api/python/
- Spark SQL Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html

---

## Key Concepts

- **DataFrames:** distributed collections of data organized into named columns
- **Structured Streaming:** stream processing using the DataFrame API
- **Batch Processing:** processing data in batches
- **Aggregations:** grouping and summarizing data
- **Windowing:** time-based aggregations on streaming data

---

## Tips

- Start with a simple batch or streaming path and make it correct before optimizing.
- Test locally where possible before deploying to OpenShift.
- Watch memory and CPU usage when moving into cluster execution.
- Verify Kafka and database connection strings carefully.

