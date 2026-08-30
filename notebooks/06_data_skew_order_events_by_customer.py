# Databricks notebook source
# MAGIC %md
# MAGIC # Order Event Sequences by Customer
# MAGIC Builds a per-customer ordered list of every order event, for a customer journey
# MAGIC visualization tool used by the support team.

# COMMAND ----------

from pyspark.sql import functions as F
import random

random.seed(42)

# BUG (realistic skew): 95% of these synthetic order events are attributed to a
# single "default" customer_id (a genuine, common real-world cause: a broken
# upstream join that falls back to a placeholder ID whenever the real customer
# can't be matched, silently piling millions of events onto one key).
rows = []
for i in range(2_000_000):
    if random.random() < 0.95:
        customer_id = "UNKNOWN_CUSTOMER"
    else:
        customer_id = f"C{random.randint(1, 500):04d}"
    rows.append((customer_id, i, f"event payload {i}" * 20))

events = spark.createDataFrame(rows, ["customer_id", "event_seq", "payload"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect each customer's full event list
# MAGIC `collect_list` per customer -- fine for normal customers, but the
# MAGIC `UNKNOWN_CUSTOMER` key alone now holds ~1.9M large payload strings, and Spark
# MAGIC must materialize that entire list on a single executor task for that one key.

# COMMAND ----------

per_customer_journeys = events.groupBy("customer_id").agg(
    F.collect_list("payload").alias("event_payloads")
)

result = per_customer_journeys.count()
print(f"Built journeys for {result} customers")
