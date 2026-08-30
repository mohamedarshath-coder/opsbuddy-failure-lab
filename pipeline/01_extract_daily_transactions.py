# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Daily Transactions
# MAGIC First stage of the daily revenue reconciliation pipeline. Pulls the day's raw
# MAGIC transaction feed (synthetic here) into a Delta table for downstream cleaning
# MAGIC and reconciliation.
# MAGIC
# MAGIC Refunds are a normal, expected part of this feed -- represented as negative
# MAGIC `amount` values, same as any real payments ledger.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

import random
from pyspark.sql import functions as F

random.seed(7)

rows = []
for i in range(1, 1001):
    account_id = f"ACC{random.randint(1, 200):04d}"
    if random.random() < 0.03:
        # a genuine, expected refund -- negative amount
        amount = -round(random.uniform(5, 250), 2)
        txn_type = "refund"
    else:
        amount = round(random.uniform(5, 500), 2)
        txn_type = "charge"
    rows.append((f"TXN{i:06d}", account_id, amount, txn_type, "2026-08-30"))

raw = spark.createDataFrame(
    rows, ["transaction_id", "account_id", "amount", "txn_type", "txn_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the raw layer
# MAGIC Overwrite each run -- this table always reflects the latest full extract, so
# MAGIC re-running this task is naturally idempotent.

# COMMAND ----------

raw.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_raw")
print(f"Wrote {raw.count()} rows to {target_schema}.daily_txn_raw")
