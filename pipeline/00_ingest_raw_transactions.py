# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Raw Transactions
# MAGIC Owned by the upstream payments-ingestion team, **not** the reconciliation pipeline --
# MAGIC deliberately its own separate job, on its own schedule, since other consumers besides
# MAGIC reconciliation read `daily_txn_raw` too (this is the same real production shape as
# MAGIC `daily_revenue_reconciliation_pipeline`, where a separate ingestion job lands the feed
# MAGIC before the reconciliation pipeline ever runs).
# MAGIC
# MAGIC Refunds are a normal, expected part of this feed -- represented as negative `amount`
# MAGIC values, same as any real payments ledger.

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
# MAGIC Overwrite each run -- this table always reflects the latest full landed feed, so
# MAGIC re-running this task is naturally idempotent. Downstream, the reconciliation pipeline
# MAGIC (a separate job) reads this table; it never writes to it.

# COMMAND ----------

import time
from pyspark.errors import AnalysisException

MAX_RETRIES = 3
for attempt in range(1, MAX_RETRIES + 1):
    try:
        raw.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_raw")
        print(f"Wrote {raw.count()} rows to {target_schema}.daily_txn_raw")
        break
    except Exception as e:
        if "ConcurrentAppendException" in str(type(e).__name__) or "DELTA_CONCURRENT" in str(e):
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt
                print(f"Delta concurrent conflict (attempt {attempt}/{MAX_RETRIES}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Delta concurrent conflict persisted after {MAX_RETRIES} attempts"
                ) from e
        else:
            raise
