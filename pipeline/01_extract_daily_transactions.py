# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Daily Transactions
# MAGIC First stage of the daily revenue reconciliation pipeline. Reads the raw feed landed by
# MAGIC the separate `daily_txn_ingestion` job and pulls out the current day's slice for
# MAGIC downstream cleaning and reconciliation -- reconciliation only ever wants "today," not
# MAGIC the ingestion job's full landed table.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

from pyspark.sql import functions as F

raw = spark.table(f"{target_schema}.daily_txn_raw")
todays_extract = raw.filter(F.col("txn_date") == "2026-08-30")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write today's extract
# MAGIC Overwrite each run -- this table always reflects the latest extraction of today's
# MAGIC slice, so re-running this task is naturally idempotent.

# COMMAND ----------

todays_extract.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_extract")
print(f"Wrote {todays_extract.count()} rows to {target_schema}.daily_txn_extract")
