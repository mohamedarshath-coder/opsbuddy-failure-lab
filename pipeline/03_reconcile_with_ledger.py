# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile With Ledger
# MAGIC Third stage of the pipeline. Joins the cleaned transaction feed against the
# MAGIC internal accounting ledger to flag any per-account discrepancies for finance
# MAGIC to review.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

from pyspark.sql import functions as F

cleaned = spark.table(f"{target_schema}.daily_txn_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: raw feed vs. cleaned feed row counts
# MAGIC Finance wants confidence that cleaning isn't silently dropping legitimate transactions --
# MAGIC compare today's raw ingested row count (from the upstream `daily_txn_ingestion` job)
# MAGIC against what actually made it into the cleaned table before reconciling.

# COMMAND ----------

raw_count = spark.table(f"{target_schema}.daily_txn_raw").count()
clean_count = cleaned.count()
print(
    f"Raw: {raw_count} rows, Clean: {clean_count} rows (dropped {raw_count - clean_count})"
)

# COMMAND ----------

txn_totals = cleaned.groupBy("account_id").agg(F.sum("amount").alias("txn_total"))

# Synthetic ledger balances -- in a real pipeline this would come from the
# accounting system's own export. is_active mirrors the accounting system's own
# dormant-account flag -- every 5th account is treated as closed/dormant so the
# downstream filter below has a real column to resolve against.
ledger = spark.createDataFrame(
    [(f"ACC{i:04d}", float(i % 500), i % 5 != 0) for i in range(1, 201)],
    ["account_id", "ledger_balance", "is_active"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restrict reconciliation to active accounts only
# MAGIC Finance asked to exclude closed/dormant accounts from the daily discrepancy review --
# MAGIC no point flagging a balance mismatch on an account nobody's monitoring anymore.

# COMMAND ----------

reconciliation = (
    txn_totals.join(ledger, on="account_id", how="left")
    .withColumn("discrepancy", F.col("txn_total") - F.col("ledger_blance"))
    .filter(F.col("is_active") == True)
    # is_active is a filter-only column, not part of the published report's
    # schema -- drop it before writing so the existing daily_reconciliation_report
    # table's schema (account_id, txn_total, ledger_balance, discrepancy) is
    # preserved and the write doesn't trip a Delta schema mismatch.
    .select("account_id", "txn_total", "ledger_balance", "discrepancy")
)

reconciliation.write.mode("overwrite").saveAsTable(
    f"{target_schema}.daily_reconciliation_report"
)
print(f"Wrote {reconciliation.count()} reconciliation rows")
