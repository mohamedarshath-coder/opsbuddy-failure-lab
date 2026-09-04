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

txn_totals = cleaned.groupBy("account_id").agg(
    F.sum("amount").alias("txn_total")
)

# Synthetic ledger balances -- in a real pipeline this would come from the
# accounting system's own export.
ledger = spark.createDataFrame(
    [(f"ACC{i:04d}", float(i % 500)) for i in range(1, 201)],
    ["account_id", "ledger_balance"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restrict reconciliation to active accounts only
# MAGIC Finance asked to exclude closed/dormant accounts from the daily discrepancy review --
# MAGIC no point flagging a balance mismatch on an account nobody's monitoring anymore.

# COMMAND ----------

reconciliation = (
    txn_totals.join(ledger, on="account_id", how="left")
    .withColumn("discrepancy", F.col("txn_total") - F.col("ledger_balance"))
    .filter(F.col("is_active") == True)
)

reconciliation.write.mode("overwrite").saveAsTable(
    f"{target_schema}.daily_reconciliation_report"
)
print(f"Wrote {reconciliation.count()} reconciliation rows")
