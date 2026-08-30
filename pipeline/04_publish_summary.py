# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Reconciliation Summary
# MAGIC Final stage of the pipeline. Rolls the per-account reconciliation report up
# MAGIC into a single daily summary for the finance team's morning review.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

from pyspark.sql import functions as F

report = spark.table(f"{target_schema}.daily_reconciliation_report")

summary = report.agg(
    F.count("*").alias("accounts_checked"),
    F.sum(F.when(F.col("discrepancy") != 0, 1).otherwise(0)).alias("accounts_with_discrepancy"),
    F.sum("discrepancy").alias("net_discrepancy"),
)

summary.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_reconciliation_summary")
display(summary)
