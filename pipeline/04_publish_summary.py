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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Also report typical discrepancy magnitude
# MAGIC Finance also wants to see how big discrepancies tend to be on average, not just how many
# MAGIC accounts have one -- helps distinguish "lots of rounding-cent mismatches" from
# MAGIC "a handful of accounts are wildly off."

# COMMAND ----------

summary = report.agg(
    F.count("*").alias("accounts_checked"),
    F.sum(F.when(F.col("discrepancy") != 0, 1).otherwise(0)).alias("accounts_with_discrepancy"),
    F.sum("discrepancy").alias("net_discrepancy"),
    F.avg(F.abs("discrepancy")).alias("avg_abs_discrepancy"),
)

summary.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_reconciliation_summary")
display(summary)
