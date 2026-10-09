# Databricks notebook source
# MAGIC %md
# MAGIC # Accounts medallion · gold (3 of 3)
# MAGIC `accounts_silver` → `account_summary`: per industry and country, the accounts,
# MAGIC total and average revenue, employees and first/last sign-up. Runs after silver (72).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = spark.table(f"{target_schema}.accounts_silver")
summary = silver.groupBy("industry", "billing_country").agg(
    F.count("*").alias("accounts_count"),
    F.sum("annual_revenue").cast("decimal(16,2)").alias("total_revenue"),
    F.round(F.avg("annual_revenue"), 2).cast("decimal(14,2)").alias("avg_revenue"),
    F.sum("employee_count").cast("bigint").alias("total_employees"),
    F.min("created_date").alias("first_created"),
    F.max("created_date").alias("last_created"),
)
summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.account_summary"
)
report = spark.table(f"{target_schema}.account_summary")
assert report.agg(F.sum("accounts_count")).collect()[0][0] == silver.count(), "accounts lost"
print(f"account_summary: {report.count()} rows")
