# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline · 3 of 3 · Revenue by country (child of orders)
# MAGIC Builds `cc_revenue_by_country` for the regional sales report: revenue and order count
# MAGIC per `country` of `cc_orders`. Runs after notebook 32.
# MAGIC Orders carry ISO alpha-3 country codes, so there is one row per country.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

cc_revenue_by_country = (
    spark.table(f"{target_schema}.cc_orders")
    .groupBy("country")
    .agg(
        F.count("*").alias("orders_count"),
        F.sum("amount").cast("decimal(14,2)").alias("revenue"),
    )
)
cc_revenue_by_country.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc_revenue_by_country"
)

# COMMAND ----------

# MAGIC %md ## The report adds up to the orders: nothing lost or counted twice

# COMMAND ----------

orders = spark.table(f"{target_schema}.cc_orders")
report = spark.table(f"{target_schema}.cc_revenue_by_country")
orders_total = orders.agg(F.sum("amount")).collect()[0][0]
report_total = report.agg(F.sum("revenue")).collect()[0][0]
assert report_total == orders_total, f"report totals {report_total} but orders total {orders_total}"
assert report.agg(F.sum("orders_count")).collect()[0][0] == orders.count(), "order count mismatch"
assert report.count() == report.select("country").distinct().count(), "more than one row per country"
print(f"cc_revenue_by_country: {report.count()} rows, revenue {report_total}")
