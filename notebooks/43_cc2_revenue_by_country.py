# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline v2 · 3 of 3 · Revenue by country (child of orders)
# MAGIC Builds `cc2_revenue_by_country` for the regional sales report: per `country` of
# MAGIC `cc2_orders`, the orders, distinct customers, units, revenue, average order value and
# MAGIC the first and last order date (8 columns). Runs after notebook 42.
# MAGIC `country` is an ISO alpha-3 code, so there is one row per country.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

cc2_revenue_by_country = (
    spark.table(f"{target_schema}.cc2_orders")
    .groupBy("country")
    .agg(
        F.count("*").alias("orders_count"),
        F.countDistinct("customer_id").alias("customers_count"),
        F.sum("quantity").cast("bigint").alias("total_quantity"),
        F.sum("amount").cast("decimal(14,2)").alias("revenue"),
        F.round(F.avg("amount"), 2).cast("decimal(12,2)").alias("avg_order_value"),
        F.min("ordered_on").alias("first_order_on"),
        F.max("ordered_on").alias("last_order_on"),
    )
)
cc2_revenue_by_country.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc2_revenue_by_country"
)

# COMMAND ----------

# MAGIC %md ## The report adds up to the orders: nothing lost or counted twice

# COMMAND ----------

orders = spark.table(f"{target_schema}.cc2_orders")
report = spark.table(f"{target_schema}.cc2_revenue_by_country")
orders_total = orders.agg(F.sum("amount")).collect()[0][0]
report_total = report.agg(F.sum("revenue")).collect()[0][0]
assert report_total == orders_total, f"report totals {report_total} but orders total {orders_total}"
assert report.agg(F.sum("orders_count")).collect()[0][0] == orders.count(), "order count mismatch"
assert (
    report.agg(F.sum("total_quantity")).collect()[0][0]
    == orders.agg(F.sum("quantity")).collect()[0][0]
), "quantity mismatch"
assert report.filter(~F.col("country").rlike("^[A-Z]{3}$")).count() == 0, "country format"
assert report.count() == report.select("country").distinct().count(), "country split across rows"
print(f"cc2_revenue_by_country: {report.count()} rows, revenue {report_total}")
