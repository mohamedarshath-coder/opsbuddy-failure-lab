# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders demo · bronze (1 of 5)
# MAGIC Lands the two web shop objects (`rt_customers_raw`, `rt_orders_raw`, every field
# MAGIC text) as they are, plus `source_object`. Feeds silver (82).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

customers_raw = spark.table("dev.opsbuddy_test.rt_customers_raw")
customers_raw.withColumn("source_object", F.lit("webshop.Customer")).write.mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(f"{target_schema}.rt_customers_bronze")
customers = spark.table(f"{target_schema}.rt_customers_bronze")
assert customers.count() == customers_raw.count(), "rt_customers_bronze lost or gained rows"
print(f"rt_customers_bronze: {customers.count()} rows")

# COMMAND ----------

orders_raw = spark.table("dev.opsbuddy_test.rt_orders_raw")
orders_raw.withColumn("source_object", F.lit("webshop.Order")).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.rt_orders_bronze")
orders = spark.table(f"{target_schema}.rt_orders_bronze")
assert orders.count() == orders_raw.count(), "rt_orders_bronze lost or gained rows"
print(f"rt_orders_bronze: {orders.count()} rows")
