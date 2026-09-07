# Databricks notebook source
# MAGIC %md
# MAGIC # 10_customer_order_enrichment
# MAGIC
# MAGIC Enriches raw orders with customer dimension attributes (tier, signup region) for the
# MAGIC downstream analytics team's customer-level order reporting.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md ## Customer dimension

# COMMAND ----------

dim_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_tier", StringType(), False),
    StructField("signup_region", StringType(), False),
])

dim_rows = [
    (401, "gold", "west"),
    (402, "silver", "east"),
    (403, "gold", "north"),
    (404, "bronze", "south"),
    # A late-arriving update for customer 402 landed as an additional row instead of an
    # in-place correction -- both the original and the updated record are now present.
    (402, "gold", "east"),
    (405, "silver", "west"),
]

dim_df = spark.createDataFrame(dim_rows, schema=dim_schema)
dim_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.dim_customer")
print(f"Customer dimension written: {dim_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Raw orders

# COMMAND ----------

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_total", DoubleType(), False),
])

order_rows = [
    (50001, 401, 120.00),
    (50002, 402, 89.50),
    (50003, 403, 340.00),
    (50004, 404, 55.25),
    (50005, 405, 210.75),
    (50006, 402, 64.00),
]

orders_df = spark.createDataFrame(order_rows, schema=order_schema)
orders_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.raw_orders")
print(f"Raw orders written: {orders_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Enrich orders with customer dimension attributes
# MAGIC
# MAGIC Each order should map to exactly one customer record — the enriched output should have
# MAGIC the same row count as the input orders. A mismatch here means something is wrong with
# MAGIC the join, not with the source data volumes.

# COMMAND ----------

orders = spark.table("dev.opsbuddy_test.raw_orders")
dim_customer = spark.table("dev.opsbuddy_test.dim_customer")

enriched_orders = orders.join(dim_customer, on="customer_id", how="inner")

order_count = orders.count()
enriched_count = enriched_orders.count()
assert enriched_count == order_count, (
    f"Row count mismatch after enrichment: expected {order_count}, got {enriched_count}"
)

enriched_orders.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_enriched_orders")
print(f"Enriched orders written: {enriched_count} rows")
display(enriched_orders)
