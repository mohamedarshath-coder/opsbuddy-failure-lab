# Databricks notebook source
# MAGIC %md
# MAGIC # 04_inventory_alert_job
# MAGIC
# MAGIC Cross-references recent orders against the active-products dimension to flag SKUs that
# MAGIC need inventory replenishment alerts sent to the warehouse team.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

# MAGIC %md ## Bronze — recent order line items referencing product IDs

# COMMAND ----------

order_line_schema = StructType([
    StructField("order_line_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
])

bronze_order_lines = [
    (9001, 5001, 3),
    (9002, 5002, 1),
    (9003, 5003, 2),
    (9004, 5005, 5),
    (9005, 5006, 1),
    (9006, 5007, 4),
]

orders_df = spark.createDataFrame(bronze_order_lines, schema=order_line_schema)
orders_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_order_lines")
print(f"Bronze order lines written: {orders_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Inventory alert candidates
# MAGIC
# MAGIC Every ordered SKU should exist in the active-products dimension — an order can't be
# MAGIC placed against a product that isn't sellable. If this join comes back empty, something
# MAGIC upstream is broken.

# COMMAND ----------

active_products = spark.table("dev.opsbuddy_test.silver_active_products")
order_lines = spark.table("dev.opsbuddy_test.bronze_order_lines")

alert_candidates = order_lines.join(active_products, on="product_id", how="inner")

candidate_count = alert_candidates.count()
assert candidate_count > 0, "No active product matches found for inventory alert processing"

alert_candidates.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_inventory_alerts")
print(f"Inventory alert candidates written: {candidate_count} rows")
display(alert_candidates)
