# Databricks notebook source
# MAGIC %md
# MAGIC # 03_global_reporting — downstream task (this is where the FAILURE surfaces)
# MAGIC
# MAGIC This task's own code has no bug. It does exactly what it should: join orders against
# MAGIC customers and assert the result isn't empty. It fails only because `silver_customers`
# MAGIC (written by `01_silver_customers`, a completely different task that ran earlier and
# MAGIC reported success) is empty due to a filtering bug that lives entirely in that other file.
# MAGIC
# MAGIC **This is intentional and is the whole point of this test**: correctly fixing this
# MAGIC incident requires tracing the failure back through `get_table_lineage`'s
# MAGIC `upstream_producers` to find `01_silver_customers` as the actual root cause — not
# MAGIC patching around the symptom here in this file.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md ## Bronze — synthetic order data referencing the same customers

# COMMAND ----------

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_total", DoubleType(), False),
])

bronze_orders = [
    (1001, 101, 250.00),
    (1002, 102, 89.99),
    (1003, 103, 412.50),
    (1004, 104, 75.00),
    (1005, 105, 310.25),
    (1006, 106, 199.99),
    (1007, 107, 145.00),
    (1008, 108, 620.00),
]

orders_df = spark.createDataFrame(bronze_orders, schema=order_schema)
orders_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_orders_global")
print(f"Bronze global orders written: {orders_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Gold — global revenue reporting
# MAGIC
# MAGIC This INNER JOIN and assertion are both completely correct as written. The bug is not
# MAGIC here — it's upstream. **Do not "fix" this by loosening the join or removing the
# MAGIC assertion** — that would hide the real problem instead of fixing it, exactly the kind of
# MAGIC error-suppression anti-pattern the Mode A review checklist is designed to catch. The
# MAGIC correct fix is entirely in `01_silver_customers.py`.

# COMMAND ----------

silver_customers = spark.table("dev.opsbuddy_test.silver_customers")
orders = spark.table("dev.opsbuddy_test.bronze_orders_global")

gold_df = orders.join(silver_customers, on="customer_id", how="inner")

row_count = gold_df.count()
assert row_count > 0, "Gold row count is zero"  # <-- fires here, but the fix is NOT here

gold_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_global_reporting")
print(f"Gold global reporting written: {row_count} rows")
display(gold_df)
