# Databricks notebook source
# MAGIC %md
# MAGIC # Order Discount Calculator
# MAGIC Parses a discount-percentage field from the raw orders feed (arrives as a string, e.g.
# MAGIC "15") and applies it to each order's total. A genuinely common real-world production
# MAGIC failure: the field is *usually* a clean numeric string, but one upstream system
# MAGIC occasionally sends "N/A" for orders with no applicable discount instead of "0" -- a
# MAGIC real, observed pattern (a human-maintained promo config, not automated) that this code
# MAGIC assumed away. Deterministic every run, regardless of cluster size -- not a scale-driven
# MAGIC OOM bug.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md ## Raw orders feed
# MAGIC Order O-2004 has discount_pct_str = "N/A" instead of a numeric string -- a legitimate
# MAGIC "no discount applies" signal from the promo system, not malformed data by upstream's own
# MAGIC standard. The UDF below now accounts for it.

# COMMAND ----------

orders = spark.createDataFrame([
    Row(order_id="O-2001", order_total=100.00, discount_pct_str="10"),
    Row(order_id="O-2002", order_total=250.00, discount_pct_str="5"),
    Row(order_id="O-2003", order_total=80.00, discount_pct_str="0"),
    Row(order_id="O-2004", order_total=42.50, discount_pct_str="N/A"),
    Row(order_id="O-2005", order_total=310.00, discount_pct_str="20"),
])

# COMMAND ----------

# MAGIC %md ## Apply the discount
# MAGIC FIX: `int(discount_pct_str)` previously had no handling for a non-numeric value and
# MAGIC crashed with a Python ValueError the moment it hit "N/A". Non-numeric/blank values (e.g.
# MAGIC "N/A") are now treated as "no discount applies" (0%) instead of raising, matching the
# MAGIC promo system's actual real-world signal.

# COMMAND ----------

def apply_discount(order_total, discount_pct_str):
    try:
        discount_pct = int(discount_pct_str)
    except (TypeError, ValueError):
        discount_pct = 0
    return order_total * (1 - discount_pct / 100.0)

apply_discount_udf = udf(apply_discount, DoubleType())

result = orders.withColumn(
    "final_total", apply_discount_udf(orders.order_total, orders.discount_pct_str)
)
result.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.orders_with_discount")
print(f"Wrote {result.count()} discounted orders")
