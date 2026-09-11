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
# MAGIC BUG (in the raw data, reflecting a real upstream inconsistency): order O-2004 has
# MAGIC discount_pct_str = "N/A" instead of a numeric string -- a legitimate "no discount applies"
# MAGIC signal from the promo system, not malformed data by upstream's own standard, but this
# MAGIC code never accounted for it.

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
# MAGIC BUG: `int(discount_pct_str)` has no handling for a non-numeric value -- crashes with a
# MAGIC real Python ValueError inside the UDF the moment it hits "N/A", not something that
# MAGIC degrades gracefully or only fails at a large scale.

# COMMAND ----------

def apply_discount(order_total, discount_pct_str):
    discount_pct = int(discount_pct_str)
    return order_total * (1 - discount_pct / 100.0)

apply_discount_udf = udf(apply_discount, DoubleType())

result = orders.withColumn(
    "final_total", apply_discount_udf(orders.order_total, orders.discount_pct_str)
)
result.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.orders_with_discount")
print(f"Wrote {result.count()} discounted orders")
