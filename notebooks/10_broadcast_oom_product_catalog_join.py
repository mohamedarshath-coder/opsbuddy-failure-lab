# Databricks notebook source
# MAGIC %md
# MAGIC # Product Catalog Enrichment (Broadcast-Optimized)
# MAGIC Joins order line items against the full product catalog. The catalog was small
# MAGIC enough to broadcast last quarter, so this join was hand-tuned with an explicit
# MAGIC broadcast hint for speed.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

order_items = spark.range(0, 5_000_000).withColumnRenamed("id", "line_item_id") \
    .withColumn("product_id", (F.col("line_item_id") % 500_000).cast("long"))

# The catalog has since grown far past "small enough to broadcast" (a new supplier
# integration added hundreds of thousands of SKUs last month), but nobody revisited
# this hand-tuned hint after that change.
product_catalog = spark.range(0, 500_000).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", F.concat(F.lit("Product-"), F.col("product_id"))) \
    .withColumn("description", F.lit("x" * 2000))  # a realistically detailed
                                                     # description field per SKU

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with the broadcast hint
# MAGIC BUG: forcing a broadcast of a now-multi-GB catalog table onto every executor
# MAGIC blows well past the cluster's broadcast memory budget -- a very real
# MAGIC "this used to be fine, then the data grew" production failure.

# COMMAND ----------

enriched = order_items.join(broadcast(product_catalog), on="product_id", how="left")

count = enriched.count()
print(f"Enriched {count} order line items with product catalog data")
