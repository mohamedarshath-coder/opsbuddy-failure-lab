# Databricks notebook source
# MAGIC %md
# MAGIC # Order Line Item Enrichment
# MAGIC Joins order headers with their line items and product catalog details to
# MAGIC produce a fully denormalized order-items export for the finance team.

# COMMAND ----------

order_headers = spark.range(0, 20000).withColumnRenamed("id", "order_id")
order_items = spark.range(0, 20000).withColumnRenamed("id", "line_item_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join headers with their line items
# MAGIC Every line item belongs to exactly one order.

# COMMAND ----------

# BUG: a genuinely common real mistake -- the join condition was meant to be
# order_headers.order_id == order_items.order_id, but order_items in this synthetic
# feed doesn't carry an order_id column (a schema this notebook's author assumed
# rather than checked), so .join() with no `on=`/condition silently falls back to a
# full cartesian product instead of raising immediately -- 400 million rows from
# two 20k-row tables.
enriched = order_headers.join(order_items)

count = enriched.count()
print(f"Enriched {count} order line items")
