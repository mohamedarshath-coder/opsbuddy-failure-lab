# Databricks notebook source
# MAGIC %md
# MAGIC # Regional Revenue Summary
# MAGIC Cleans up raw order records and produces a per-region revenue rollup for the
# MAGIC weekly regional performance dashboard.

# COMMAND ----------

orders = spark.createDataFrame(
    [
        ("O1", "NA-EAST", 120.50, "internal_region_code_v1"),
        ("O2", "NA-WEST", 89.00, "internal_region_code_v1"),
        ("O3", "EU-CENTRAL", 210.25, "internal_region_code_v1"),
    ],
    ["order_id", "region_code", "order_amount", "region_code_legacy"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize on the new region code column
# MAGIC The legacy region code column is being retired this quarter -- drop it early in
# MAGIC the pipeline so nothing downstream can accidentally depend on it.

# COMMAND ----------

orders_clean = orders.drop("region_code_legacy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the regional rollup

# COMMAND ----------

from pyspark.sql import functions as F

# BUG: this still references region_code_legacy, which was dropped two cells ago --
# a realistic case of a rollup query written against an older version of the
# pipeline that never got updated after the column cleanup.
regional_summary = orders_clean.groupBy("region_code_legacy").agg(
    F.sum("order_amount").alias("total_revenue")
)

display(regional_summary)
