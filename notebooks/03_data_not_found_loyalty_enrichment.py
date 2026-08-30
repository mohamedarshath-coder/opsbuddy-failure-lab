# Databricks notebook source
# MAGIC %md
# MAGIC # Loyalty Program Enrichment
# MAGIC Joins core customer records with the loyalty program vendor's extended
# MAGIC attributes feed (tier history, points balance, redemption activity) to build
# MAGIC a unified customer 360 view.

# COMMAND ----------

customers = spark.createDataFrame(
    [("C001", "gold"), ("C002", "silver"), ("C003", "bronze")],
    ["customer_id", "loyalty_tier"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the vendor's extended attributes feed
# MAGIC This lands daily via an external mount managed by the loyalty vendor's ETL job.

# COMMAND ----------

# BUG: this mount path was decommissioned when the loyalty vendor migrated to a new
# delivery mechanism months ago, but this notebook was never updated to match --
# a very common real-world "the upstream source moved and nobody told us" failure.
loyalty_extended = spark.read.parquet(
    "/mnt/loyalty_vendor_feed/extended_attributes/daily/"
)

enriched = customers.join(loyalty_extended, on="customer_id", how="left")
display(enriched)
