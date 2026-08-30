# Databricks notebook source
# MAGIC %md
# MAGIC # Full Customer Interaction History Export
# MAGIC Builds a complete customer x touchpoint interaction matrix for the annual
# MAGIC engagement report -- every customer paired with every marketing touchpoint
# MAGIC they could plausibly have seen, for downstream attribution modeling.

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.range(0, 50000).withColumnRenamed("id", "customer_id")
touchpoints = spark.range(0, 50000).withColumnRenamed("id", "touchpoint_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the full interaction matrix
# MAGIC Every customer needs a row for every touchpoint they *could* have seen, before
# MAGIC we filter down to actual interactions in a later step.

# COMMAND ----------

# BUG: an explicit crossJoin of two 50k-row DataFrames produces 2.5 BILLION rows.
# collect() then tries to pull the entire thing into the driver's memory at once.
interaction_matrix = customers.crossJoin(touchpoints).withColumn(
    "synthetic_score", F.rand()
)

all_rows = interaction_matrix.collect()
print(f"Built interaction matrix with {len(all_rows)} rows")
