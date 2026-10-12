# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders demo · silver (2 of 5)
# MAGIC `rt_orders_bronze` + `rt_customers_bronze` → `rt_orders_silver`: one row per order,
# MAGIC typed, with the customer's country and segment (notebooks/common/retail_orders.py).
# MAGIC Feeds the two gold jobs (83, 84).

# COMMAND ----------

import os
import sys

# The repo root on sys.path, so `notebooks.common` imports work both as a bundle job
# (path contains "/files/") and from a Databricks Git folder (repo/notebooks/<this>).
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_idx = _notebook_path.find("/files/")
_repo_root = "/Workspace" + (
    _notebook_path[: _idx + len("/files")]
    if _idx != -1
    else os.path.dirname(os.path.dirname(_notebook_path))
)
if _repo_root not in sys.path:
    sys.path.append(_repo_root)

from pyspark.sql import functions as F

from notebooks.common.retail_orders import SILVER_COLUMNS, orders_silver

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = orders_silver(
    spark.table(f"{target_schema}.rt_orders_bronze"),
    spark.table(f"{target_schema}.rt_customers_bronze"),
)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.rt_orders_silver"
)
written = spark.table(f"{target_schema}.rt_orders_silver")
assert written.columns == SILVER_COLUMNS, f"unexpected columns {written.columns}"
assert written.select("order_id").distinct().count() == written.count(), "duplicate order_id"
assert written.filter(F.col("amount").isNull()).count() == 0, "an amount did not parse"
print(f"rt_orders_silver: {written.count()} orders")
