# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders demo · gold revenue (3 of 5)
# MAGIC `rt_orders_silver` → `rt_daily_revenue`: per day and country, delivered orders,
# MAGIC their revenue and cancelled orders. Feeds the country mart (85).

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

from notebooks.common.retail_orders import REVENUE_COLUMNS, daily_revenue

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = spark.table(f"{target_schema}.rt_orders_silver")
daily_revenue(silver).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.rt_daily_revenue"
)
written = spark.table(f"{target_schema}.rt_daily_revenue")
assert written.columns == REVENUE_COLUMNS, f"unexpected columns {written.columns}"
delivered = silver.filter(F.col("status") == "delivered").count()
assert written.agg(F.sum("delivered_orders")).first()[0] == delivered, "delivered orders lost"
print(f"rt_daily_revenue: {written.count()} rows")
