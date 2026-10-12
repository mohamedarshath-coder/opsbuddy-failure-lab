# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders demo · gold customer lifetime value (4 of 5)
# MAGIC `rt_orders_silver` → `rt_customer_ltv`: per customer, delivered orders, lifetime
# MAGIC revenue and first and last order dates.

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

from notebooks.common.retail_orders import LTV_COLUMNS, customer_ltv

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

customer_ltv(spark.table(f"{target_schema}.rt_orders_silver")).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.rt_customer_ltv")
written = spark.table(f"{target_schema}.rt_customer_ltv")
assert written.columns == LTV_COLUMNS, f"unexpected columns {written.columns}"
assert written.select("customer_id").distinct().count() == written.count(), "duplicate customer"
print(f"rt_customer_ltv: {written.count()} customers")
