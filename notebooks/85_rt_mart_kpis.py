# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders demo · country KPI mart (5 of 5)
# MAGIC `rt_daily_revenue` → `rt_country_kpis`: per country, total and average daily revenue,
# MAGIC active days and total orders.

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

from notebooks.common.retail_orders import KPI_COLUMNS, country_kpis

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

country_kpis(spark.table(f"{target_schema}.rt_daily_revenue")).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.rt_country_kpis")
written = spark.table(f"{target_schema}.rt_country_kpis")
assert written.columns == KPI_COLUMNS, f"unexpected columns {written.columns}"
assert written.select("customer_country").distinct().count() == written.count(), "duplicate country"
print(f"rt_country_kpis: {written.count()} countries")
