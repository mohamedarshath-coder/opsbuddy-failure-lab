# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · gold AR aging (6 of 7)
# MAGIC `bl_invoices_silver` → `bl_ar_aging`: per account, the invoices not fully paid,
# MAGIC what is still open and its age buckets as of the reporting date. Feeds the
# MAGIC finance mart (97).

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

from notebooks.common.billing import AGING_COLUMNS, ar_aging

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

invoices = spark.table(f"{target_schema}.bl_invoices_silver")
ar_aging(invoices).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_ar_aging"
)
written = spark.table(f"{target_schema}.bl_ar_aging")
assert written.columns == AGING_COLUMNS, f"unexpected columns {written.columns}"
gap = (
    F.col("bucket_0_30_usd")
    + F.col("bucket_31_60_usd")
    + F.col("bucket_61_90_usd")
    + F.col("bucket_90_plus_usd")
    - F.col("open_usd")
)
assert written.filter(gap != 0).count() == 0, "age buckets do not add up to open_usd"
print(f"bl_ar_aging: {written.count()} accounts with open receivables")
