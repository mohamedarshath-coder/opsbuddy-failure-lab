# Databricks notebook source
# MAGIC %md
# MAGIC # Accounts medallion · gold (3 of 3)
# MAGIC `accounts_silver` → `account_summary`: per industry and country, the accounts,
# MAGIC total and average revenue, employees and first/last sign-up, Q4 2026 renewals (count and revenue) and the number of
# MAGIC distinct regional owner teams. Runs after silver (72).

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

from notebooks.common.account_summary import build_account_summary

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = spark.table(f"{target_schema}.accounts_silver")
summary = build_account_summary(silver)
summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.account_summary"
)
report = spark.table(f"{target_schema}.account_summary")
assert report.agg(F.sum("accounts_count")).collect()[0][0] == silver.count(), "accounts lost"
print(f"account_summary: {report.count()} rows")
