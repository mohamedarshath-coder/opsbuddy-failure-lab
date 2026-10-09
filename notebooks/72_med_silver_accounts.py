# Databricks notebook source
# MAGIC %md
# MAGIC # Accounts medallion · silver (2 of 3)
# MAGIC `accounts_bronze` → `accounts_silver`: one row per account, typed, industry
# MAGIC standardized (notebooks/common/account_cleaning.py). Runs after bronze (71),
# MAGIC feeds gold (73).

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

from notebooks.common.account_cleaning import INDUSTRIES, SILVER_COLUMNS, clean_accounts

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = clean_accounts(spark.table(f"{target_schema}.accounts_bronze"))
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.accounts_silver"
)
written = spark.table(f"{target_schema}.accounts_silver")
assert written.columns == SILVER_COLUMNS, f"unexpected columns {written.columns}"
assert written.select("account_id").distinct().count() == written.count(), "duplicate account_id"
assert written.filter(~F.col("industry").isin(INDUSTRIES)).count() == 0, "unknown industry"
print(f"accounts_silver: {written.count()} accounts")
