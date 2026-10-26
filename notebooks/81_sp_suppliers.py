# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier spend demo · silver suppliers (1 of 3)
# MAGIC `sp_suppliers_raw` (the procurement system's supplier master, every field text) →
# MAGIC `sp_suppliers`: one row per supplier, typed, contract value in USD
# MAGIC (notebooks/common/supplier_spend.py). Country is passed through as buyers typed it.
# MAGIC Feeds purchase orders (82) and the spend by country report (83).

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

from notebooks.common.supplier_spend import SUPPLIER_COLUMNS, clean_suppliers

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table("dev.opsbuddy_test.sp_suppliers_raw")
clean_suppliers(raw).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.sp_suppliers"
)
written = spark.table(f"{target_schema}.sp_suppliers")
assert written.columns == SUPPLIER_COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == raw.select(F.trim("supplier_id")).distinct().count(), (
    "a supplier was lost or repeated"
)
assert written.filter(F.col("annual_contract_usd").isNull()).count() == 0, "a contract value"
print(f"sp_suppliers: {written.count()} suppliers")
