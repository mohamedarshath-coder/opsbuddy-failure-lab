# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier spend demo · silver purchase orders (2 of 3)
# MAGIC `sp_purchase_orders_raw` + `sp_suppliers` → `sp_purchase_orders`: one row per PO,
# MAGIC typed, with its supplier's name and country and the amount in USD
# MAGIC (notebooks/common/supplier_spend.py). Feeds the spend by country report (83).

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

from notebooks.common.supplier_spend import PO_COLUMNS, purchase_orders

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table("dev.opsbuddy_test.sp_purchase_orders_raw")
suppliers = spark.table(f"{target_schema}.sp_suppliers")
purchase_orders(raw, suppliers).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.sp_purchase_orders")
written = spark.table(f"{target_schema}.sp_purchase_orders")
assert written.columns == PO_COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == raw.select(F.trim("po_id")).distinct().count(), "a PO was lost or repeated"
assert written.filter(F.col("supplier_name").isNull()).count() == 0, "a PO has no known supplier"
assert written.filter(F.col("amount_usd").isNull()).count() == 0, "an amount could not be converted"
print(f"sp_purchase_orders: {written.count()} purchase orders")
