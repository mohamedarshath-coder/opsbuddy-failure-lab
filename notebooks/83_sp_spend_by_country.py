# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier spend demo · spend by country (3 of 3)
# MAGIC `sp_purchase_orders` + `sp_suppliers` → `sp_spend_by_country`: per supplier country,
# MAGIC the suppliers, preferred suppliers, POs and USD spend (cancelled POs excluded), open PO
# MAGIC value and first/last PO date (notebooks/common/supplier_spend.py). Procurement reads it
# MAGIC to plan supplier diversification by country.

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

from notebooks.common.supplier_spend import SPEND_COLUMNS, spend_by_country

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

orders = spark.table(f"{target_schema}.sp_purchase_orders")
suppliers = spark.table(f"{target_schema}.sp_suppliers")
spend_by_country(orders, suppliers).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.sp_spend_by_country")

# COMMAND ----------

# MAGIC %md ## The report adds up to the orders and suppliers: nothing lost or counted twice

# COMMAND ----------

report = spark.table(f"{target_schema}.sp_spend_by_country")
live = orders.filter(F.col("status") != "cancelled")
assert report.columns == SPEND_COLUMNS, f"unexpected columns {report.columns}"
assert report.agg(F.sum("suppliers_count")).collect()[0][0] == suppliers.count(), "supplier count"
assert report.agg(F.sum("po_count")).collect()[0][0] == live.count(), "PO count mismatch"
assert (
    report.agg(F.sum("total_spend_usd")).collect()[0][0]
    == live.agg(F.sum("amount_usd")).collect()[0][0]
), "spend mismatch"
print(f"sp_spend_by_country: {report.count()} rows")
