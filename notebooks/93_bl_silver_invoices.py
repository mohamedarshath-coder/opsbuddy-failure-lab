# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · silver invoices (3 of 7)
# MAGIC `bl_invoices_bronze` + `bl_payments_bronze` + `bl_accounts_bronze` →
# MAGIC `bl_invoices_silver`: one row per invoice or credit note, in USD, with what was
# MAGIC paid against it and its status (notebooks/common/billing.py). Feeds the AR aging
# MAGIC job (96).

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

from notebooks.common.billing import INVOICE_COLUMNS, invoices_silver

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = invoices_silver(
    spark.table(f"{target_schema}.bl_invoices_bronze"),
    spark.table(f"{target_schema}.bl_payments_bronze"),
    spark.table(f"{target_schema}.bl_accounts_bronze"),
)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_invoices_silver"
)
written = spark.table(f"{target_schema}.bl_invoices_silver")
bronze = spark.table(f"{target_schema}.bl_invoices_bronze")
assert written.columns == INVOICE_COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == bronze.select(F.trim("invoice_id")).distinct().count(), (
    "an invoice was lost or repeated"
)
assert written.filter(F.col("amount_usd").isNull()).count() == 0, "an amount did not convert"
print(f"bl_invoices_silver: {written.count()} invoices and credit notes")
