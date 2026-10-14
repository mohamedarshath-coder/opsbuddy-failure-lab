# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · finance KPI mart (7 of 7)
# MAGIC `bl_mrr_monthly` + `bl_ar_aging` → `bl_finance_kpis`: per country, MRR and ARR
# MAGIC for the reporting month, open receivables, the share over 90 days and days
# MAGIC sales outstanding.

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

from notebooks.common.billing import AS_OF, KPI_COLUMNS, finance_kpis

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

mrr = spark.table(f"{target_schema}.bl_mrr_monthly")
aging = spark.table(f"{target_schema}.bl_ar_aging")
finance_kpis(mrr, aging).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_finance_kpis"
)
written = spark.table(f"{target_schema}.bl_finance_kpis")
assert written.columns == KPI_COLUMNS, f"unexpected columns {written.columns}"
assert written.select("account_country").distinct().count() == written.count(), "duplicate country"
latest = mrr.filter(F.col("month") == F.trunc(F.lit(AS_OF), "month"))
assert (
    written.agg(F.sum("mrr_usd")).collect()[0][0] == latest.agg(F.sum("mrr_usd")).collect()[0][0]
), "MRR does not reconcile with bl_mrr_monthly"
assert (
    written.agg(F.sum("open_ar_usd")).collect()[0][0]
    == aging.agg(F.sum("open_usd")).collect()[0][0]
), "open receivables do not reconcile with bl_ar_aging"
print(f"bl_finance_kpis: {written.count()} countries")
