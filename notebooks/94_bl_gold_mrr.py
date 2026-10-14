# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · gold MRR (4 of 7)
# MAGIC `bl_subscriptions_silver` → `bl_mrr_monthly`: per month, country and plan tier,
# MAGIC the active subscriptions and their MRR. Feeds the finance mart (97).

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

from notebooks.common.billing import AS_OF, MRR_COLUMNS, mrr_monthly

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

subs = spark.table(f"{target_schema}.bl_subscriptions_silver")
mrr_monthly(subs).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_mrr_monthly"
)
written = spark.table(f"{target_schema}.bl_mrr_monthly")
assert written.columns == MRR_COLUMNS, f"unexpected columns {written.columns}"
# The reporting month counts every subscription that has not ended.
now = written.filter(F.col("month") == F.trunc(F.lit(AS_OF), "month"))
active = subs.filter(F.col("end_date").isNull())
assert now.agg(F.sum("active_subscriptions")).collect()[0][0] == active.count(), (
    "active subscriptions do not reconcile"
)
months = written.select("month").distinct().count()
print(f"bl_mrr_monthly: {written.count()} rows over {months} months")
