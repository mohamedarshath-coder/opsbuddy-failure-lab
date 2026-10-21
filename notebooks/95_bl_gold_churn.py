# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · gold churn cohorts (5 of 7)
# MAGIC `bl_subscriptions_silver` → `bl_churn_cohorts`: per start month, subscriptions
# MAGIC started and how many ended within three months.

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

from notebooks.common.billing import CHURN_COLUMNS, churn_cohorts

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

subs = spark.table(f"{target_schema}.bl_subscriptions_silver")
churn_cohorts(subs).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_churn_cohorts"
)
written = spark.table(f"{target_schema}.bl_churn_cohorts")
assert written.columns == CHURN_COLUMNS, f"unexpected columns {written.columns}"
assert written.agg(F.sum("subscriptions_started")).collect()[0][0] == subs.count(), (
    "cohorts do not add up to the subscriptions"
)
print(f"bl_churn_cohorts: {written.count()} cohorts")
