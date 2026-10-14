# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · silver subscriptions (2 of 7)
# MAGIC `bl_subscriptions_bronze` + `bl_plans_bronze` + `bl_accounts_bronze` →
# MAGIC `bl_subscriptions_silver`: one row per subscription, typed, with its plan tier,
# MAGIC the account's country and industry, and MRR in USD
# MAGIC (notebooks/common/billing.py). Feeds the MRR (94) and churn (95) jobs.

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

from notebooks.common.billing import SUBSCRIPTION_COLUMNS, subscriptions_silver

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

silver = subscriptions_silver(
    spark.table(f"{target_schema}.bl_subscriptions_bronze"),
    spark.table(f"{target_schema}.bl_plans_bronze"),
    spark.table(f"{target_schema}.bl_accounts_bronze"),
)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_subscriptions_silver"
)
written = spark.table(f"{target_schema}.bl_subscriptions_silver")
bronze = spark.table(f"{target_schema}.bl_subscriptions_bronze")
assert written.columns == SUBSCRIPTION_COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == bronze.select(F.trim("subscription_id")).distinct().count(), (
    "a subscription was lost or repeated"
)
assert written.filter(F.col("mrr_usd").isNull()).count() == 0, "an MRR could not be computed"
print(f"bl_subscriptions_silver: {written.count()} subscriptions")
