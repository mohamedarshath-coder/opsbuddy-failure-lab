# Databricks notebook source
# MAGIC %md
# MAGIC # Assess Late Fees (Nightly)
# MAGIC Applies the shared late-fee rule from `common/late_fee_rules.py` to a synthetic overdue-
# MAGIC accounts snapshot, then asserts a standing regulatory invariant: international accounts
# MAGIC must be capped at a LOWER maximum fee than domestic accounts, since several jurisdictions
# MAGIC this company operates in cap late fees below the US domestic $100 maximum -- legal flagged
# MAGIC a domestic-rate fee charged to an international account once before as a compliance risk.

# COMMAND ----------

import sys, os

# Dynamically resolve the repo root onto sys.path -- needed when this notebook runs as part of a
# Databricks Asset Bundle deployment, where the repo root is NOT automatically on sys.path the
# way it is for a job whose task Source is a direct Git-provider link. General version: finds
# "/files/" in this notebook's own workspace path and treats everything up to and including it as
# the repo root, correct regardless of how deeply nested this notebook is under the bundle.
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_marker = "/files/"
_marker_idx = _notebook_path.find(_marker)
_repo_root = (
    "/Workspace" + _notebook_path[: _marker_idx + len(_marker) - 1]
    if _marker_idx != -1
    else None
)
if _repo_root and _repo_root not in sys.path:
    sys.path.append(_repo_root)

from pyspark.sql import functions as F
from notebooks.common.late_fee_rules import calculate_late_fee

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a synthetic overdue-accounts snapshot
# MAGIC Includes both domestic and international accounts, several severely overdue enough (30+
# MAGIC days) to hit the $100 flat cap under the current, region-blind rule.

# COMMAND ----------

overdue_accounts = spark.createDataFrame(
    [
        ("ACC2001", "domestic", 10),       # domestic, moderate -- should be capped at $100 max
        ("ACC2002", "domestic", 40),       # domestic, severe -- should hit the $100 cap
        ("ACC2003", "international", 5),   # international, small -- under either cap, fine either way
        ("ACC2004", "international", 40),  # international, severe -- must be capped at $50, NOT $100
    ],
    ["account_id", "region", "days_overdue"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate fees using the shared rule
# MAGIC Then check the standing invariant that no international account was ever charged more than
# MAGIC the international cap, regardless of how overdue it is.

# COMMAND ----------

calculate_late_fee_udf = F.udf(calculate_late_fee, "double")
assessed = overdue_accounts.withColumn(
    "late_fee", calculate_late_fee_udf(F.col("days_overdue"), F.col("region"))
)
assessed.write.mode("overwrite").saveAsTable(f"{target_schema}.late_fee_assessments_demo")

INTERNATIONAL_CAP = 50.0
max_international_fee = (
    assessed.filter(F.col("region") == "international")
    .agg(F.max("late_fee"))
    .collect()[0][0]
)
assert max_international_fee <= INTERNATIONAL_CAP, (
    f"An international account was charged ${max_international_fee:.2f}, exceeding the "
    f"${INTERNATIONAL_CAP:.2f} international cap -- calculate_late_fee needs to apply a lower, "
    f"region-aware maximum for international accounts instead of the flat domestic cap."
)
print(
    f"Assessed {assessed.count()} account(s); max international fee "
    f"${max_international_fee:.2f}, within the ${INTERNATIONAL_CAP:.2f} cap as expected"
)
