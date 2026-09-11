# Databricks notebook source
# MAGIC %md
# MAGIC # Flag High-Risk Accounts (Nightly)
# MAGIC Applies the shared high-risk discrepancy rule from `common/discrepancy_rules.py` to a
# MAGIC synthetic reconciliation snapshot, then asserts a standing business invariant: VIP
# MAGIC accounts should never get auto-flagged by the standard threshold, since larger balances
# MAGIC (and larger discrepancies) are normal for them.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys, os

# Dynamically resolve the repo root onto sys.path -- needed when this notebook runs as part of
# a Databricks Asset Bundle deployment (workspace path like .../.bundle/<name>/<target>/files/
# notebooks/11_...), where the repo root is NOT automatically on sys.path the way it is for a
# job whose task Source is a direct Git-provider link. Confirmed necessary in practice: this
# exact notebook worked fine as a git-linked job (repo root on sys.path automatically) but threw
# "ModuleNotFoundError: No module named 'notebooks'" the first time it ran as a bundle job --
# computed dynamically here (not hardcoded) so it keeps working regardless of which workspace
# path a future bundle/target/deployment actually lands this notebook at.
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_repo_root = "/Workspace" + os.path.dirname(os.path.dirname(_notebook_path))
if _repo_root not in sys.path:
    sys.path.append(_repo_root)

from notebooks.common.discrepancy_rules import is_high_risk

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a synthetic reconciliation snapshot
# MAGIC Includes both standard and VIP-tier accounts. VIP accounts routinely carry larger
# MAGIC balances, so a discrepancy that would be alarming on a standard account is unremarkable
# MAGIC on a VIP one.

# COMMAND ----------

snapshot = spark.createDataFrame(
    [
        ("ACC0001", "standard", 250.0),  # standard, large discrepancy -- should be flagged
        ("ACC0002", "standard", 15.0),  # standard, small -- should not be flagged
        ("ACC0003", "vip", 150.0),  # vip, moderate for a VIP -- should NOT be flagged
        ("ACC0004", "vip", 15.0),  # vip, small -- should not be flagged
    ],
    ["account_id", "tier", "discrepancy"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flag using the shared rule
# MAGIC Then check the standing invariant that no VIP account was flagged by the standard
# MAGIC threshold -- support escalated a false-positive VIP flag once before, so this is a real
# MAGIC regression check, not just a smoke test.

# COMMAND ----------

is_high_risk_udf = F.udf(is_high_risk, BooleanType())
flagged = snapshot.filter(is_high_risk_udf(F.col("discrepancy"), F.col("tier")))
flagged.write.mode("overwrite").saveAsTable(f"{target_schema}.high_risk_accounts_demo")

vip_flagged_count = flagged.filter(F.col("tier") == "vip").count()
assert vip_flagged_count == 0, (
    f"{vip_flagged_count} VIP account(s) were flagged high-risk using the standard threshold -- "
    f"VIP accounts need their own, higher threshold, since larger discrepancies are normal for "
    f"them."
)
print(f"Flagged {flagged.count()} high-risk account(s); 0 of them VIP, as expected")
