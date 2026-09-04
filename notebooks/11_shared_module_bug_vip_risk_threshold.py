# Databricks notebook source
# MAGIC %md
# MAGIC # Flag High-Risk Accounts (Nightly)
# MAGIC Applies the shared high-risk discrepancy rule from `common/discrepancy_rules.py` to a
# MAGIC synthetic reconciliation snapshot, then asserts a standing business invariant: VIP
# MAGIC accounts should never get auto-flagged by the standard threshold, since larger balances
# MAGIC (and larger discrepancies) are normal for them.

# COMMAND ----------

from pyspark.sql import functions as F
from notebooks.common.discrepancy_rules import is_high_risk

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

flagged = snapshot.filter(F.udf(is_high_risk, "boolean")(F.col("discrepancy")))
flagged.write.mode("overwrite").saveAsTable("default.high_risk_accounts_demo")

vip_flagged_count = flagged.filter(F.col("tier") == "vip").count()
assert vip_flagged_count == 0, (
    f"{vip_flagged_count} VIP account(s) were flagged high-risk using the standard threshold -- "
    f"VIP accounts need their own, higher threshold, since larger discrepancies are normal for "
    f"them."
)
print(f"Flagged {flagged.count()} high-risk account(s); 0 of them VIP, as expected")
