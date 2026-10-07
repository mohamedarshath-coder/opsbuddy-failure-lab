# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline v2 · 1 of 3 · Customers (parent)
# MAGIC Builds `cc2_customers`, the customer master from the CRM. `country` is typed by sales
# MAGIC reps, so the same country arrives in many spellings; it is passed through as received.
# MAGIC Downstream: `cc2_orders` (notebook 42) and `cc2_revenue_by_country` (notebook 43).

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

from datetime import date, timedelta

from notebooks.common.country_spellings import SPELLINGS

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md ## Two customers per country; the CRM spelling rotates through that country's list

# COMMAND ----------

customers = []
for c, spellings in enumerate(SPELLINGS):
    for k in range(2):
        n = c * 2 + k + 1
        customers.append(
            (
                f"CU{n:03d}",
                f"Customer {n:03d}",
                spellings[(c + k) % len(spellings)],
                date(2025, 1, 1) + timedelta(days=(n * 11) % 365),
            )
        )
cc2_customers = spark.createDataFrame(
    customers, ["customer_id", "customer_name", "country", "signed_up_on"]
)
cc2_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc2_customers"
)

# COMMAND ----------

written = spark.table(f"{target_schema}.cc2_customers")
assert written.count() == len(customers), "cc2_customers row count does not match"
assert written.select("customer_id").distinct().count() == len(customers), "duplicate customer_id"
print(f"cc2_customers: {len(customers)} customers, {written.select('country').distinct().count()} country values")
