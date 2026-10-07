# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline v2 · 1 of 3 · Customers (parent)
# MAGIC Builds `cc2_customers`, the customer master from the CRM: 52 customers, 8 columns.
# MAGIC `country` is typed by sales reps, so the same country arrives in many spellings; it is
# MAGIC standardized to the ISO 3166-1 alpha-3 code. Every other column is unchanged.
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
from decimal import Decimal

from pyspark.sql import functions as F

from notebooks.common.country_iso import ISO3, to_iso3
from notebooks.common.country_spellings import SPELLINGS

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# One main city per country, in the same order as SPELLINGS.
CITIES = [
    "New York", "Toronto", "Mexico City", "Sao Paulo", "Buenos Aires", "London", "Dublin",
    "Paris", "Berlin", "Madrid", "Rome", "Amsterdam", "Zurich", "Stockholm", "Mumbai",
    "Shanghai", "Tokyo", "Seoul", "Singapore", "Dubai", "Riyadh", "Johannesburg", "Lagos",
    "Cairo", "Sydney", "Auckland",
]
SEGMENTS = ["Retail", "SMB", "Enterprise"]

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
                SEGMENTS[n % len(SEGMENTS)],
                to_iso3(spellings[(c + k) % len(spellings)]),
                CITIES[c],
                date(2025, 1, 1) + timedelta(days=(n * 11) % 365),
                str(Decimal(1000 + (n * 250) % 9000)),
                n % 7 != 0,
            )
        )
COLUMNS = [
    "customer_id", "customer_name", "segment", "country", "city",
    "signed_up_on", "credit_limit", "is_active",
]
cc2_customers = spark.createDataFrame(customers, COLUMNS).withColumn(
    "credit_limit", F.col("credit_limit").cast("decimal(12,2)")
)
cc2_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc2_customers"
)

# COMMAND ----------

written = spark.table(f"{target_schema}.cc2_customers")
assert written.columns == COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == len(customers), "cc2_customers row count does not match"
assert written.select("customer_id").distinct().count() == len(customers), "duplicate customer_id"
assert written.filter(~F.col("country").isin(ISO3)).count() == 0, "country not an ISO code"
assert written.filter(~F.col("country").rlike("^[A-Z]{3}$")).count() == 0, "country format"
print(f"cc2_customers: {len(customers)} customers, {written.select('country').distinct().count()} country values")
