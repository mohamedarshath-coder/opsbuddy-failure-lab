# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline v2 · 2 of 3 · Orders (child of customers)
# MAGIC Builds `cc2_orders` from the order system: one to three orders per customer in
# MAGIC `cc2_customers`, 8 columns. Each order carries the country the order system recorded,
# MAGIC spelled independently of the CRM and standardized here to the ISO 3166-1 alpha-3 code. Runs after notebook 41; feeds `cc2_revenue_by_country` (notebook 43).

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

CATEGORIES = ["Electronics", "Home", "Fashion", "Grocery", "Sports"]
CHANNELS = ["Web", "Store", "Partner"]

# COMMAND ----------

orders = []
order_no = 0
for c, spellings in enumerate(SPELLINGS):
    for k in range(2):
        n = c * 2 + k + 1
        for j in range(1 + (n % 3)):
            order_no += 1
            amount = Decimal(25 + (n * 37 + j * 53) % 400) + Decimal("0.99")
            orders.append(
                (
                    f"OR{order_no:04d}",
                    f"CU{n:03d}",
                    date(2026, 9, 1) + timedelta(days=(n + j * 7) % 30),
                    CATEGORIES[(n + j) % len(CATEGORIES)],
                    1 + (n + j) % 5,
                    str(amount),
                    CHANNELS[(n * 2 + j) % len(CHANNELS)],
                    to_iso3(spellings[(c + k + j + 1) % len(spellings)]),
                )
            )
COLUMNS = [
    "order_id", "customer_id", "ordered_on", "product_category",
    "quantity", "amount", "channel", "country",
]
cc2_orders = (
    spark.createDataFrame(orders, COLUMNS)
    .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    .withColumn("quantity", F.col("quantity").cast("int"))
)
cc2_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc2_orders"
)

# COMMAND ----------

# MAGIC %md ## Every order belongs to a customer in the parent table

# COMMAND ----------

written = spark.table(f"{target_schema}.cc2_orders")
assert written.columns == COLUMNS, f"unexpected columns {written.columns}"
orphans = written.join(
    spark.table(f"{target_schema}.cc2_customers"), on="customer_id", how="left_anti"
).count()
assert orphans == 0, f"{orphans} order(s) have no customer in cc2_customers"
assert written.select("order_id").distinct().count() == len(orders), "duplicate order_id"
assert written.filter(~F.col("country").isin(ISO3)).count() == 0, "country not an ISO code"
assert written.filter(~F.col("country").rlike("^[A-Z]{3}$")).count() == 0, "country format"
print(f"cc2_orders: {len(orders)} orders, total {written.agg(F.sum('amount')).collect()[0][0]}")
