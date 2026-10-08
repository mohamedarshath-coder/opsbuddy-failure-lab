# Databricks notebook source
# MAGIC %md
# MAGIC # customers_30_45
# MAGIC Customers aged 30 to 45 inclusive from `dev.academy.customers` (read only), all 25
# MAGIC columns unchanged, written to `<academy_schema>.customers_30_45` (full overwrite).

# COMMAND ----------

import os
import sys

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

from notebooks.common.customer_age_band import filter_age_30_45

dbutils.widgets.text("target_schema", "dev.academy")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

result = filter_age_30_45(spark.table("dev.academy.customers"))
result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.customers_30_45"
)
print(f"customers_30_45: {spark.table(f'{target_schema}.customers_30_45').count()} rows")
