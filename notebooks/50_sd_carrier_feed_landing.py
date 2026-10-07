# Databricks notebook source
# MAGIC %md
# MAGIC # Shipment SLA · landing · Raw carrier feed (upstream, not part of the pipeline job)
# MAGIC Simulates the carriers' daily export landing in `sd_carrier_feed`: 120 shipments,
# MAGIC 8 text columns, as each carrier sends them. `feed_version` v1 is the format every
# MAGIC carrier has used so far; v2 is the batch after Kestrel Express changed its export
# MAGIC (weights with units and thousands separators, freight with a currency sign).
# MAGIC The pipeline (notebooks 51 to 53) reads this table and never writes it.

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

from notebooks.common.carrier_feed import COLUMNS, feed_rows

dbutils.widgets.text("feed_table", "dev.opsbuddy_test.sd_carrier_feed")
dbutils.widgets.dropdown("feed_version", "v1", ["v1", "v2"])
feed_table = dbutils.widgets.get("feed_table")
feed_version = dbutils.widgets.get("feed_version")

# COMMAND ----------

spark.createDataFrame(feed_rows(feed_version), COLUMNS).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(feed_table)
print(f"{feed_table}: {spark.table(feed_table).count()} rows, feed {feed_version}")
