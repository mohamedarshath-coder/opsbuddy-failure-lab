# Databricks notebook source
# MAGIC %md
# MAGIC # Shipment SLA pipeline · 3 of 3 · Carrier SLA report (child of shipments)
# MAGIC Builds `sd_carrier_sla` for the weekly carrier review: per carrier, shipments,
# MAGIC delivered, on time against the carrier's promise, on-time %, average transit days,
# MAGIC total freight and freight per kg (8 columns). Runs after notebook 52.

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

from itertools import chain

from pyspark.sql import functions as F

from notebooks.common.carrier_sla_rules import PROMISED_DAYS

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# The carrier's promise as a Spark map, so the rule runs on the executors as an
# expression (same rule as carrier_sla_rules.is_on_time).
promise = F.create_map(*[F.lit(x) for x in chain(*PROMISED_DAYS.items())])
shipments = (
    spark.table(f"{target_schema}.sd_shipments")
    .withColumn("promised_days", promise[F.col("carrier")])
    .withColumn("on_time", F.col("transit_days") <= F.col("promised_days"))
)
unknown = shipments.filter(F.col("promised_days").isNull()).select("carrier").distinct().collect()
assert not unknown, f"no delivery promise for carrier(s): {[r.carrier for r in unknown]}"
sd_carrier_sla = shipments.groupBy("carrier").agg(
    F.count("*").alias("shipments_count"),
    F.count("delivered_on").alias("delivered_count"),
    F.count(F.when(F.col("on_time"), True)).alias("on_time_count"),
    F.round(F.count(F.when(F.col("on_time"), True)) * 100.0 / F.count("delivered_on"), 1)
    .cast("decimal(5,1)")
    .alias("on_time_pct"),
    F.round(F.avg("transit_days"), 2).cast("decimal(6,2)").alias("avg_transit_days"),
    F.sum("freight_usd").cast("decimal(14,2)").alias("total_freight_usd"),
    F.round(F.sum("freight_usd") / F.sum("weight_kg"), 2).cast("decimal(10,2)").alias("freight_per_kg"),
)
sd_carrier_sla.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.sd_carrier_sla"
)

# COMMAND ----------

# MAGIC %md ## The report adds up to the shipments

# COMMAND ----------

report = spark.table(f"{target_schema}.sd_carrier_sla")
assert report.agg(F.sum("shipments_count")).collect()[0][0] == shipments.count(), "shipment count mismatch"
assert (
    report.agg(F.sum("total_freight_usd")).collect()[0][0]
    == shipments.agg(F.sum("freight_usd")).collect()[0][0]
), "freight total mismatch"
print(f"sd_carrier_sla: {report.count()} carriers")
