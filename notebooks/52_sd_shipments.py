# Databricks notebook source
# MAGIC %md
# MAGIC # Shipment SLA pipeline · 2 of 3 · Shipments (child of warehouses)
# MAGIC Builds `sd_shipments` from the raw carrier feed `sd_carrier_feed`: one row per
# MAGIC shipment, 8 columns, with typed dates, weight in kg, freight in USD and transit days.
# MAGIC Runs after notebook 51; feeds `sd_carrier_sla` (notebook 53).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
dbutils.widgets.text("feed_table", "dev.opsbuddy_test.sd_carrier_feed")
target_schema = dbutils.widgets.get("target_schema")
feed_table = dbutils.widgets.get("feed_table")

# COMMAND ----------

feed = spark.table(feed_table)
sd_shipments = feed.select(
    F.col("shipment_ref").alias("shipment_id"),
    F.col("warehouse_code").alias("warehouse_id"),
    F.col("carrier_name").alias("carrier"),
    F.to_date("shipped_at").alias("shipped_on"),
    F.expr("to_date(nullif(delivered_at, ''))").alias("delivered_on"),
    F.col("weight").cast("double").alias("weight_kg"),
    F.col("freight_charge").cast("decimal(12,2)").alias("freight_usd"),
).withColumn("transit_days", F.datediff("delivered_on", "shipped_on"))
sd_shipments.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.sd_shipments"
)

# COMMAND ----------

# MAGIC %md ## Every shipment is kept, has a weight and belongs to a known warehouse

# COMMAND ----------

written = spark.table(f"{target_schema}.sd_shipments")
assert written.count() == feed.count(), "shipments lost between the feed and sd_shipments"
assert written.filter(F.col("weight_kg").isNull()).count() == 0, "shipment without a weight"
orphans = written.join(
    spark.table(f"{target_schema}.sd_warehouses"), on="warehouse_id", how="left_anti"
).count()
assert orphans == 0, f"{orphans} shipment(s) have no warehouse in sd_warehouses"
print(f"sd_shipments: {written.count()} shipments, freight {written.agg(F.sum('freight_usd')).collect()[0][0]}")
