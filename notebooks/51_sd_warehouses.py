# Databricks notebook source
# MAGIC %md
# MAGIC # Shipment SLA pipeline · 1 of 3 · Warehouses (parent)
# MAGIC Builds `sd_warehouses`, the warehouse master: 12 warehouses, 8 columns.
# MAGIC Downstream: `sd_shipments` (notebook 52) and `sd_carrier_sla` (notebook 53).

# COMMAND ----------

from datetime import date

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

WAREHOUSES = [
    ("WH01", "Riverside Hub", "North", "Leeds", 4200, date(2019, 3, 4), "Inbound A", True),
    ("WH02", "Canal Street DC", "North", "Manchester", 3800, date(2020, 6, 15), "Inbound A", True),
    ("WH03", "Harbour Gate", "South", "Southampton", 5100, date(2018, 1, 22), "Port Ops", True),
    ("WH04", "Oakfield Depot", "South", "Reading", 2600, date(2021, 9, 1), "Inbound B", True),
    ("WH05", "Eastmoor Park", "East", "Norwich", 3100, date(2017, 11, 13), "Inbound B", True),
    ("WH06", "Fenland Store", "East", "Cambridge", 2200, date(2022, 2, 28), "Outbound C", True),
    ("WH07", "Westway Logistics", "West", "Bristol", 4700, date(2016, 5, 9), "Outbound C", True),
    ("WH08", "Severn Cross", "West", "Cardiff", 3300, date(2019, 8, 19), "Port Ops", True),
    ("WH09", "Midland Link", "Central", "Birmingham", 6000, date(2015, 4, 6), "Hub Ops", True),
    ("WH10", "Trent Valley", "Central", "Nottingham", 2900, date(2023, 1, 9), "Hub Ops", True),
    ("WH11", "Highland Point", "North", "Glasgow", 2400, date(2020, 10, 5), "Outbound D", True),
    ("WH12", "Old Mill Annex", "Central", "Leicester", 900, date(2014, 7, 21), "Hub Ops", False),
]
COLUMNS = [
    "warehouse_id", "warehouse_name", "region", "city",
    "capacity_pallets", "opened_on", "ops_team", "is_active",
]
spark.createDataFrame(WAREHOUSES, COLUMNS).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{target_schema}.sd_warehouses")

# COMMAND ----------

written = spark.table(f"{target_schema}.sd_warehouses")
assert written.columns == COLUMNS, f"unexpected columns {written.columns}"
assert written.count() == len(WAREHOUSES), "sd_warehouses row count does not match"
print(f"sd_warehouses: {written.count()} warehouses")
