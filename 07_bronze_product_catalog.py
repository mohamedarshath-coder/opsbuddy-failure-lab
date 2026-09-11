# Databricks notebook source
# MAGIC %md
# MAGIC # 07_bronze_product_catalog
# MAGIC
# MAGIC Ingests the raw product catalog feed and filters to active SKUs only, for use by all
# MAGIC downstream pricing and inventory jobs.
# MAGIC
# MAGIC Filter logic now lives in `transforms_07_bronze_product_catalog.py` -- this
# MAGIC notebook is just an orchestrator: read, call the extracted function, write.

# COMMAND ---------------------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from transforms_07_bronze_product_catalog import filter_active_products

# COMMAND ---------------------

# MAGIC %md ## Raw product feed

# COMMAND ---------------------

raw_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("is_active", StringType(), False),
    StructField("base_price", DoubleType(), False),
])

raw_products = [
    (7001, "Bluetooth Speaker", "TRUE", 59.99),
    (7002, "Noise Cancelling Headphones", "TRUE", 149.99),
    (7003, "Charging Cable", "TRUE", 12.99),
    (7004, "Retired Tablet Model", "FALSE", 199.99),
    (7005, "Wireless Charger", "TRUE", 29.99),
    (7006, "Screen Protector", "TRUE", 9.99),
    (7007, "Old Phone Case", "FALSE", 14.99),
    (7008, "Portable Battery Pack", "TRUE", 34.99),
]

raw_df = spark.createDataFrame(raw_products, schema=raw_schema)
raw_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.raw_product_feed")
print(f"Raw product feed written: {raw_df.count()} rows")

# COMMAND ---------------------

# MAGIC %md ## Filter to active products only (via the extracted, testable module)

# COMMAND ---------------------

raw_product_table = spark.table("dev.opsbuddy_test.raw_product_feed")
active_products = filter_active_products(raw_product_table)

active_products.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_active_products")
print(f"Bronze active products written: {active_products.count()} rows")
display(active_products)


# COMMAND ---------------------

# MAGIC %md ## Business invariant: 6 of the 8 synthetic SKUs are marked active
# MAGIC Confirmed by inspecting the raw feed above -- exactly 6 rows are "TRUE". If this count
# MAGIC drifts, either the raw feed changed (expected, update this assertion) or the filter logic
# MAGIC in transforms_07_bronze_product_catalog.py is silently mismatching case variants (not
# MAGIC expected -- this is the real regression this assertion exists to catch).

# COMMAND ---------------------

actual_active_count = active_products.count()
assert actual_active_count == 6, (
    f"Expected 6 active products (6 of 8 synthetic SKUs are TRUE in the raw feed), got "
    f"{actual_active_count}. filter_active_products in transforms_07_bronze_product_catalog.py "
    f"is likely not matching all case variants of the is_active flag correctly."
)
print(f"Business invariant confirmed: {actual_active_count} active products, as expected")
