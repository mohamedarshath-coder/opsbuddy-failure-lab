# Databricks notebook source
# MAGIC %md
# MAGIC # 02_build_active_products
# MAGIC
# MAGIC Builds the silver-layer active-products dimension table, used by downstream inventory
# MAGIC and reporting jobs to identify which SKUs are currently sellable.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md ## Bronze — raw product catalog feed

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("unit_price", DoubleType(), False),
])

bronze_products = [
    (5001, "Wireless Mouse", "ACTIVE", 24.99),
    (5002, "USB-C Hub", "ACTIVE", 39.99),
    (5003, "Mechanical Keyboard", "ACTIVE", 89.99),
    (5004, "Webcam 1080p", "DISCONTINUED", 49.99),
    (5005, "Laptop Stand", "ACTIVE", 34.99),
    (5006, "Desk Lamp", "ACTIVE", 22.50),
    (5007, "Monitor Arm", "ACTIVE", 64.99),
    (5008, "Cable Organizer", "DISCONTINUED", 9.99),
]

bronze_df = spark.createDataFrame(bronze_products, schema=product_schema)
bronze_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_products")
print(f"Bronze products written: {bronze_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Silver — active products only

# COMMAND ----------

silver_df = (
    spark.table("dev.opsbuddy_test.bronze_products")
    .filter(F.col("status") == "ACTIVE")
    .select("product_id", "product_name", "unit_price")
)

silver_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.silver_active_products")
print(f"Silver active products written: {silver_df.count()} rows")
display(silver_df)
