# Databricks notebook source
# MAGIC %md
# MAGIC # 08_silver_pricing_calc
# MAGIC
# MAGIC Applies active promotions to the current product catalog to compute discounted prices,
# MAGIC used by the pricing team's alert and reporting jobs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md ## Current active promotions

# COMMAND ----------

promo_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("discount_pct", DoubleType(), False),
])

promotions = [
    (7001, 0.10),
    (7002, 0.15),
    (7003, 0.05),
    (7005, 0.20),
    (7006, 0.10),
    (7008, 0.25),
]

promo_df = spark.createDataFrame(promotions, schema=promo_schema)
promo_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.active_promotions")
print(f"Active promotions written: {promo_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Join products with promotions to compute discounted prices

# COMMAND ----------

active_products = spark.table("dev.opsbuddy_test.bronze_active_products")
promos = spark.table("dev.opsbuddy_test.active_promotions")

silver_pricing = (
    active_products
    .join(promos, on="product_id", how="inner")
    .withColumn("discounted_price", F.col("base_price") * (1 - F.col("discount_pct")))
)

silver_pricing.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.silver_pricing")
print(f"Silver pricing written: {silver_pricing.count()} rows")
display(silver_pricing)
