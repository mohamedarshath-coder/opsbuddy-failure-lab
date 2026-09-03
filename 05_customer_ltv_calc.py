# Databricks notebook source
# MAGIC %md
# MAGIC # 05_customer_ltv_calc
# MAGIC
# MAGIC Calculates a simple lifetime-value estimate per customer from recent order history, used
# MAGIC by the marketing team's tiering dashboard.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md ## Bronze — recent customer order summaries

# COMMAND ----------

summary_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_tier", StringType(), False),
    StructField("avg_order_value", DoubleType(), True),
    StructField("orders_last_year", IntegerType(), False),
])

bronze_summaries = [
    (201, "gold", 145.50, 12),
    (202, "silver", 89.25, 6),
    (203, "gold", 210.00, 18),
    (204, "bronze", None, 1),
    (205, "silver", 102.75, 8),
    (206, "gold", 175.30, 14),
    (207, "bronze", None, 2),
    (208, "silver", 95.00, 5),
]

bronze_df = spark.createDataFrame(bronze_summaries, schema=summary_schema)
bronze_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_customer_summaries")
print(f"Bronze customer summaries written: {bronze_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Lifetime value estimate
# MAGIC
# MAGIC Simple estimate: average order value multiplied by orders in the last year, times a
# MAGIC tier-based retention multiplier.

# COMMAND ----------

TIER_MULTIPLIERS = {"gold": 3.5, "silver": 2.0, "bronze": 1.2}

rows = spark.table("dev.opsbuddy_test.bronze_customer_summaries").collect()

ltv_estimates = []
for row in rows:
    multiplier = TIER_MULTIPLIERS.get(row.customer_tier, 1.0)
    ltv = row.avg_order_value * row.orders_last_year * multiplier
    ltv_estimates.append((row.customer_id, row.customer_tier, ltv))

ltv_df = spark.createDataFrame(
    ltv_estimates, schema=["customer_id", "customer_tier", "estimated_ltv"]
)
ltv_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_customer_ltv")
print(f"Customer LTV estimates written: {ltv_df.count()} rows")
display(ltv_df)
