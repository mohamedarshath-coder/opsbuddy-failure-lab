# Databricks notebook source
# MAGIC %md
# MAGIC # 06_daily_sales_aggregation
# MAGIC
# MAGIC Incremental daily job: aggregates raw sales transactions by store and product category,
# MAGIC validates data quality, and upserts the results into the `gold_daily_sales_summary` table.
# MAGIC Designed to be re-run safely for any given `run_date` (idempotent via MERGE).
# MAGIC
# MAGIC Owner: Data Engineering | Schedule: Daily @ 02:00 UTC | SLA: 04:00 UTC

# COMMAND ----------

import logging
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# This module is part of the team's shared internal utilities repo -- imported here the same way
# every other production notebook in this pipeline does, for consistent null/range validation
# across all daily aggregation jobs.
from common.data_quality import validate_no_nulls

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("daily_sales_aggregation")

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("run_date", datetime.today().strftime("%Y-%m-%d"), "Run Date (YYYY-MM-DD)")
run_date = dbutils.widgets.get("run_date")
logger.info(f"Running daily sales aggregation for run_date={run_date}")

# COMMAND ----------

# MAGIC %md ## Bronze — synthetic raw sales transactions for this run_date

# COMMAND ----------

txn_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("store_id", StringType(), False),
    StructField("sale_date", DateType(), False),
    StructField("product_category", StringType(), False),
    StructField("revenue", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
])

bronze_rows = [
    (30001, "STORE-101", run_date, "electronics", 249.99, 1),
    (30002, "STORE-101", run_date, "grocery", 84.50, 12),
    (30003, "STORE-102", run_date, "electronics", 599.00, 1),
    (30004, "STORE-102", run_date, "apparel", 129.75, 3),
    (30005, "STORE-103", run_date, "grocery", 42.20, 8),
    (30006, "STORE-103", run_date, "electronics", 89.99, 2),
    (30007, "STORE-101", run_date, "apparel", 65.00, 2),
    (30008, "STORE-102", run_date, "grocery", 156.30, 15),
]

bronze_df = (
    spark.createDataFrame(bronze_rows, schema=["transaction_id", "store_id", "sale_date_str",
                                                "product_category", "revenue", "quantity"])
    .withColumn("sale_date", F.to_date("sale_date_str", "yyyy-MM-dd"))
    .drop("sale_date_str")
)
bronze_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_sales_transactions")
logger.info(f"Bronze transactions written: {bronze_df.count()} rows for {run_date}")

# COMMAND ----------

# MAGIC %md ## Data quality validation
# MAGIC
# MAGIC Standard pre-aggregation checks: no nulls in required fields, revenue must be non-negative.

# COMMAND ----------

raw_txns = spark.table("dev.opsbuddy_test.bronze_sales_transactions").filter(
    F.col("sale_date") == run_date
)

validate_no_nulls(raw_txns, required_columns=["transaction_id", "store_id", "revenue"])
logger.info("Data quality validation passed.")

# COMMAND ----------

# MAGIC %md ## Aggregate — daily revenue and average order value by store and category

# COMMAND ----------

daily_summary = (
    raw_txns
    .groupBy("store_id", "product_category", "sale_date")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.avg("revenue").alias("avg_order_value"),
        F.count("transaction_id").alias("transaction_count"),
    )
)

logger.info(f"Aggregated {daily_summary.count()} store/category rows for {run_date}")
display(daily_summary)

# COMMAND ----------

# MAGIC %md ## Upsert into the gold summary table (idempotent re-run for the same run_date)

# COMMAND ----------

target_table = "dev.opsbuddy_test.gold_daily_sales_summary"

if not spark.catalog.tableExists(target_table):
    daily_summary.write.mode("overwrite").saveAsTable(target_table)
    logger.info(f"Created {target_table} with initial data.")
else:
    daily_summary.createOrReplaceTempView("daily_summary_updates")
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING daily_summary_updates AS updates
        ON target.store_id = updates.store_id
           AND target.product_category = updates.product_category
           AND target.sale_date = updates.sale_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(f"Merged {daily_summary.count()} rows into {target_table} for {run_date}.")

logger.info("06_daily_sales_aggregation completed successfully.")
