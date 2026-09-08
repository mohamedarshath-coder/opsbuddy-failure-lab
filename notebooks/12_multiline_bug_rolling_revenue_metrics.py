# Databricks notebook source
# MAGIC %md
# MAGIC # Rolling Revenue Metrics
# MAGIC Computes 7-day rolling revenue, order count, and average order value per store,
# MAGIC for the merchandising team's trend dashboard.

# COMMAND ----------

import random

from pyspark.sql import functions as F
from pyspark.sql.window import Window

random.seed(11)

rows = []
for i in range(1, 501):
    store_id = f"STORE{random.randint(1, 10):03d}"
    txn_date = f"2026-08-{random.randint(1, 30):02d}"
    revenue = round(random.uniform(50, 500), 2)
    rows.append((store_id, txn_date, revenue))

daily_sales = spark.createDataFrame(rows, ["store_id", "txn_date", "revenue"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize the date column name
# MAGIC The reporting layer's naming convention going forward is `transaction_date`, not
# MAGIC `txn_date` -- renaming here so this table's schema matches every other table in
# MAGIC the reporting layer.

# COMMAND ----------

daily_sales = daily_sales.withColumnRenamed("txn_date", "transaction_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling 7-day metrics per store
# MAGIC Three related metrics, each windowed the same way: revenue, order count, and the
# MAGIC derived average order value.

# COMMAND ----------

metrics = (
    daily_sales.withColumn(
        "rolling_revenue",
        F.sum("revenue").over(
            Window.partitionBy("store_id").orderBy("transaction_date").rowsBetween(-6, 0)
        ),
    )
    .withColumn(
        "rolling_order_count",
        F.count("revenue").over(
            Window.partitionBy("store_id").orderBy("transaction_date").rowsBetween(-6, 0)
        ),
    )
    .withColumn(
        "rolling_avg_order_value",
        F.avg("revenue").over(
            Window.partitionBy("store_id").orderBy("transaction_date").rowsBetween(-6, 0)
        ),
    )
)

print(f"Computed rolling metrics for {metrics.count()} rows")
display(metrics)
