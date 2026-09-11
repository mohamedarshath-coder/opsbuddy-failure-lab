# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Balance Upsert (Delta MERGE)
# MAGIC Applies today's incremental balance-change feed onto the customer balances table via a
# MAGIC Delta `MERGE INTO` -- the standard, real-world upsert pattern. A genuinely common
# MAGIC production failure mode for this exact pattern: if the upstream feed contains more than
# MAGIC one row for the same key in the same batch (a duplicate, a late-arriving correction that
# MAGIC wasn't deduplicated, or a retry that re-sent a row), Delta's MERGE refuses to proceed --
# MAGIC deterministically, every time, regardless of cluster size or data volume (unlike an
# MAGIC OOM-style bug, this doesn't depend on scale to reproduce).

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md ## Existing customer balances (the MERGE target)

# COMMAND ----------

balances = spark.createDataFrame([
    Row(customer_id="C1001", balance=250.00),
    Row(customer_id="C1002", balance=1200.50),
    Row(customer_id="C1003", balance=75.25),
])
balances.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.customer_balances")

# COMMAND ----------

# MAGIC %md ## Today's incremental balance-change feed
# MAGIC BUG: C1002 appears TWICE in today's feed -- a real, common cause in production: the
# MAGIC upstream system retried a delivery after a timeout without checking whether the first
# MAGIC attempt actually succeeded, landing the same customer's change twice in one batch.

# COMMAND ----------

balance_changes = spark.createDataFrame([
    Row(customer_id="C1001", change_amount=50.00),
    Row(customer_id="C1002", change_amount=-100.00),
    Row(customer_id="C1002", change_amount=-100.00),  # duplicate -- the real bug
    Row(customer_id="C1003", change_amount=10.00),
])
balance_changes.createOrReplaceTempView("balance_changes")

# COMMAND ----------

# MAGIC %md ## Upsert via MERGE INTO
# MAGIC This is where it fails: Delta requires at most one matching source row per target row --
# MAGIC "multiple source rows matched" is not a data-quality warning, it's a hard MERGE error.

# COMMAND ----------

spark.sql("""
    MERGE INTO dev.opsbuddy_test.customer_balances AS target
    USING balance_changes AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET target.balance = target.balance + source.change_amount
    WHEN NOT MATCHED THEN INSERT (customer_id, balance) VALUES (source.customer_id, source.change_amount)
""")

print("Balance upsert complete")
