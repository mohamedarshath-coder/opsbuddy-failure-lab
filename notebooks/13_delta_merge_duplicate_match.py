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
from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md ## Existing customer balances (the MERGE target)

# COMMAND ----------

balances = spark.createDataFrame([
    Row(customer_id="C1001", balance=250.00),
    Row(customer_id="C1002", balance=1200.50),
    Row(customer_id="C1003", balance=75.25),
])

# COMMAND ----------

# MAGIC %md ## Add each customer's current region (SCRUM-130)
# MAGIC Regions come from the customer-regions reference, which keeps history: a customer who
# MAGIC moved has one row per region they were in, with `is_current` marking today's region.
# MAGIC Only the current row is joined, so the join keeps exactly one row per customer
# MAGIC (SCRUM-128 joined every history row and duplicated C1002). A customer with more than
# MAGIC one current region is a reference-data error: the job stops instead of writing duplicates.

# COMMAND ----------

customer_regions = spark.createDataFrame([
    Row(customer_id="C1001", region="North", is_current=True),
    Row(customer_id="C1002", region="North", is_current=False),  # moved in March
    Row(customer_id="C1002", region="South", is_current=True),
    Row(customer_id="C1003", region="West", is_current=True),
    Row(customer_id="C1004", region="East", is_current=True),  # new customer, only in the feed
])

current_regions = customer_regions.filter(F.col("is_current")).select("customer_id", "region")
conflicts = current_regions.groupBy("customer_id").count().filter(F.col("count") > 1).collect()
if conflicts:
    raise ValueError(
        "customer-regions reference has more than one current region for: "
        + ", ".join(sorted(r["customer_id"] for r in conflicts))
    )

# A left join keeps every customer; one without a current region gets a null region.
balances_with_region = balances.join(current_regions, on="customer_id", how="left")
if balances_with_region.count() != balances.count():
    raise ValueError("adding regions changed the number of customers")
(
    balances_with_region.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{target_schema}.customer_balances")
)

# COMMAND ----------

# MAGIC %md ## Today's incremental balance-change feed
# MAGIC C1002 appears TWICE in today's feed -- a real, common cause in production: the
# MAGIC upstream system retried a delivery after a timeout without checking whether the first
# MAGIC attempt actually succeeded, landing the same customer's change twice in one batch.

# COMMAND ----------

balance_changes = spark.createDataFrame([
    Row(customer_id="C1001", change_amount=50.00),
    Row(customer_id="C1002", change_amount=-100.00),
    Row(customer_id="C1002", change_amount=-100.00),  # duplicate retry of the same change
    Row(customer_id="C1003", change_amount=10.00),
    Row(customer_id="C1004", change_amount=30.00),  # new customer, not yet in customer_balances
])

# COMMAND ----------

# MAGIC %md ## Deduplicate the feed before merging
# MAGIC Delta's `MERGE INTO` allows at most one matching source row per target row. Pre-aggregate
# MAGIC the feed by `customer_id` (summing `change_amount`) so duplicate/retried rows for the same
# MAGIC customer collapse into a single net change before the MERGE ever sees them. This also
# MAGIC correctly nets out legitimate cases where a customer has more than one real change in the
# MAGIC same batch, rather than arbitrarily keeping or dropping one row.

# COMMAND ----------

balance_changes_deduped = (
    balance_changes
    .groupBy("customer_id")
    .agg(F.sum("change_amount").alias("change_amount"))
)

# SCRUM-132: give each feed customer their current region so new customers are inserted with it.
# current_regions has one row per customer (checked above), so this join keeps the row count.
balance_changes_with_region = balance_changes_deduped.join(current_regions, on="customer_id", how="left")
if balance_changes_with_region.count() != balance_changes_deduped.count():
    raise ValueError("adding regions changed the number of feed rows")
balance_changes_with_region.createOrReplaceTempView("balance_changes")

# COMMAND ----------

# MAGIC %md ## Upsert via MERGE INTO
# MAGIC With the source pre-aggregated to at most one row per `customer_id`, the MERGE below no
# MAGIC longer hits the "multiple source rows matched" ambiguity.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {target_schema}.customer_balances AS target
    USING balance_changes AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET target.balance = target.balance + source.change_amount
    WHEN NOT MATCHED THEN INSERT (customer_id, balance, region) VALUES (source.customer_id, source.change_amount, source.region)
""")

print("Balance upsert complete")
