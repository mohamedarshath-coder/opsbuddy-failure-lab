# Databricks notebook source
# MAGIC %md
# MAGIC # Validate & Clean Transactions
# MAGIC Second stage of the pipeline. Applies data-quality rules to the raw extract and
# MAGIC produces the "clean" table that reconciliation and reporting both read from.
# MAGIC
# MAGIC Negative amounts are expected here -- they represent legitimate refunds, which
# MAGIC the upstream feed has carried as a normal transaction type since that became a
# MAGIC real business requirement. "Clean" only means non-null amounts.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table(f"{target_schema}.daily_txn_raw")
cleaned = raw.filter(raw.amount.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the clean layer
# MAGIC No positive-amount constraint here (fixed SCRUM-80): a prior `amount >= 0` CHECK
# MAGIC constraint predated refunds being added to the upstream feed and rejected every
# MAGIC legitimate refund row with a Delta invariant violation. Reconciliation (stage 3)
# MAGIC sums amounts per account and relies on refunds netting against charges, so this
# MAGIC stage must pass negative amounts through rather than filtering or rejecting them.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_schema}.daily_txn_clean")
cleaned.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_clean")

print(f"Wrote {cleaned.count()} rows to {target_schema}.daily_txn_clean")
