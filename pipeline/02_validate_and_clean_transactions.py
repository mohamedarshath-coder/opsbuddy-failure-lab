# Databricks notebook source
# MAGIC %md
# MAGIC # Validate & Clean Transactions
# MAGIC Second stage of the pipeline. Applies data-quality rules to the raw extract and
# MAGIC produces the "clean" table that reconciliation and reporting both read from.
# MAGIC
# MAGIC Negative amounts are expected here for `txn_type = 'refund'` rows -- a normal,
# MAGIC documented transaction type. Any OTHER transaction type must still never be
# MAGIC negative -- that's a real data-quality problem worth catching, not a false alarm.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table(f"{target_schema}.daily_txn_extract")
cleaned = raw.filter(raw.amount.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the clean layer, with a refined data-quality constraint
# MAGIC SCRUM-80 removed the original `amount >= 0` CHECK entirely, since it rejected
# MAGIC every legitimate refund. That fix was too broad -- it also removed all
# MAGIC validation for non-refund rows, so a genuinely bad row (e.g. a charge posted
# MAGIC with a nonsensical negative amount from an unrelated upstream bug) would now
# MAGIC pass through silently. The constraint should exempt refunds specifically, not
# MAGIC be deleted outright.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_schema}.daily_txn_clean")
cleaned.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_clean")
spark.sql(
    f"ALTER TABLE {target_schema}.daily_txn_clean "
    f"ADD CONSTRAINT non_refund_amount_positive "
    f"CHECK (txn_type = 'refund' OR amount >= 0)"
)

print(f"Wrote {cleaned.count()} rows to {target_schema}.daily_txn_clean")
