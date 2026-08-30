# Databricks notebook source
# MAGIC %md
# MAGIC # Validate & Clean Transactions
# MAGIC Second stage of the pipeline. Applies data-quality rules to the raw extract and
# MAGIC produces the "clean" table that reconciliation and reporting both read from.
# MAGIC
# MAGIC The clean table enforces a hard data-quality gate: no negative amounts, on the
# MAGIC assumption that "clean" transactions are always positive charges.

# COMMAND ----------

dbutils.widgets.text("target_schema", "default")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table(f"{target_schema}.daily_txn_raw")
cleaned = raw.filter(raw.amount.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the clean layer, with a CHECK constraint
# MAGIC BUG: this constraint (`amount >= 0`) was written when this feed only ever
# MAGIC carried positive charges. Refunds were added to the upstream feed later as a
# MAGIC real business requirement, and this constraint was never revisited -- so every
# MAGIC legitimate refund now fails the write outright with a Delta invariant violation.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_schema}.daily_txn_clean")
cleaned.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_clean")
spark.sql(
    f"ALTER TABLE {target_schema}.daily_txn_clean "
    f"ADD CONSTRAINT positive_amount_only CHECK (amount >= 0)"
)

# Re-write to force constraint enforcement against the actual refund rows already
# present in this batch (constraints only reject rows written AFTER they're added,
# so this second write is what actually surfaces the violation for this run's data).
cleaned.write.mode("overwrite").saveAsTable(f"{target_schema}.daily_txn_clean")

print(f"Wrote {cleaned.count()} rows to {target_schema}.daily_txn_clean")
