# Databricks notebook source
# MAGIC %md
# MAGIC # Accounts medallion · bronze (1 of 3)
# MAGIC Lands the Salesforce accounts object (`sf_accounts_raw`, every field text) as
# MAGIC `accounts_bronze`, unchanged, plus where it came from. Feeds silver (notebook 72).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

raw = spark.table("dev.opsbuddy_test.sf_accounts_raw")
bronze = raw.withColumn("source_object", F.lit("salesforce.Account"))
bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.accounts_bronze"
)
written = spark.table(f"{target_schema}.accounts_bronze")
assert written.count() == raw.count(), "accounts_bronze lost or gained rows"
print(f"accounts_bronze: {written.count()} rows")
