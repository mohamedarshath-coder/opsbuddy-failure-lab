# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription billing demo · bronze (1 of 7)
# MAGIC Lands the five billing system objects (`bl_*_raw`, every field text) as they
# MAGIC are, plus `source_object`. Feeds the two silver jobs (92, 93). Each table is
# MAGIC named in full, so lineage tools reading this code see what it builds.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")


def land(raw_table, bronze_table, source_object):
    raw = spark.table(raw_table)
    landed = spark.table(bronze_table)
    assert landed.count() == raw.count(), f"{bronze_table} lost or gained rows"
    print(f"{bronze_table}: {landed.count()} rows ({source_object})")


# COMMAND ----------

spark.table("dev.opsbuddy_test.bl_accounts_raw").withColumn(
    "source_object", F.lit("billing.Account")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_accounts_bronze"
)
land("dev.opsbuddy_test.bl_accounts_raw", f"{target_schema}.bl_accounts_bronze", "billing.Account")

# COMMAND ----------

spark.table("dev.opsbuddy_test.bl_plans_raw").withColumn(
    "source_object", F.lit("billing.Plan")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_plans_bronze"
)
land("dev.opsbuddy_test.bl_plans_raw", f"{target_schema}.bl_plans_bronze", "billing.Plan")

# COMMAND ----------

spark.table("dev.opsbuddy_test.bl_subscriptions_raw").withColumn(
    "source_object", F.lit("billing.Subscription")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_subscriptions_bronze"
)
land(
    "dev.opsbuddy_test.bl_subscriptions_raw",
    f"{target_schema}.bl_subscriptions_bronze",
    "billing.Subscription",
)

# COMMAND ----------

spark.table("dev.opsbuddy_test.bl_invoices_raw").withColumn(
    "source_object", F.lit("billing.Invoice")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_invoices_bronze"
)
land("dev.opsbuddy_test.bl_invoices_raw", f"{target_schema}.bl_invoices_bronze", "billing.Invoice")

# COMMAND ----------

spark.table("dev.opsbuddy_test.bl_payments_raw").withColumn(
    "source_object", F.lit("billing.Payment")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.bl_payments_bronze"
)
land("dev.opsbuddy_test.bl_payments_raw", f"{target_schema}.bl_payments_bronze", "billing.Payment")
