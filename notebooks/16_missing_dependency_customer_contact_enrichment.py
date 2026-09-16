# Databricks notebook source
# MAGIC %md
# MAGIC # customer_contact_enrichment — Dependency / Library Import Error
# MAGIC
# MAGIC A genuinely different failure category from every other scenario in this repo: nothing
# MAGIC wrong with the business logic itself, just a missing library install on this cluster.
# MAGIC `ModuleNotFoundError`, not `AnalysisException`/`AssertionError` — a useful contrast for
# MAGIC opsbuddy-fix's classification step.

# COMMAND ----------

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md ## Bronze — synthetic raw customer contact numbers, mixed international formats

# COMMAND ----------

contact_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("raw_phone", StringType(), False),
    StructField("country_code", StringType(), False),
])

bronze_contacts = [
    (201, "9876543210", "IN"),
    (202, "07911123456", "GB"),
    (203, "2025550123", "US"),
    (204, "0170123456", "DE"),
    (205, "0412345678", "AU"),
]

bronze_df = spark.createDataFrame(bronze_contacts, schema=contact_schema)
bronze_df.write.mode("overwrite").saveAsTable(f"{target_schema}.bronze_customer_contacts")
print(f"Bronze contacts written: {bronze_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Silver — format each number into E.164 using a real phone-parsing library
# MAGIC
# MAGIC **THE BUG IS HERE.** This cell imports `phonenumbers` directly, but this cluster has never
# MAGIC had it installed — there is no `%pip install phonenumbers` cell anywhere above this one.
# MAGIC Expect:
# MAGIC ```
# MAGIC ModuleNotFoundError: No module named 'phonenumbers'
# MAGIC ```
# MAGIC **The correct minimal fix**: add a `%pip install phonenumbers` cell immediately before this
# MAGIC one, followed by `dbutils.library.restartPython()` — the standard Databricks pattern for a
# MAGIC notebook-scoped library that isn't already on the cluster. Don't work around this by
# MAGIC hand-rolling phone-formatting logic instead — the point of this scenario is the missing
# MAGIC install step, not the formatting logic itself.

# COMMAND ----------

import phonenumbers


def format_e164(raw_phone: str, country_code: str) -> str:
    parsed = phonenumbers.parse(raw_phone, country_code)
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)


format_udf = F.udf(format_e164, StringType())

silver_df = (
    spark.table(f"{target_schema}.bronze_customer_contacts")
    .withColumn("formatted_phone", format_udf(F.col("raw_phone"), F.col("country_code")))
)

silver_df.write.mode("overwrite").saveAsTable(f"{target_schema}.silver_customer_contacts")
print(f"Silver contacts written: {silver_df.count()} rows")
display(silver_df)

# COMMAND ----------

# MAGIC %md ## Validate — every contact must have a real, non-null E.164-formatted number

# COMMAND ----------

result = spark.table(f"{target_schema}.silver_customer_contacts")
null_count = result.filter(F.col("formatted_phone").isNull()).count()
assert null_count == 0, f"{null_count} customer contact(s) failed to format to E.164"

total = result.count()
assert total == 5, f"Expected 5 formatted contacts, got {total}"

print(f"customer_contact_enrichment completed successfully -- {total} contacts formatted, 0 failures.")
