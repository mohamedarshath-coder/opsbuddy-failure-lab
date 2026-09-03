# Databricks notebook source
# MAGIC %md
# MAGIC # 01_silver_customers — upstream task (this is where the REAL bug lives)
# MAGIC
# MAGIC This task runs successfully and produces no error of its own — that's the whole point of
# MAGIC this demo. The bug here is silent: a leftover debug filter accidentally drops every
# MAGIC non-US customer. The actual FAILURE surfaces two tasks downstream, in
# MAGIC `03_global_reporting`, which is why this scenario specifically exercises
# MAGIC `get_table_lineage`'s `upstream_producers` lookup — the failing task's own code has no
# MAGIC bug in it at all.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md ## Bronze — synthetic customer data, deliberately weighted with ZERO US customers
# MAGIC
# MAGIC This guarantees the downstream filter bug produces a literal zero-row result, not just a
# MAGIC reduced one — matching the scenario's exact `AssertionError: Gold row count is zero`.

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("country", StringType(), False),
])

bronze_customers = [
    (101, "Aisha Khan", "IN"),
    (102, "Liam O'Connor", "UK"),
    (103, "Sofia Rossi", "IT"),
    (104, "Mateus Silva", "BR"),
    (105, "Yuki Tanaka", "JP"),
    (106, "Emma Dubois", "FR"),
    (107, "Lukas Weber", "DE"),
    (108, "Fatima Al-Sayed", "AE"),
]  # -- note: NO US customers in this batch at all

bronze_df = spark.createDataFrame(bronze_customers, schema=customer_schema)
bronze_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_customers")
print(f"Bronze customers written: {bronze_df.count()} rows (0 of them US)")

# COMMAND ----------

# MAGIC %md ## Silver — clean customer data
# MAGIC
# MAGIC **THE BUG IS HERE.** This `WHERE country = 'US'` filter was added months ago during a
# MAGIC one-off US-market debugging session and was never removed — it has nothing to do with
# MAGIC this notebook's actual purpose (which is just deduplication and name normalization).
# MAGIC Nobody caught it because this task has never itself thrown an error: it runs cleanly and
# MAGIC writes a (silently near-empty or fully empty) table every time.
# MAGIC
# MAGIC **The correct minimal fix**: delete this filter line entirely. It doesn't belong here —
# MAGIC this notebook has no business being US-only, and there's no configuration flag or comment
# MAGIC anywhere suggesting this was ever intentional for the notebook's actual purpose.

# COMMAND ----------

silver_df = (
    spark.table("dev.opsbuddy_test.bronze_customers")
    .filter(F.col("country") == "US")          # <-- THE BUG: this line should not exist
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", F.trim(F.col("customer_name")))
)

silver_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.silver_customers")
print(f"Silver customers written: {silver_df.count()} rows")
display(silver_df)

# COMMAND ----------

# MAGIC %md ## This task ends here, cleanly, with no error
# MAGIC
# MAGIC This is the important part of the demo: **this notebook's own execution reports SUCCESS.**
# MAGIC Whatever's wrong here is invisible until a downstream consumer of `silver_customers`
# MAGIC actually tries to use it — which is `03_global_reporting`, run as a separate task in the
# MAGIC same job, potentially minutes later.
print("01_silver_customers completed successfully -- 0 rows written, but no error raised here.")
