# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Email Domain Extractor
# MAGIC Extracts the domain portion of each customer's email address, for a marketing
# MAGIC segmentation report grouping customers by email provider (gmail.com, corporate
# MAGIC domains, etc).

# COMMAND ----------

from pyspark.sql import Row

customers = spark.createDataFrame([
    Row(customer_id="C001", email="alice@example.com"),
    Row(customer_id="C002", email="bob@corp-example.com"),
    Row(customer_id="C003", email=None),  # a real, common case: some signups never
                                           # captured an email (e.g. phone-only accounts)
    Row(customer_id="C004", email="dana@example.com"),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract domain
# MAGIC Using a plain Python UDF for full control over the split logic.

# COMMAND ----------

def extract_domain(email):
    return email.split("@")[1]

rows = customers.collect()
results = []
for row in rows:
    domain = extract_domain(row.email)  # BUG: no null check -- fails the moment a row
                                         # with email=None is processed
    results.append((row.customer_id, domain))

for r in results:
    print(r)
