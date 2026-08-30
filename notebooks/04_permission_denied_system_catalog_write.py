# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Trail Snapshot Writer
# MAGIC Writes a point-in-time snapshot of table access patterns for the compliance
# MAGIC team's quarterly audit review.

# COMMAND ----------

audit_snapshot = spark.createDataFrame(
    [("customer_orders", "SELECT", "analytics_svc"), ("customer_pii", "SELECT", "ml_svc")],
    ["table_name", "access_type", "principal"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the snapshot
# MAGIC Writing directly into the platform's own `system` catalog so it's queryable
# MAGIC alongside Unity Catalog's built-in audit views.

# COMMAND ----------

# BUG: `system` is Databricks' own reserved, read-only catalog (built-in audit logs,
# billing, lineage tables) -- no principal can write to it, regardless of how broad
# their other grants are. A realistic mistake: assuming "system" is just a normal
# catalog name available to reuse, rather than a reserved one.
audit_snapshot.write.mode("append").saveAsTable("system.default.audit_trail_snapshot")

print("Audit snapshot written")
