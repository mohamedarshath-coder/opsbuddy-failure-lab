# Databricks notebook source
# MAGIC %md
# MAGIC # opsbuddy-fix test pipeline — medallion architecture with TWO intentional bugs
# MAGIC
# MAGIC This notebook exists purely to generate a real, realistic failed Databricks job run for
# MAGIC testing `opsbuddy-fix` end-to-end. It has two separate, deliberate bugs:
# MAGIC
# MAGIC 1. **Schema Mismatch** (surfaces first) — the Silver layer renames a column, but the Gold
# MAGIC    layer still references the old name. Classic `AnalysisException`, `CODE_FIX_POSSIBLE: true`.
# MAGIC 2. **Null Pointer / NoneType** (surfaces only after bug #1 is fixed and the job re-runs) —
# MAGIC    a subset of synthetic rows have a null `order_amount`, and the Gold layer's discount
# MAGIC    calculation doesn't guard against it.
# MAGIC
# MAGIC This tests the FULL opsbuddy-fix loop: first fix → PR → merge → real re-run (Gate 8.5) →
# MAGIC re-run hits the SECOND bug → second fix cycle → real re-run → genuinely passes.
# MAGIC
# MAGIC **Setup requirement**: this notebook must be git-linked (via Databricks Repos, or a job
# MAGIC with git-source configured) for `get_repo_mapping` to resolve a real repo — a bare
# MAGIC workspace notebook with no git linkage will cause opsbuddy-fix to halt at Phase 4 before
# MAGIC ever reaching remediation. See the deployment notes at the bottom of this file.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# COMMAND ----------

# MAGIC %md ## Bronze layer — synthetic raw order data (no external dependency, always succeeds)

# COMMAND ----------

bronze_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_amount", DoubleType(), True),  # nullable on purpose -- feeds bug #2
    StructField("discount_pct", DoubleType(), False),
    StructField("region", StringType(), False),
])

bronze_data = [
    (1, "CUST-001", 250.00, 0.10, "west"),
    (2, "CUST-002", 130.50, 0.05, "east"),
    (3, "CUST-003", None, 0.15, "west"),      # null order_amount -- triggers bug #2 later
    (4, "CUST-004", 89.99, 0.00, "north"),
    (5, "CUST-005", 410.25, 0.20, "south"),
    (6, "CUST-001", 75.00, 0.10, "west"),
    (7, "CUST-006", None, 0.05, "east"),      # null order_amount -- triggers bug #2 later
    (8, "CUST-003", 199.99, 0.15, "west"),
]

bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)
bronze_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_orders")

print(f"Bronze layer written: {bronze_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Silver layer — clean and rename columns
# MAGIC
# MAGIC **This is where BUG #1 originates**: `customer_id` is renamed to `cust_id` here, as part
# MAGIC of a (fictional) upstream schema standardization effort — but the Gold layer below was
# MAGIC never updated to match.

# COMMAND ----------

silver_df = (
    spark.table("dev.opsbuddy_test.bronze_orders")
    .withColumnRenamed("customer_id", "cust_id")   # <-- the rename that breaks Gold below
    .withColumn("order_amount", F.col("order_amount"))  # left nullable on purpose
    .filter(F.col("order_id").isNotNull())
)

silver_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.silver_orders")

print(f"Silver layer written: {silver_df.count()} rows")
display(silver_df)

# COMMAND ----------

# MAGIC %md ## Gold layer — aggregate by customer with discount applied
# MAGIC
# MAGIC **BUG #1 fires here**: this still references `customer_id`, which no longer exists after
# MAGIC the Silver layer's rename above. Expect:
# MAGIC ```
# MAGIC AnalysisException: cannot resolve 'customer_id' given input columns: [cust_id, order_amount, ...]
# MAGIC ```
# MAGIC
# MAGIC **The correct minimal fix**: change `"customer_id"` to `"cust_id"` in the groupBy below —
# MAGIC nothing else needs to change. This is intentionally a one-line, unambiguous fix so Mode A
# MAGIC review's "targeted, minimal, no scope creep" checks have a clean case to validate against.

# COMMAND ----------

gold_df = (
    spark.table("dev.opsbuddy_test.silver_orders")
    .groupBy("cust_id")  # FIXED: was "customer_id", renamed to "cust_id" in Silver layer
    .agg(
        F.sum("order_amount").alias("total_order_amount"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.count("order_id").alias("order_count"),
    )
)

# COMMAND ----------

# MAGIC %md ## Gold layer, part 2 — discount-adjusted totals
# MAGIC
# MAGIC **BUG #2 lives here, but won't surface until BUG #1 is fixed and this cell actually runs.**
# MAGIC `order_amount` is nullable (see Bronze layer — two rows have `None`), and this calculation
# MAGIC does `row.order_amount * discount_pct` with no null guard, which raises:
# MAGIC ```
# MAGIC TypeError: unsupported operand type(s) for *: 'NoneType' and 'float'
# MAGIC ```
# MAGIC (or an equivalent NoneType error, depending on exactly how the null propagates through the
# MAGIC UDF/collect step below — the point is it's a real, unguarded null, not a contrived one).
# MAGIC
# MAGIC **The correct minimal fix**: guard against `None` before the multiplication — e.g.
# MAGIC `coalesce(order_amount, 0.0)` in the Spark expression, or an explicit null check if this is
# MAGIC rewritten as row-wise Python. Do NOT fix this by dropping null rows silently or wrapping in
# MAGIC a bare `except:` — that's exactly the kind of error-suppression anti-pattern the Mode A
# MAGIC review checklist is designed to catch.

# COMMAND ----------

# Collect to driver for a final adjusted-total calculation (intentionally naive, for the test)
rows = spark.table("dev.opsbuddy_test.bronze_orders").collect()

adjusted_totals = []
for row in rows:
    # BUG #2: no null check on row.order_amount before the multiplication
    adjusted = row.order_amount * (1 - row.discount_pct)
    adjusted_totals.append((row.order_id, row.customer_id, adjusted))

adjusted_df = spark.createDataFrame(
    adjusted_totals, schema=["order_id", "customer_id", "adjusted_amount"]
)
adjusted_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_adjusted_orders")

print(f"Gold adjusted-orders layer written: {adjusted_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Final gold aggregate (only reached once both bugs are fixed)

# COMMAND ----------

gold_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_customer_summary")
print("Pipeline completed successfully -- both bugs fixed if you're reading this.")
display(gold_df)

# COMMAND ----------

# MAGIC %md ## Setup notes for wiring this into a real, testable Databricks Job
# MAGIC
# MAGIC 1. **Put this file in your actual git repo** (the same one `GITHUB_TOKEN` already has
# MAGIC    access to) — e.g. `notebooks/opsbuddy_test_pipeline.py` — and push it to a branch.
# MAGIC 2. **Git-link it in Databricks**, one of two ways:
# MAGIC    - **Databricks Repos** (recommended, matches `get_repo_mapping`'s primary resolution
# MAGIC      path): Repos → Add Repo → your repo URL → clone it. The notebook will then live under
# MAGIC      `/Repos/<you>/<repo-name>/notebooks/opsbuddy_test_pipeline`.
# MAGIC    - **Job-level Git source**: when creating the Job (next step), under "Source" choose
# MAGIC      "Git provider" instead of "Workspace", and point it at your repo + branch + this
# MAGIC      file's path.
# MAGIC 3. **Create a Databricks Job**: Workflows → Create Job → add a Notebook task pointing at
# MAGIC    whichever git-linked path you set up in step 2. Name the job something identifiable,
# MAGIC    e.g. `opsbuddy-fix-test-pipeline`.
# MAGIC 4. **Run it once manually.** It will fail on BUG #1 (`AnalysisException` on `customer_id`).
# MAGIC    Note the **job_id** and **run_id** from the run's URL or the Jobs UI.
# MAGIC 5. **In a fresh Genie Code chat**, say: `job <job_id> failed, fix it` — this exercises the
# MAGIC    full first fix cycle.
# MAGIC 6. **Once that PR is merged and Gate 8.5 triggers a real re-run**, it should now hit BUG
# MAGIC    #2 instead of succeeding outright — a second, independent incident. Watch whether
# MAGIC    `opsbuddy-fix` correctly treats this as `VERIFICATION_FAILED` (loop back to Phase 5
# MAGIC    once) or, if it's invoked completely fresh against the new failed run, whether
# MAGIC    `databricks-debug` correctly classifies it as a *different* category (Null Pointer /
# MAGIC    NoneType) than bug #1 (Schema Mismatch) rather than assuming it's the same issue.
