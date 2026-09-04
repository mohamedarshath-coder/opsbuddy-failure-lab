# Databricks notebook source
# MAGIC %md
# MAGIC # 09_gold_price_alerts
# MAGIC
# MAGIC Flags any product whose discounted price has dropped below its alert threshold, for the
# MAGIC merchandising team's daily review queue.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Price-drop alert candidates
# MAGIC
# MAGIC Every priced product should be eligible for alert evaluation — if this table comes back
# MAGIC empty, something upstream in the pricing pipeline is broken.

# COMMAND ----------

silver_pricing = spark.table("dev.opsbuddy_test.silver_pricing")

alert_candidates = silver_pricing.filter(F.col("discounted_price") < F.col("base_price") * 0.9)

candidate_count = alert_candidates.count()
assert candidate_count > 0, "No price-drop alert candidates found"

alert_candidates.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.gold_price_alerts")
print(f"Price alert candidates written: {candidate_count} rows")
display(alert_candidates)
