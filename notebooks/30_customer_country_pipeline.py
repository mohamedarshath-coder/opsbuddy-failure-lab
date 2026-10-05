# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Country Pipeline (Nightly)
# MAGIC Builds three governed tables in `target_schema`, parent first:
# MAGIC 1. `cc_customers` (parent) -- the customer master from the CRM. `country` is typed by
# MAGIC    sales reps, so the same country arrives in many spellings.
# MAGIC 2. `cc_orders` (child) -- orders from the order system. Each order carries the country
# MAGIC    the order system recorded, which is spelled independently of the CRM.
# MAGIC 3. `cc_revenue_by_country` (grandchild) -- revenue and order count per country, built
# MAGIC    from `cc_orders`, for the regional sales report.
# MAGIC
# MAGIC Today `country` is passed through as received, so the report splits one country across
# MAGIC several rows (USA alone appears under many spellings). DataSakshi checks each table after
# MAGIC every release (specs/cc_*.yaml).

# COMMAND ----------

from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md ## Spellings seen in the source systems
# MAGIC One list per real country: every way the CRM and the order system have written it.

# COMMAND ----------

SPELLINGS = [
    ["USA", "U.S.A.", "United States", "united states of america", " us ", "America"],
    ["Canada", "CA", "canada "],
    ["Mexico", "MEX", "mexico"],
    ["Brazil", "Brasil", "BR"],
    ["Argentina", "argentina", "AR"],
    ["UK", "United Kingdom", "Great Britain", "England"],
    ["Ireland", "Republic of Ireland", "IE"],
    ["France", "FR", "france"],
    ["Germany", "Deutschland", "DE"],
    ["Spain", "España", "ES"],
    ["Italy", "Italia", "IT"],
    ["Netherlands", "Holland", "The Netherlands"],
    ["Switzerland", "Schweiz", "CH"],
    ["Sweden", "sweden", "SE"],
    ["India", "IND", "india", "Bharat"],
    ["China", "PRC", "People's Republic of China"],
    ["Japan", "JP", "japan"],
    ["South Korea", "Korea, Republic of", "Republic of Korea"],
    ["Singapore", "SG", "singapore"],
    ["UAE", "United Arab Emirates", "U.A.E."],
    ["Saudi Arabia", "KSA", "saudi arabia"],
    ["South Africa", "RSA", "south africa"],
    ["Nigeria", "nigeria", "NG"],
    ["Egypt", "egypt", "EG"],
    ["Australia", "AU", "Aus"],
    ["New Zealand", "NZ", "new zealand"],
]

# COMMAND ----------

# MAGIC %md ## 1. Customers (parent)
# MAGIC Two customers per country; the CRM spelling rotates through that country's list.

# COMMAND ----------

customers = []
for c, spellings in enumerate(SPELLINGS):
    for k in range(2):
        n = c * 2 + k + 1
        customers.append(
            (
                f"CU{n:03d}",
                f"Customer {n:03d}",
                spellings[(c + k) % len(spellings)],
                date(2025, 1, 1) + timedelta(days=(n * 11) % 365),
            )
        )
cc_customers = spark.createDataFrame(
    customers, ["customer_id", "customer_name", "country", "signed_up_on"]
)
cc_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc_customers"
)

# COMMAND ----------

# MAGIC %md ## 2. Orders (child of customers)
# MAGIC One to three orders per customer. The order system's country spelling rotates
# MAGIC independently of the CRM's, so a customer's orders may spell its country differently.

# COMMAND ----------

orders = []
order_no = 0
for c, spellings in enumerate(SPELLINGS):
    for k in range(2):
        n = c * 2 + k + 1
        for j in range(1 + (n % 3)):
            order_no += 1
            amount = Decimal(25 + (n * 37 + j * 53) % 400) + Decimal("0.99")
            orders.append(
                (
                    f"OR{order_no:04d}",
                    f"CU{n:03d}",
                    date(2026, 9, 1) + timedelta(days=(n + j * 7) % 30),
                    str(amount),
                    spellings[(c + k + j + 1) % len(spellings)],
                )
            )
cc_orders = spark.createDataFrame(
    orders, ["order_id", "customer_id", "ordered_on", "amount", "country"]
).withColumn("amount", F.col("amount").cast("decimal(12,2)"))
cc_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc_orders"
)

# COMMAND ----------

# MAGIC %md ## 3. Revenue by country (child of orders)

# COMMAND ----------

cc_revenue_by_country = (
    spark.table(f"{target_schema}.cc_orders")
    .groupBy("country")
    .agg(
        F.count("*").alias("orders_count"),
        F.sum("amount").cast("decimal(14,2)").alias("revenue"),
    )
)
cc_revenue_by_country.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.cc_revenue_by_country"
)

# COMMAND ----------

# MAGIC %md ## Self-checks before the job reports success

# COMMAND ----------

orders_df = spark.table(f"{target_schema}.cc_orders")
customers_df = spark.table(f"{target_schema}.cc_customers")
revenue_df = spark.table(f"{target_schema}.cc_revenue_by_country")

orphans = orders_df.join(customers_df, on="customer_id", how="left_anti").count()
assert orphans == 0, f"{orphans} order(s) have no customer in cc_customers"

orders_total = orders_df.agg(F.sum("amount")).collect()[0][0]
revenue_total = revenue_df.agg(F.sum("revenue")).collect()[0][0]
assert revenue_total == orders_total, (
    f"revenue by country totals {revenue_total} but orders total {orders_total}"
)
assert revenue_df.agg(F.sum("orders_count")).collect()[0][0] == orders_df.count()

print(
    f"Customer country pipeline complete: {customers_df.count()} customers, "
    f"{orders_df.count()} orders, revenue {orders_total} across {revenue_df.count()} country rows"
)
