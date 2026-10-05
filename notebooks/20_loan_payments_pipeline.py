# Databricks notebook source
# MAGIC %md
# MAGIC # Loan Repayments Pipeline (Nightly)
# MAGIC Builds four governed tables in `target_schema`, in order:
# MAGIC 1. `loan_accounts` -- the loan book (one row per account).
# MAGIC 2. `loan_payments_raw` -- payment events exactly as the payments provider sent them,
# MAGIC    including a retried delivery that repeats a payment.
# MAGIC 3. `loan_payments` -- one row per payment: the retry is removed, amounts are typed.
# MAGIC 4. `loan_daily_summary` -- settled amount and payment count per day and region, for the
# MAGIC    collections team's daily report. Reversed payments are not counted as collected.
# MAGIC
# MAGIC DataSakshi checks each table after every release (specs/loan_*.yaml).

# COMMAND ----------

from datetime import date
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

dbutils.widgets.text("target_schema", "dev.opsbuddy_test")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md ## 1. Loan accounts

# COMMAND ----------

loan_accounts = spark.createDataFrame([
    Row(account_id="LN1001", customer_id="C1001", region="North", product="personal", principal=Decimal("5000.00"), opened_on=date(2026, 1, 15)),
    Row(account_id="LN1002", customer_id="C1002", region="South", product="auto", principal=Decimal("18000.00"), opened_on=date(2025, 11, 2)),
    Row(account_id="LN1003", customer_id="C1003", region="West", product="home", principal=Decimal("240000.00"), opened_on=date(2024, 6, 30)),
    Row(account_id="LN1004", customer_id="C1004", region="East", product="personal", principal=Decimal("3500.00"), opened_on=date(2026, 3, 9)),
    Row(account_id="LN1005", customer_id="C1005", region="North", product="auto", principal=Decimal("22000.00"), opened_on=date(2025, 8, 21)),
    Row(account_id="LN1006", customer_id="C1006", region="South", product="home", principal=Decimal("310000.00"), opened_on=date(2023, 12, 1)),
    Row(account_id="LN1007", customer_id="C1007", region="West", product="personal", principal=Decimal("7500.00"), opened_on=date(2026, 2, 27)),
    Row(account_id="LN1008", customer_id="C1008", region="East", product="auto", principal=Decimal("15500.00"), opened_on=date(2025, 5, 14)),
])
loan_accounts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.loan_accounts"
)

# COMMAND ----------

# MAGIC %md ## 2. Raw payment events (as received)
# MAGIC PAY0007 arrives twice: the provider retried after a timeout. PAY0013 was reversed
# MAGIC (a bounced direct debit), so it is not collected money.

# COMMAND ----------

raw_events = [
    ("PAY0001", "LN1001", date(2026, 10, 1), "250.00", "online", "settled"),
    ("PAY0002", "LN1002", date(2026, 10, 1), "540.00", "autopay", "settled"),
    ("PAY0003", "LN1003", date(2026, 10, 1), "1850.00", "autopay", "settled"),
    ("PAY0004", "LN1004", date(2026, 10, 1), "120.00", "branch", "settled"),
    ("PAY0005", "LN1005", date(2026, 10, 1), "610.00", "online", "settled"),
    ("PAY0006", "LN1006", date(2026, 10, 2), "2300.00", "autopay", "settled"),
    ("PAY0007", "LN1007", date(2026, 10, 2), "300.00", "online", "settled"),
    ("PAY0007", "LN1007", date(2026, 10, 2), "300.00", "online", "settled"),  # retried delivery
    ("PAY0008", "LN1008", date(2026, 10, 2), "480.00", "autopay", "settled"),
    ("PAY0009", "LN1001", date(2026, 10, 2), "250.00", "online", "settled"),
    ("PAY0010", "LN1002", date(2026, 10, 3), "540.00", "autopay", "settled"),
    ("PAY0011", "LN1003", date(2026, 10, 3), "1850.00", "autopay", "settled"),
    ("PAY0012", "LN1004", date(2026, 10, 3), "120.00", "branch", "settled"),
    ("PAY0013", "LN1005", date(2026, 10, 3), "610.00", "autopay", "reversed"),  # bounced
    ("PAY0014", "LN1006", date(2026, 10, 3), "2300.00", "autopay", "settled"),
    ("PAY0015", "LN1007", date(2026, 10, 4), "300.00", "online", "settled"),
    ("PAY0016", "LN1008", date(2026, 10, 4), "480.00", "branch", "settled"),
    ("PAY0017", "LN1001", date(2026, 10, 4), "250.00", "online", "settled"),
    ("PAY0018", "LN1005", date(2026, 10, 4), "610.00", "online", "settled"),
    ("PAY0019", "LN1002", date(2026, 10, 4), "540.00", "autopay", "settled"),
    ("PAY0020", "LN1003", date(2026, 10, 4), "1850.00", "autopay", "settled"),
    ("PAY0021", "LN1004", date(2026, 10, 4), "120.00", "branch", "settled"),
]
loan_payments_raw = spark.createDataFrame(
    raw_events, ["payment_id", "account_id", "paid_on", "amount", "channel", "status"]
).withColumn("received_at", F.current_timestamp())
loan_payments_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.loan_payments_raw"
)

# COMMAND ----------

# MAGIC %md ## 3. Clean payments: one row per payment

# COMMAND ----------

loan_payments = (
    spark.table(f"{target_schema}.loan_payments_raw")
    .dropDuplicates(["payment_id"])
    .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    .withColumn("loaded_at", F.current_timestamp())
    .drop("received_at")
)
loan_payments.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.loan_payments"
)

# COMMAND ----------

# MAGIC %md ## 4. Daily summary for collections (settled payments only)
# MAGIC Collections also wants the loan product on each line, from the product history kept by
# MAGIC the loan servicing system (an account can be moved to another product, e.g. refinanced).

# COMMAND ----------

loan_account_products = spark.createDataFrame([
    Row(account_id="LN1001", product="personal", is_current=True),
    Row(account_id="LN1002", product="auto", is_current=True),
    Row(account_id="LN1003", product="home", is_current=False),  # refinanced in August
    Row(account_id="LN1003", product="home_refinance", is_current=True),
    Row(account_id="LN1004", product="personal", is_current=True),
    Row(account_id="LN1005", product="auto", is_current=True),
    Row(account_id="LN1006", product="home", is_current=True),
    Row(account_id="LN1007", product="personal", is_current=True),
    Row(account_id="LN1008", product="auto", is_current=True),
])

payments = spark.table(f"{target_schema}.loan_payments")
accounts = spark.table(f"{target_schema}.loan_accounts")
loan_daily_summary = (
    payments.filter(F.col("status") == "settled")
    .join(accounts.select("account_id", "region"), on="account_id", how="inner")
    .join(loan_account_products.select("account_id", "product"), on="account_id", how="inner")
    .groupBy("paid_on", "region", "product")
    .agg(
        F.count("*").alias("payments_count"),
        F.sum("amount").cast("decimal(14,2)").alias("settled_amount"),
    )
)
loan_daily_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{target_schema}.loan_daily_summary"
)

# COMMAND ----------

# MAGIC %md ## Self-checks before the job reports success

# COMMAND ----------

raw_ids = spark.table(f"{target_schema}.loan_payments_raw").select("payment_id").distinct().count()
clean_rows = spark.table(f"{target_schema}.loan_payments").count()
assert clean_rows == raw_ids, f"loan_payments has {clean_rows} rows for {raw_ids} distinct payments"

settled_total = (
    payments.filter(F.col("status") == "settled").agg(F.sum("amount")).collect()[0][0]
)
summary_total = spark.table(f"{target_schema}.loan_daily_summary").agg(
    F.sum("settled_amount")
).collect()[0][0]
assert summary_total == settled_total, (
    f"daily summary total {summary_total} does not match settled payments {settled_total}"
)
print(
    f"Loan payments pipeline complete: {clean_rows} payments, settled total {settled_total}, "
    f"{spark.table(f'{target_schema}.loan_daily_summary').count()} summary rows"
)
