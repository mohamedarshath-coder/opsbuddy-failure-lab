"""Subscription billing demo: silver cleaning, gold aggregates and the finance
mart (notebooks 92-97).

A SaaS company's billing system sends every field as text. Accounts carry a
country as " us" or "GB ", amounts can carry thousands separators ("1,250.00"),
and its exports can repeat a row. Plans are priced per seat per month (or per
year) in USD; invoices and payments are in the account's local currency and are
converted to USD with the fixed rates below. Spark expressions only, so they
run on the executors.

Lineage, as built:
  subscriptions silver <- subscriptions, plans and accounts bronze
  invoices silver      <- invoices, payments and accounts bronze
  MRR and churn gold   <- subscriptions silver
  AR aging gold        <- invoices silver
  finance KPI mart     <- MRR gold and AR aging gold
Silver keeps only the fields the reports use today: invoices silver drops the
due date (bronze has it), and credit notes stay their own rows."""

from datetime import date

# Every report is "as of" the last day of the data, so a rerun gives the same numbers.
AS_OF = date(2026, 9, 30)
CURRENCY = {"US": "USD", "DE": "EUR", "GB": "GBP", "IN": "INR", "AU": "AUD"}
FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "INR": 0.012, "AUD": 0.66}
TIERS = ["starter", "growth", "business", "enterprise"]
SUBSCRIPTION_STATUSES = ["active", "cancelled"]
INVOICE_STATUSES = ["paid", "partial", "open", "credit"]

SUBSCRIPTION_COLUMNS = [
    "subscription_id", "account_id", "plan_id", "plan_tier", "billing_period",
    "start_date", "end_date", "seats", "discount_pct", "status",
    "account_country", "account_industry", "mrr_usd",
]
INVOICE_COLUMNS = [
    "invoice_id", "account_id", "subscription_id", "invoice_type", "credit_for",
    "issue_date", "currency", "amount_usd", "paid_usd", "last_paid_date", "status",
    "account_country",
]
MRR_COLUMNS = ["month", "account_country", "plan_tier", "active_subscriptions", "mrr_usd"]
CHURN_COLUMNS = [
    "cohort_month", "subscriptions_started", "churned_within_3m", "churn_rate_3m_pct",
]
AGING_COLUMNS = [
    "account_id", "account_country", "open_invoices", "open_usd",
    "bucket_0_30_usd", "bucket_31_60_usd", "bucket_61_90_usd", "bucket_90_plus_usd",
]
KPI_COLUMNS = [
    "account_country", "mrr_usd", "arr_usd", "open_ar_usd", "overdue_90_pct", "dso_days",
]


def _money(col):
    """A text amount ("1,250.00", " 99.5 ") as decimal(14,2)."""
    from pyspark.sql import functions as F

    return F.regexp_replace(F.trim(col), ",", "").cast("decimal(14,2)")


def _fx(currency_col):
    """The USD rate for a currency column (null for an unknown currency)."""
    from itertools import chain

    from pyspark.sql import functions as F

    rates = F.create_map(*[F.lit(x) for x in chain(*FX_TO_USD.items())])
    return rates[currency_col]


def _first(df, key, *order):
    """One row per `key` (trimmed): a repeated export keeps its first copy."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(F.trim(key)).orderBy(*order)
    return df.withColumn("_n", F.row_number().over(w)).filter(F.col("_n") == 1).drop("_n")


def clean_accounts(df):
    """One row per account_id, country as an upper-case code, industry lower case."""
    from pyspark.sql import functions as F

    return _first(df, "account_id", "account_name").select(
        F.trim("account_id").alias("account_id"),
        F.upper(F.trim("country")).alias("country"),
        F.lower(F.trim("industry")).alias("industry"),
    )


def subscriptions_silver(subscriptions, plans, accounts):
    """One row per subscription, typed, with its plan's tier and billing period,
    the account's country and industry, and its monthly recurring revenue in USD
    (the plan's monthly price per seat x seats, less the discount; an annual
    plan's price is spread over 12 months)."""
    from pyspark.sql import functions as F

    subs = _first(subscriptions, "subscription_id", "start_date").select(
        F.trim("subscription_id").alias("subscription_id"),
        F.trim("account_id").alias("account_id"),
        F.trim("plan_id").alias("plan_id"),
        F.to_date(F.trim("start_date")).alias("start_date"),
        F.to_date(F.nullif(F.trim("end_date"), F.lit(""))).alias("end_date"),
        F.trim("seats").cast("int").alias("seats"),
        F.trim("discount_pct").cast("int").alias("discount_pct"),
        F.lower(F.trim("status")).alias("status"),
    )
    plan = _first(plans, "plan_id", "plan_name").select(
        F.trim("plan_id").alias("plan_id"),
        F.lower(F.trim("tier")).alias("plan_tier"),
        F.lower(F.trim("billing_period")).alias("billing_period"),
        _money("list_price").alias("_price"),
    )
    people = clean_accounts(accounts).select(
        "account_id",
        F.col("country").alias("account_country"),
        F.col("industry").alias("account_industry"),
    )
    monthly = F.when(F.col("billing_period") == "annual", F.col("_price") / 12).otherwise(
        F.col("_price")
    )
    mrr = monthly * F.col("seats") * (1 - F.col("discount_pct") / 100)
    return (
        subs.join(plan, "plan_id", "left")
        .join(people, "account_id", "left")
        .withColumn("mrr_usd", F.round(mrr.cast("double"), 2))
        .select(*SUBSCRIPTION_COLUMNS)
    )


def invoices_silver(invoices, payments, accounts):
    """One row per invoice or credit note, amounts in USD, with what was paid
    against it and its status: paid, partial, open (nothing paid yet) or credit
    (a credit note). Credit notes keep their own rows (`credit_for` names the
    invoice they credit)."""
    from pyspark.sql import functions as F

    inv = _first(invoices, "invoice_id", "issue_date").select(
        F.trim("invoice_id").alias("invoice_id"),
        F.trim("account_id").alias("account_id"),
        F.trim("subscription_id").alias("subscription_id"),
        F.lower(F.trim("invoice_type")).alias("invoice_type"),
        F.nullif(F.trim("credit_for"), F.lit("")).alias("credit_for"),
        F.to_date(F.trim("issue_date")).alias("issue_date"),
        F.upper(F.trim("currency")).alias("currency"),
        _money("amount").alias("_amount"),
    )
    paid = (
        _first(payments, "payment_id", "paid_date")
        .select(
            F.trim("invoice_id").alias("invoice_id"),
            F.to_date(F.trim("paid_date")).alias("paid_date"),
            (_money("amount") * _fx(F.upper(F.trim("currency")))).alias("_usd"),
        )
        .groupBy("invoice_id")
        .agg(
            F.round(F.sum("_usd"), 2).cast("decimal(12,2)").alias("paid_usd"),
            F.max("paid_date").alias("last_paid_date"),
        )
    )
    people = clean_accounts(accounts).select(
        "account_id", F.col("country").alias("account_country")
    )
    amount_usd = F.round(F.col("_amount") * _fx(F.col("currency")), 2).cast("decimal(12,2)")
    paid_usd = F.coalesce(F.col("paid_usd"), F.lit(0).cast("decimal(12,2)"))
    status = (
        F.when(F.col("invoice_type") == "credit_note", "credit")
        .when(paid_usd >= F.col("amount_usd"), "paid")
        .when(paid_usd > 0, "partial")
        .otherwise("open")
    )
    return (
        inv.withColumn("amount_usd", amount_usd)
        .join(paid, "invoice_id", "left")
        .withColumn("paid_usd", paid_usd)
        .withColumn("status", status)
        .join(people, "account_id", "left")
        .select(*INVOICE_COLUMNS)
    )


def mrr_monthly(subscriptions):
    """Per month, country and plan tier: subscriptions active that month and
    their MRR. A subscription counts from its start month up to the month
    before it ended (still active: up to the AS_OF month)."""
    from pyspark.sql import functions as F

    start = F.trunc("start_date", "month")
    last = F.when(F.col("end_date").isNull(), F.trunc(F.lit(AS_OF), "month")).otherwise(
        F.add_months(F.trunc("end_date", "month"), -1)
    )
    months = (
        subscriptions.withColumn("_start", start)
        .withColumn("_last", last)
        .filter(F.col("_last") >= F.col("_start"))
        .withColumn("month", F.explode(F.sequence("_start", "_last", F.expr("interval 1 month"))))
    )
    return (
        months.groupBy("month", "account_country", "plan_tier")
        .agg(
            F.count("*").cast("bigint").alias("active_subscriptions"),
            F.sum("mrr_usd").cast("decimal(14,2)").alias("mrr_usd"),
        )
        .select(*MRR_COLUMNS)
    )


def churn_cohorts(subscriptions):
    """Per start month: subscriptions started, and how many ended within three
    months, as a count and a percentage."""
    from pyspark.sql import functions as F

    early = F.col("end_date").isNotNull() & (
        F.col("end_date") < F.add_months(F.col("start_date"), 3)
    )
    return (
        subscriptions.groupBy(F.trunc("start_date", "month").alias("cohort_month"))
        .agg(
            F.count("*").cast("bigint").alias("subscriptions_started"),
            F.count(F.when(early, 1)).cast("bigint").alias("churned_within_3m"),
        )
        .withColumn(
            "churn_rate_3m_pct",
            F.round(F.col("churned_within_3m") * 100.0 / F.col("subscriptions_started"), 2).cast(
                "decimal(5,2)"
            ),
        )
        .select(*CHURN_COLUMNS)
    )


def ar_aging(invoices):
    """Per account: its invoices not fully paid, what is still open on them in
    USD, and that open amount by age since the issue date as of AS_OF. Credit
    notes are not invoices and are left out."""
    from pyspark.sql import functions as F

    open_items = invoices.filter(
        (F.col("invoice_type") == "invoice") & F.col("status").isin("open", "partial")
    ).withColumn("_open", F.col("amount_usd") - F.col("paid_usd"))
    age = F.datediff(F.lit(AS_OF), F.col("issue_date"))

    def bucket(cond, name):
        return (
            F.coalesce(F.sum(F.when(cond, F.col("_open"))), F.lit(0)).cast("decimal(14,2)").alias(name)
        )

    return (
        open_items.groupBy("account_id", "account_country")
        .agg(
            F.count("*").cast("bigint").alias("open_invoices"),
            F.sum("_open").cast("decimal(14,2)").alias("open_usd"),
            bucket(age <= 30, "bucket_0_30_usd"),
            bucket((age > 30) & (age <= 60), "bucket_31_60_usd"),
            bucket((age > 60) & (age <= 90), "bucket_61_90_usd"),
            bucket(age > 90, "bucket_90_plus_usd"),
        )
        .select(*AGING_COLUMNS)
    )


def finance_kpis(mrr, aging):
    """Per country: MRR and ARR for the AS_OF month (MRR gold), receivables
    still open (AR aging gold), the share open more than 90 days, and days
    sales outstanding (open receivables over a day of MRR)."""
    from pyspark.sql import functions as F

    latest = (
        mrr.filter(F.col("month") == F.trunc(F.lit(AS_OF), "month"))
        .groupBy("account_country")
        .agg(F.sum("mrr_usd").alias("_mrr"))
    )
    open_ar = aging.groupBy("account_country").agg(
        F.sum("open_usd").alias("_open"), F.sum("bucket_90_plus_usd").alias("_90")
    )
    mrr_usd = F.coalesce(F.col("_mrr"), F.lit(0))
    open_usd = F.coalesce(F.col("_open"), F.lit(0))
    return (
        latest.join(open_ar, "account_country", "full_outer")
        .select(
            "account_country",
            mrr_usd.cast("decimal(14,2)").alias("mrr_usd"),
            (mrr_usd * 12).cast("decimal(16,2)").alias("arr_usd"),
            open_usd.cast("decimal(14,2)").alias("open_ar_usd"),
            F.when(open_usd > 0, F.round(F.coalesce(F.col("_90"), F.lit(0)) * 100 / open_usd, 1))
            .otherwise(F.lit(0))
            .cast("decimal(5,1)")
            .alias("overdue_90_pct"),
            F.when(mrr_usd > 0, F.round(open_usd / (mrr_usd / 30), 1))
            .cast("decimal(8,1)")
            .alias("dso_days"),
        )
        .select(*KPI_COLUMNS)
    )
