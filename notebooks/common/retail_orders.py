"""Retail orders demo: silver cleaning, gold aggregates and the country mart
(notebooks 82-85).

The web shop sends every field as text: country as " in" or "us", channel as
"WEB " or "App", and its export can repeat an order. Silver keeps one row per
order_id with typed fields and the customer's country and segment; gold and the
mart aggregate delivered orders. Spark expressions only, so they run on the
executors. Silver keeps only the fields the reports use today."""

COUNTRIES = ["AU", "DE", "GB", "IN", "US"]
CHANNELS = ["app", "store", "web"]
STATUSES = ["cancelled", "delivered", "returned"]
SILVER_COLUMNS = [
    "order_id", "customer_id", "order_date", "amount", "channel", "status",
    "customer_country", "customer_segment",
]
REVENUE_COLUMNS = [
    "order_date", "customer_country", "delivered_orders", "revenue", "cancelled_orders",
]
LTV_COLUMNS = [
    "customer_id", "delivered_orders", "lifetime_revenue", "first_order_date", "last_order_date",
]
KPI_COLUMNS = [
    "customer_country", "total_revenue", "avg_daily_revenue", "active_days", "total_orders",
]


def clean_customers(df):
    """Trimmed customers, one row per customer_id, country as an upper-case code."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    first = Window.partitionBy(F.trim("customer_id")).orderBy("customer_name")
    return (
        df.withColumn("_n", F.row_number().over(first))
        .filter(F.col("_n") == 1)
        .select(
            F.trim("customer_id").alias("customer_id"),
            F.upper(F.trim("country")).alias("country"),
            F.trim("segment").alias("segment"),
        )
    )


def orders_silver(orders, customers):
    """One row per order_id, typed, with the customer's country and segment.
    A repeated order keeps one copy; an order of an unknown customer is kept
    with no country or segment."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    first = Window.partitionBy(F.trim("order_id")).orderBy("order_date", "amount")
    typed = (
        orders.withColumn("_n", F.row_number().over(first))
        .filter(F.col("_n") == 1)
        .select(
            F.trim("order_id").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.to_date(F.trim("order_date")).alias("order_date"),
            F.trim("amount").cast("decimal(12,2)").alias("amount"),
            F.lower(F.trim("channel")).alias("channel"),
            F.lower(F.trim("status")).alias("status"),
        )
    )
    people = clean_customers(customers).select(
        "customer_id",
        F.col("country").alias("customer_country"),
        F.col("segment").alias("customer_segment"),
    )
    return typed.join(people, "customer_id", "left").select(*SILVER_COLUMNS)


def daily_revenue(silver):
    """Per order_date and customer_country: delivered orders, their revenue and
    cancelled orders."""
    from pyspark.sql import functions as F

    delivered = F.col("status") == "delivered"
    return (
        silver.groupBy("order_date", "customer_country")
        .agg(
            F.count(F.when(delivered, 1)).cast("bigint").alias("delivered_orders"),
            F.coalesce(F.sum(F.when(delivered, F.col("amount"))), F.lit(0))
            .cast("decimal(14,2)")
            .alias("revenue"),
            F.count(F.when(F.col("status") == "cancelled", 1)).cast("bigint").alias("cancelled_orders"),
        )
        .select(*REVENUE_COLUMNS)
    )


def customer_ltv(silver):
    """Per customer: delivered orders, lifetime revenue, first and last order dates."""
    from pyspark.sql import functions as F

    delivered = silver.filter(F.col("status") == "delivered")
    return (
        delivered.groupBy("customer_id")
        .agg(
            F.count("*").cast("bigint").alias("delivered_orders"),
            F.sum("amount").cast("decimal(14,2)").alias("lifetime_revenue"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .select(*LTV_COLUMNS)
    )


def country_kpis(daily):
    """Per customer_country, from the daily revenue table."""
    from pyspark.sql import functions as F

    return (
        daily.groupBy("customer_country")
        .agg(
            F.sum("revenue").cast("decimal(16,2)").alias("total_revenue"),
            F.avg("revenue").cast("decimal(14,2)").alias("avg_daily_revenue"),
            F.countDistinct(F.when(F.col("delivered_orders") > 0, F.col("order_date")))
            .cast("bigint")
            .alias("active_days"),
            F.sum("delivered_orders").cast("bigint").alias("total_orders"),
        )
        .select(*KPI_COLUMNS)
    )
