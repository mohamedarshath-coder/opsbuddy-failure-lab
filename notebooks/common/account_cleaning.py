"""Silver-layer cleaning for the Salesforce accounts medallion demo (notebook 72).

The CRM sends every field as text and types industry by hand (" software ",
"RETAIL"), and its export can repeat an account. Silver keeps one row per
account_id, types the numbers and dates, and writes industry as one of
INDUSTRIES. Spark expressions only, so it runs on the executors."""

INDUSTRIES = ["Software", "Retail", "Healthcare", "Finance", "Manufacturing"]
SILVER_COLUMNS = [
    "account_id", "account_name", "industry", "billing_country", "annual_revenue",
    "employee_count", "created_date", "owner_team",
]


def clean_accounts(df):
    """Typed, trimmed, one row per account_id (the first by account_name order)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    first = Window.partitionBy("account_id").orderBy("account_name")
    return (
        df.withColumn("_n", F.row_number().over(first))
        .filter(F.col("_n") == 1)
        .select(
            F.trim("account_id").alias("account_id"),
            F.trim("account_name").alias("account_name"),
            F.initcap(F.lower(F.trim("industry"))).alias("industry"),
            F.upper(F.trim("billing_country")).alias("billing_country"),
            F.col("annual_revenue").cast("decimal(14,2)").alias("annual_revenue"),
            F.col("employee_count").cast("int").alias("employee_count"),
            F.to_date("created_date").alias("created_date"),
            F.trim("owner_team").alias("owner_team"),
        )
    )


TIERS = ["Gold", "Silver", "Bronze"]
UNASSIGNED = "Unassigned"
TIER_COLUMNS = ["tier", "tier_since", "renewal_date", "health_score", "is_strategic"]
# clean_accounts keeps SILVER_COLUMNS; the silver table is those plus the tier fields.
SILVER_TIER_COLUMNS = SILVER_COLUMNS + TIER_COLUMNS


def clean_tiers(df):
    """Typed tier records, one row per account_id. A tier outside TIERS becomes
    "Unassigned"; unparseable dates, decimals and booleans become null."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    first = Window.partitionBy(F.trim("account_id")).orderBy(
        F.col("tier_since").desc_nulls_last(), F.col("tier")
    )
    flag = F.lower(F.trim("is_strategic"))
    tier = F.initcap(F.lower(F.trim("tier")))
    return (
        df.withColumn("_n", F.row_number().over(first))
        .filter(F.col("_n") == 1)
        .select(
            F.trim("account_id").alias("account_id"),
            F.when(tier.isin(TIERS), tier).otherwise(F.lit(UNASSIGNED)).alias("tier"),
            F.expr("try_cast(trim(tier_since) as date)").alias("tier_since"),
            F.expr("try_cast(trim(renewal_date) as date)").alias("renewal_date"),
            F.expr("try_cast(trim(health_score) as decimal(5,2))").alias("health_score"),
            F.when(flag.isin("true", "yes", "1"), F.lit(True))
            .when(flag.isin("false", "no", "0"), F.lit(False))
            .alias("is_strategic"),
        )
    )


def add_tiers(accounts, tiers):
    """Left join the typed tiers onto cleaned accounts; no record -> "Unassigned"."""
    from pyspark.sql import functions as F

    t = tiers.select("account_id", *TIER_COLUMNS).withColumnRenamed("tier", "_tier")
    joined = accounts.join(t, "account_id", "left")
    return joined.select(
        *SILVER_COLUMNS,
        F.coalesce(F.col("_tier"), F.lit(UNASSIGNED)).alias("tier"),
        "tier_since", "renewal_date", "health_score", "is_strategic",
    )
