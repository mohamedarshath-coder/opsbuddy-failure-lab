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
