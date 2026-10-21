"""Gold account_summary aggregation (notebook 73). Reads only silver columns."""

Q4_START = "2026-10-01"
Q4_END = "2026-12-31"


def build_account_summary(silver):
    """One row per industry and billing_country; existing columns unchanged, plus
    renewals_q4, renewal_revenue_q4 and region_owner_teams."""
    from pyspark.sql import functions as F

    in_q4 = F.col("renewal_date").between(F.lit(Q4_START).cast("date"), F.lit(Q4_END).cast("date"))
    return silver.groupBy("industry", "billing_country").agg(
        F.count("*").alias("accounts_count"),
        F.sum("annual_revenue").cast("decimal(16,2)").alias("total_revenue"),
        F.round(F.avg("annual_revenue"), 2).cast("decimal(14,2)").alias("avg_revenue"),
        F.sum("employee_count").cast("bigint").alias("total_employees"),
        F.min("created_date").alias("first_created"),
        F.max("created_date").alias("last_created"),
        F.count(F.when(F.col("is_strategic"), 1)).cast("bigint").alias("strategic_accounts"),
        *[
            F.sum(F.when(F.col("tier") == t, F.col("annual_revenue")))
            .cast("decimal(16,2)")
            .alias(f"revenue_{t.lower()}")
            for t in ["Gold", "Silver", "Bronze", "Unassigned"]
        ],
        F.count(F.when(in_q4, 1)).cast("bigint").alias("renewals_q4"),
        F.coalesce(F.sum(F.when(in_q4, F.col("annual_revenue"))), F.lit(0))
        .cast("decimal(16,2)")
        .alias("renewal_revenue_q4"),
        F.countDistinct("region_owner_team").cast("bigint").alias("region_owner_teams"),
    )
