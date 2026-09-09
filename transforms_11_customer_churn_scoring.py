"""
transforms_11_customer_churn_scoring.py -- extracted, testable churn risk scoring logic for
11_customer_churn_scoring.py.

The notebook itself is a thin orchestration layer:
    from transforms_11_customer_churn_scoring import compute_churn_risk
    raw_df = spark.table("dev.opsbuddy_test.customer_activity")
    scored_df = compute_churn_risk(raw_df)
    scored_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.customer_churn_scores")
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Weighting for each risk factor -- tuned by the growth team, not arbitrary. Higher weight =
# more influence on the final score. These are intentionally exposed as module-level constants
# (not buried in the function) so a future weighting change is a one-line diff, not a rewrite.
WEIGHT_INACTIVITY = 0.40
WEIGHT_SUPPORT_TICKETS = 0.25
WEIGHT_TIER = 0.15
WEIGHT_PAYMENT_FAILURES = 0.20

TIER_RISK_MULTIPLIER = {"free": 1.0, "basic": 0.7, "premium": 0.4, "enterprise": 0.2}


def compute_churn_risk(df: DataFrame) -> DataFrame:
    """Computes a 0-100 churn risk score per customer from four weighted factors, and buckets
    each customer into a risk_tier for the retention team's dashboard.

    Expects columns: customer_id, days_since_last_login, support_tickets_opened_90d,
    subscription_tier, payment_failures_90d.

    A brand-new customer (days_since_last_login is NULL -- they've never logged in a second
    time yet) must NOT be treated as maximally inactive -- that would incorrectly flag every
    new signup as high churn risk on day one. Guarded explicitly below.
    """
    inactivity_score = F.when(
        F.col("days_since_last_login").isNull(), F.lit(0)
    ).otherwise(
        F.least(F.col("days_since_last_login") / F.lit(90.0) * 100, F.lit(100.0))
    )

    support_score = F.least(
        F.col("support_tickets_opened_90d") / F.lit(10.0) * 100, F.lit(100.0)
    )

    tier_multiplier = F.coalesce(
        *[
            F.when(F.lower(F.col("subscription_tier")) == tier, F.lit(mult))
            for tier, mult in TIER_RISK_MULTIPLIER.items()
        ],
        F.lit(1.0),  # unknown tier defaults to the highest-risk multiplier, not a crash
    )
    tier_score = tier_multiplier * F.lit(100.0)

    payment_score = F.least(
        F.col("payment_failures_90d") / F.lit(3.0) * 100, F.lit(100.0)
    )

    risk_score = (
        inactivity_score * F.lit(WEIGHT_INACTIVITY)
        + support_score * F.lit(WEIGHT_SUPPORT_TICKETS)
        + tier_score * F.lit(WEIGHT_TIER)
        + payment_score * F.lit(WEIGHT_PAYMENT_FAILURES)
    )

    return (
        df.withColumn("churn_risk_score", F.round(risk_score, 1))
        .withColumn(
            "risk_tier",
            F.when(F.col("churn_risk_score") >= 70, F.lit("High"))
            .when(F.col("churn_risk_score") >= 40, F.lit("Medium"))
            .otherwise(F.lit("Low")),
        )
        .select(
            "customer_id", "subscription_tier", "days_since_last_login",
            "support_tickets_opened_90d", "payment_failures_90d",
            "churn_risk_score", "risk_tier",
        )
    )
