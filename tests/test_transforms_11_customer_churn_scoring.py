"""Level 2: churn risk scoring in transforms_11_customer_churn_scoring.py, on a
local SparkSession with a few made-up customers.

score = inactivity x 0.40 + support x 0.25 + tier x 0.15 + payment failures x 0.20,
each factor 0-100; High from 70, Medium from 40, else Low."""

import os

import pytest

if os.environ.get("CI") != "true":  # in CI a missing PySpark must fail, not skip
    pytest.importorskip("pyspark", reason="PySpark is not installed here")

from transforms_11_customer_churn_scoring import compute_churn_risk  # noqa: E402

pytestmark = pytest.mark.spark

SCHEMA = (
    "customer_id string, days_since_last_login int, support_tickets_opened_90d int, "
    "subscription_tier string, payment_failures_90d int"
)
OUTPUT = [
    "customer_id",
    "subscription_tier",
    "days_since_last_login",
    "support_tickets_opened_90d",
    "payment_failures_90d",
    "churn_risk_score",
    "risk_tier",
]


def _score(spark, *rows):
    df = compute_churn_risk(spark.createDataFrame(list(rows), schema=SCHEMA))
    return {r["customer_id"]: (r["churn_risk_score"], r["risk_tier"]) for r in df.collect()}


def test_a_new_customer_who_never_logged_in_again_is_not_maximally_inactive(spark):
    # inactivity 0 (not 100) + enterprise tier 0.2 x 100 x 0.15 = 3.0
    assert _score(spark, ("new", None, 0, "enterprise", 0)) == {"new": (3.0, "Low")}


def test_every_factor_at_its_ceiling_scores_100_high(spark):
    # 180 days, 20 tickets and 6 failures are each capped at 100
    assert _score(spark, ("max", 180, 20, "free", 6)) == {"max": (100.0, "High")}


def test_tier_boundaries_70_and_40(spark):
    scores = _score(
        spark,
        ("just_under_high", 90, 10, "enterprise", 0),  # 40 + 25 + 3 = 68.0
        ("high", 90, 10, "enterprise", 1),  # 68 + 6.67 = 74.7
        ("at_medium", 45, 2, "free", 0),  # 20 + 5 + 15 = 40.0
        ("just_under_medium", 45, 1, "free", 0),  # 20 + 2.5 + 15 = 37.5
    )
    assert scores == {
        "just_under_high": (68.0, "Medium"),
        "high": (74.7, "High"),
        "at_medium": (40.0, "Medium"),
        "just_under_medium": (37.5, "Low"),
    }


def test_tier_is_case_insensitive_and_an_unknown_tier_counts_as_highest_risk(spark):
    scores = _score(
        spark,
        ("upper", 0, 0, "PREMIUM", 0),  # 0.4 x 100 x 0.15 = 6.0
        ("unknown", 0, 0, "gold", 0),  # 1.0 x 100 x 0.15 = 15.0
    )
    assert scores == {"upper": (6.0, "Low"), "unknown": (15.0, "Low")}


def test_one_output_row_per_input_row_with_the_expected_columns(spark):
    df = compute_churn_risk(
        spark.createDataFrame([("a", 1, 0, "basic", 0), ("b", 2, 1, "free", 1)], schema=SCHEMA)
    )
    assert df.columns == OUTPUT
    assert df.count() == 2


def test_empty_input_gives_empty_output(spark):
    assert compute_churn_risk(spark.createDataFrame([], schema=SCHEMA)).collect() == []
