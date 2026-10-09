"""Level 2: silver cleaning for the accounts medallion
(notebooks/common/account_cleaning.py), on a local SparkSession."""

import os
from datetime import date
from decimal import Decimal

import pytest

if os.environ.get("CI") != "true":  # in CI a missing PySpark must fail, not skip
    pytest.importorskip("pyspark", reason="PySpark is not installed here")

from notebooks.common.account_cleaning import INDUSTRIES, SILVER_COLUMNS, clean_accounts  # noqa: E402

pytestmark = pytest.mark.spark

RAW = (
    "account_id string, account_name string, industry string, billing_country string, "
    "annual_revenue string, employee_count string, created_date string, owner_team string"
)


def _clean(spark, *rows):
    return clean_accounts(spark.createDataFrame(list(rows), schema=RAW))


ROW = ("A001", "Account 001", " software ", "usa", "125000.50", "120", "2024-03-05", "Commercial")


def test_types_and_columns(spark):
    [r] = _clean(spark, ROW).collect()
    assert list(r.asDict()) == SILVER_COLUMNS
    assert r["annual_revenue"] == Decimal("125000.50") and r["employee_count"] == 120
    assert r["created_date"] == date(2024, 3, 5) and r["billing_country"] == "USA"


@pytest.mark.parametrize("typed", [" software ", "SOFTWARE", "software", "Software"])
def test_industry_is_standardized(spark, typed):
    [r] = _clean(spark, ROW[:2] + (typed,) + ROW[3:]).collect()
    assert r["industry"] == "Software" and r["industry"] in INDUSTRIES


def test_a_repeated_account_is_kept_once(spark):
    out = _clean(spark, ROW, ROW, ("A002",) + ROW[1:]).collect()
    assert sorted(r["account_id"] for r in out) == ["A001", "A002"]


def test_empty_input_gives_empty_output(spark):
    assert _clean(spark).count() == 0
