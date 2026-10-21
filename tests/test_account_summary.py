"""Unit tests for notebooks/common/account_summary.py (SCRUM-146)."""
from datetime import date
from decimal import Decimal

import pytest

from notebooks.common.account_summary import build_account_summary

pytestmark = pytest.mark.spark

SCHEMA = (
    "account_id string, industry string, billing_country string, annual_revenue decimal(14,2), "
    "employee_count int, created_date date, tier string, renewal_date date, "
    "is_strategic boolean, region_owner_team string"
)


def _row(i, ind="Software", c="US", rev="100.00", tier="Gold", ren=None, team="EMEA", strat=True):
    return (i, ind, c, Decimal(rev), 10, date(2024, 1, 1), tier, ren, strat, team)


def _run(spark, *rows):
    return {(r["industry"], r["billing_country"]): r
            for r in build_account_summary(spark.createDataFrame(list(rows), SCHEMA)).collect()}


def test_account_renewing_in_q4_is_counted(spark):
    r = _run(spark, _row("1", ren=date(2026, 11, 15), rev="250.50"))[("Software", "US")]
    assert (r["renewals_q4"], r["renewal_revenue_q4"]) == (1, Decimal("250.50"))


def test_account_renewing_outside_q4_is_not_counted(spark):
    r = _run(spark, _row("1", ren=date(2027, 1, 5)), _row("2", ren=date(2026, 9, 30)),
             _row("3", ren=date(2027, 1, 1)))[("Software", "US")]
    assert (r["renewals_q4"], r["renewal_revenue_q4"]) == (0, Decimal("0.00"))
    assert r["accounts_count"] == 3


def test_account_with_no_tier_record_counts_for_nothing_extra(spark):
    r = _run(spark, _row("1", tier="Unassigned", ren=None, team=None))[("Software", "US")]
    assert (r["accounts_count"], r["renewals_q4"], r["region_owner_teams"]) == (1, 0, 0)
    assert r["revenue_unassigned"] == Decimal("100.00")


def test_q4_boundaries_are_included(spark):
    r = _run(spark, _row("1", ren=date(2026, 10, 1)), _row("2", ren=date(2026, 12, 31)))[
        ("Software", "US")]
    assert (r["renewals_q4"], r["renewal_revenue_q4"]) == (2, Decimal("200.00"))


def test_duplicate_and_null_teams_count_once_or_none(spark):
    r = _run(spark, _row("1", team="A"), _row("2", team="A"), _row("3", team="B"),
             _row("4", team=None))[("Software", "US")]
    assert r["region_owner_teams"] == 2


def test_blank_team_after_cleaning_is_null_so_counts_none(spark):
    r = _run(spark, _row("1", team=None), _row("2", team=None))[("Software", "US")]
    assert r["region_owner_teams"] == 0


def test_existing_columns_and_rows_are_unchanged(spark):
    out = build_account_summary(spark.createDataFrame(
        [_row("1", ren=date(2026, 11, 1)), _row("2", c="UK", tier="Silver")], SCHEMA))
    assert out.columns[:13] == [
        "industry", "billing_country", "accounts_count", "total_revenue", "avg_revenue",
        "total_employees", "first_created", "last_created", "strategic_accounts",
        "revenue_gold", "revenue_silver", "revenue_bronze", "revenue_unassigned"]
    assert out.columns[13:] == ["renewals_q4", "renewal_revenue_q4", "region_owner_teams"]
    assert out.count() == 2 and sum(r["accounts_count"] for r in out.collect()) == 2


def test_empty_input_gives_no_rows(spark):
    assert build_account_summary(spark.createDataFrame([], SCHEMA)).count() == 0
