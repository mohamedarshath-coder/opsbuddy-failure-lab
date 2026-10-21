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


# ---- SCRUM-141: tier fields ----
from notebooks.common.account_cleaning import (  # noqa: E402
    SILVER_TIER_COLUMNS,
    add_tiers,
    clean_tiers,
)

TRAW = (
    "account_id string, tier string, tier_since string, region_owner_team string, "
    "renewal_date string, health_score string, is_strategic string, source_system string"
)


def _tiers(spark, *rows):
    return clean_tiers(spark.createDataFrame(list(rows), schema=TRAW))


def _tr(acc, tier, strat="true", since="2024-01-31", ren="2025-12-31", hs="87.5"):
    return (acc, tier, since, "T", ren, hs, strat, "sf")


def _silver(spark, accounts, *tier_rows):
    return add_tiers(_clean(spark, *accounts), _tiers(spark, *tier_rows))


def test_gold_silver_bronze_accounts_get_tier_and_typed_fields(spark):
    a = [ROW, ("A002",) + ROW[1:], ("A003",) + ROW[1:]]
    rows = _silver(
        spark, a, _tr("A001", "Gold"), _tr("A002", "Silver", "no"), _tr("A003", "Bronze", "1")
    ).orderBy("account_id").collect()
    assert [r["tier"] for r in rows] == ["Gold", "Silver", "Bronze"]
    assert [r["is_strategic"] for r in rows] == [True, False, True]
    assert rows[0]["tier_since"] == date(2024, 1, 31)
    assert rows[0]["renewal_date"] == date(2025, 12, 31)
    assert rows[0]["health_score"] == Decimal("87.50")
    assert list(rows[0].asDict()) == SILVER_TIER_COLUMNS


def test_account_with_no_tier_is_unassigned_with_null_fields(spark):
    a = [ROW, ("A037",) + ROW[1:]]
    rows = _silver(spark, a, _tr("A001", "Gold")).orderBy("account_id").collect()
    assert len(rows) == 2
    r = rows[1]
    assert r["tier"] == "Unassigned"
    assert (r["tier_since"], r["renewal_date"], r["health_score"], r["is_strategic"]) == (
        None, None, None, None)


def test_text_to_type_conversion_and_bad_values(spark):
    rows = {
        r["account_id"]: r
        for r in _tiers(
            spark,
            _tr(" A1 ", " gOLD ", " YES ", "2024-02-29", "2024-12-31", " 9.999 "),
            _tr("A2", "Silver", "false", "bad", "", "abc"),
            _tr("A3", "Bronze", "maybe", None, None, None),
            _tr("A4", "Platinum"),
            _tr("A5", None),
        ).collect()
    }
    assert rows["A1"]["tier"] == "Gold" and rows["A1"]["is_strategic"] is True
    assert rows["A1"]["tier_since"] == date(2024, 2, 29)
    assert rows["A1"]["health_score"] == Decimal("10.00")
    assert rows["A2"]["is_strategic"] is False
    assert (rows["A2"]["tier_since"], rows["A2"]["renewal_date"], rows["A2"]["health_score"]) == (
        None, None, None)
    assert rows["A3"]["is_strategic"] is None and rows["A3"]["tier_since"] is None
    assert rows["A4"]["tier"] == "Unassigned"
    assert rows["A5"]["tier"] == "Unassigned"


def test_duplicate_tier_rows_do_not_duplicate_accounts(spark):
    rows = _silver(
        spark, [ROW], _tr("A001", "Gold"), _tr("A001", "Silver", since="2023-01-01"), _tr("A001 ", "Gold")
    ).collect()
    assert len(rows) == 1 and rows[0]["tier"] == "Gold"


def test_existing_columns_keep_values_and_empty_tiers(spark):
    before = _clean(spark, ROW).collect()[0].asDict()
    after = _silver(spark, [ROW]).collect()[0].asDict()
    assert all(after[c] == before[c] for c in before)
    assert after["tier"] == "Unassigned"


# ---- SCRUM-146: region_owner_team ----
def test_region_owner_team_is_trimmed_and_carried_to_silver_last(spark):
    s = _silver(spark, [ROW], _tr(ROW[0], "Gold")[:3] + (" EMEA-West ",) + _tr(ROW[0], "Gold")[4:])
    assert s.columns[-1] == "region_owner_team"
    assert s.collect()[0]["region_owner_team"] == "EMEA-West"


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_blank_region_owner_team_becomes_null(spark, blank):
    t = _tr("A1", "Gold")
    row = t[:3] + (blank,) + t[4:]
    assert _tiers(spark, row).collect()[0]["region_owner_team"] is None


def test_account_without_tier_record_has_null_region_owner_team(spark):
    assert _silver(spark, [ROW]).collect()[0]["region_owner_team"] is None
