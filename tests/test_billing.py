"""Level 2: the subscription billing demo (notebooks/common/billing.py): silver
cleaning, the three gold tables and the finance mart, on a local SparkSession."""

import os
from datetime import date
from decimal import Decimal

import pytest

if os.environ.get("CI") != "true":  # in CI a missing PySpark must fail, not skip
    pytest.importorskip("pyspark", reason="PySpark is not installed here")

from notebooks.common.billing import (  # noqa: E402
    AGING_COLUMNS,
    AS_OF,
    CHURN_COLUMNS,
    INVOICE_COLUMNS,
    KPI_COLUMNS,
    MRR_COLUMNS,
    SUBSCRIPTION_COLUMNS,
    ar_aging,
    churn_cohorts,
    finance_kpis,
    invoices_silver,
    mrr_monthly,
    subscriptions_silver,
)

pytestmark = pytest.mark.spark

ACCOUNTS = "account_id string, account_name string, country string, industry string, signup_date string"
PLANS = "plan_id string, plan_name string, tier string, billing_period string, list_price string, currency string"
SUBS = (
    "subscription_id string, account_id string, plan_id string, start_date string, "
    "end_date string, seats string, discount_pct string, status string"
)
INVOICES = (
    "invoice_id string, account_id string, subscription_id string, invoice_type string, "
    "credit_for string, issue_date string, due_date string, amount string, currency string"
)
PAYMENTS = "payment_id string, invoice_id string, paid_date string, amount string, currency string"

A1 = ("A1", "Account 1", " gb", "Software ", "2025-01-01")
MONTHLY = ("P01", "Growth monthly", "Growth", "monthly", "29.00", "USD")
ANNUAL = ("P17", "Growth annual", "growth", "Annual", "1,200.00", "USD")


def _subs(spark, subs, plans=(MONTHLY, ANNUAL), accounts=(A1,)):
    return subscriptions_silver(
        spark.createDataFrame(list(subs), schema=SUBS),
        spark.createDataFrame(list(plans), schema=PLANS),
        spark.createDataFrame(list(accounts), schema=ACCOUNTS),
    )


def _invoices(spark, invoices, payments=(), accounts=(A1,)):
    return invoices_silver(
        spark.createDataFrame(list(invoices), schema=INVOICES),
        spark.createDataFrame(list(payments), schema=PAYMENTS),
        spark.createDataFrame(list(accounts), schema=ACCOUNTS),
    )


S1 = ("S1", "A1", "P01", "2026-07-15", "", "10", "10", "Active ")


def test_silver_subscription_is_typed_with_plan_account_and_mrr(spark):
    [r] = _subs(spark, [S1]).collect()
    assert list(r.asDict()) == SUBSCRIPTION_COLUMNS
    assert r["start_date"] == date(2026, 7, 15) and r["end_date"] is None
    assert r["status"] == "active" and r["plan_tier"] == "growth"
    assert r["account_country"] == "GB" and r["account_industry"] == "software"
    assert r["mrr_usd"] == pytest.approx(29 * 10 * 0.9)


def test_an_annual_plan_is_spread_over_twelve_months(spark):
    [r] = _subs(spark, [("S2", "A1", "P17", "2026-01-01", "", "2", "0", "active")]).collect()
    assert r["billing_period"] == "annual" and r["mrr_usd"] == pytest.approx(200.0)


def test_a_repeated_subscription_is_kept_once(spark):
    assert _subs(spark, [S1, S1]).count() == 1


I1 = ("I1", "A1", "S1", "Invoice", "", "2026-08-01", "2026-08-31", "1,000.00", "gbp")


def test_silver_invoice_is_in_usd_with_payments_and_status(spark):
    pays = [
        ("PM1", "I1", "2026-08-20", "600.00", "GBP"),
        ("PM2", "I1", "2026-09-05", "400.00", "GBP"),
    ]
    [r] = _invoices(spark, [I1], pays).collect()
    assert list(r.asDict()) == INVOICE_COLUMNS
    assert r["amount_usd"] == Decimal("1270.00") and r["paid_usd"] == Decimal("1270.00")
    assert r["status"] == "paid" and r["last_paid_date"] == date(2026, 9, 5)
    assert r["account_country"] == "GB" and r["currency"] == "GBP"


def test_silver_invoices_drop_the_due_date_and_keep_credit_notes(spark):
    credit = ("CN1", "A1", "S1", "credit_note", "I1", "2026-08-10", "", "100.00", "GBP")
    rows = {r["invoice_id"]: r for r in _invoices(spark, [I1, credit]).collect()}
    assert "due_date" not in INVOICE_COLUMNS
    assert rows["CN1"]["status"] == "credit" and rows["CN1"]["credit_for"] == "I1"
    assert rows["I1"]["status"] == "open" and rows["I1"]["paid_usd"] == Decimal("0.00")


def test_a_part_payment_is_partial(spark):
    [r] = _invoices(spark, [I1], [("PM1", "I1", "2026-08-20", "10.00", "GBP")]).collect()
    assert r["status"] == "partial"


def test_mrr_counts_each_month_until_the_one_before_the_end(spark):
    ended = ("S3", "A1", "P01", "2026-06-10", "2026-08-20", "1", "0", "cancelled")
    rows = {r["month"]: r for r in mrr_monthly(_subs(spark, [S1, ended])).collect()}
    assert list(next(iter(rows.values())).asDict()) == MRR_COLUMNS
    assert rows[date(2026, 6, 1)]["active_subscriptions"] == 1  # S3 only
    assert rows[date(2026, 7, 1)]["active_subscriptions"] == 2  # S1 and S3
    assert rows[date(2026, 8, 1)]["active_subscriptions"] == 1  # S3 ended in August
    assert rows[date(AS_OF.year, AS_OF.month, 1)]["mrr_usd"] == Decimal("261.00")


def test_churn_cohorts_count_early_ends(spark):
    early = ("S4", "A1", "P01", "2026-07-02", "2026-08-01", "1", "0", "cancelled")
    [r] = churn_cohorts(_subs(spark, [S1, early])).collect()
    assert list(r.asDict()) == CHURN_COLUMNS
    assert r["subscriptions_started"] == 2 and r["churned_within_3m"] == 1
    assert r["churn_rate_3m_pct"] == Decimal("50.00")


def test_ar_aging_buckets_open_invoices_by_age_and_skips_credit_notes(spark):
    old = ("I2", "A1", "S1", "invoice", "", "2026-05-01", "2026-05-31", "500.00", "GBP")
    credit = ("CN1", "A1", "S1", "credit_note", "I1", "2026-09-25", "", "100.00", "GBP")
    [r] = ar_aging(_invoices(spark, [I1, old, credit])).collect()
    assert list(r.asDict()) == AGING_COLUMNS
    assert r["open_invoices"] == 2 and r["open_usd"] == Decimal("1905.00")
    assert r["bucket_31_60_usd"] == Decimal("1270.00")  # I1, 60 days before AS_OF
    assert r["bucket_90_plus_usd"] == Decimal("635.00")  # I2


def test_finance_kpis_reconcile_mrr_and_receivables(spark):
    mrr = mrr_monthly(_subs(spark, [S1]))
    aging = ar_aging(_invoices(spark, [I1]))
    [r] = finance_kpis(mrr, aging).collect()
    assert list(r.asDict()) == KPI_COLUMNS
    assert r["account_country"] == "GB" and r["mrr_usd"] == Decimal("261.00")
    assert r["arr_usd"] == Decimal("3132.00") and r["open_ar_usd"] == Decimal("1270.00")
    assert r["overdue_90_pct"] == Decimal("0.0")
    assert r["dso_days"] == Decimal("146.0")  # 1270 / (261 / 30)
