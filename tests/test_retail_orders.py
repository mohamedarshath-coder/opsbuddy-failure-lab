"""Level 2: the retail orders demo (notebooks/common/retail_orders.py): silver
cleaning, the two gold aggregates and the country mart, on a local SparkSession."""

import os
from datetime import date
from decimal import Decimal

import pytest

if os.environ.get("CI") != "true":  # in CI a missing PySpark must fail, not skip
    pytest.importorskip("pyspark", reason="PySpark is not installed here")

from notebooks.common.retail_orders import (  # noqa: E402
    KPI_COLUMNS,
    LTV_COLUMNS,
    REVENUE_COLUMNS,
    SILVER_COLUMNS,
    country_kpis,
    customer_ltv,
    daily_revenue,
    orders_silver,
)

pytestmark = pytest.mark.spark

ORDERS = (
    "order_id string, customer_id string, order_date string, amount string, "
    "channel string, promo_code string, status string"
)
CUSTOMERS = "customer_id string, customer_name string, country string, segment string, signup_date string"


def _silver(spark, orders, customers=None):
    customers = customers or [("C1", "Customer 1", " in", "SMB", "2024-01-01")]
    return orders_silver(
        spark.createDataFrame(orders, schema=ORDERS),
        spark.createDataFrame(customers, schema=CUSTOMERS),
    )


O1 = ("O1", "C1", "2026-07-01", "100.50", "WEB ", "SUMMER10", "Delivered")


def test_silver_types_cleans_and_adds_the_customer(spark):
    [r] = _silver(spark, [O1]).collect()
    assert list(r.asDict()) == SILVER_COLUMNS
    assert r["order_date"] == date(2026, 7, 1) and r["amount"] == Decimal("100.50")
    assert r["channel"] == "web" and r["status"] == "delivered"
    assert r["customer_country"] == "IN" and r["customer_segment"] == "SMB"


def test_a_repeated_order_is_kept_once(spark):
    assert _silver(spark, [O1, O1]).count() == 1


def test_an_order_of_an_unknown_customer_is_kept_without_country(spark):
    [r] = _silver(spark, [("O9", "C404", "2026-07-02", "5.00", "app", "", "delivered")]).collect()
    assert r["customer_id"] == "C404" and r["customer_country"] is None


def test_silver_does_not_carry_the_promo_code(spark):
    assert "promo_code" not in _silver(spark, [O1]).columns


def _orders(spark, rows):
    return _silver(
        spark,
        rows,
        [("C1", "A", "IN", "SMB", "2024-01-01"), ("C2", "B", "us ", "Consumer", "2024-01-01")],
    )


ROWS = [
    ("O1", "C1", "2026-07-01", "100.00", "web", "", "delivered"),
    ("O2", "C1", "2026-07-01", "50.00", "app", "", "cancelled"),
    ("O3", "C2", "2026-07-01", "20.00", "store", "", "delivered"),
    ("O4", "C1", "2026-07-02", "30.00", "web", "", "delivered"),
    ("O5", "C1", "2026-07-03", "10.00", "web", "", "returned"),
]


def test_daily_revenue_counts_only_delivered_orders(spark):
    rows = {
        (r["order_date"], r["customer_country"]): r
        for r in daily_revenue(_orders(spark, ROWS)).collect()
    }
    first = rows[(date(2026, 7, 1), "IN")]
    assert list(first.asDict()) == REVENUE_COLUMNS
    assert first["delivered_orders"] == 1 and first["revenue"] == Decimal("100.00")
    assert first["cancelled_orders"] == 1
    returned_only = rows[(date(2026, 7, 3), "IN")]
    assert returned_only["revenue"] == Decimal("0.00") and returned_only["delivered_orders"] == 0


def test_customer_ltv_per_customer(spark):
    rows = {r["customer_id"]: r for r in customer_ltv(_orders(spark, ROWS)).collect()}
    c1 = rows["C1"]
    assert list(c1.asDict()) == LTV_COLUMNS
    assert c1["delivered_orders"] == 2 and c1["lifetime_revenue"] == Decimal("130.00")
    assert c1["first_order_date"] == date(2026, 7, 1) and c1["last_order_date"] == date(2026, 7, 2)
    assert rows["C2"]["lifetime_revenue"] == Decimal("20.00")


def test_country_kpis_from_daily_revenue(spark):
    rows = {
        r["customer_country"]: r
        for r in country_kpis(daily_revenue(_orders(spark, ROWS))).collect()
    }
    india = rows["IN"]
    assert list(india.asDict()) == KPI_COLUMNS
    assert india["total_revenue"] == Decimal("130.00") and india["total_orders"] == 2
    assert india["active_days"] == 2  # 3 July had only a returned order
    assert india["avg_daily_revenue"] == Decimal("43.33")  # (100 + 30 + 0) / 3 days
    assert rows["US"]["total_revenue"] == Decimal("20.00")
