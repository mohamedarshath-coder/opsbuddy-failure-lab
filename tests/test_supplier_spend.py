"""Level 2: the supplier spend demo (notebooks/common/supplier_spend.py): silver
suppliers, silver purchase orders and the spend by country report, on a local
SparkSession."""

import os
from datetime import date
from decimal import Decimal

import pytest

if os.environ.get("CI") != "true":  # in CI a missing PySpark must fail, not skip
    pytest.importorskip("pyspark", reason="PySpark is not installed here")

from notebooks.common.supplier_spend import (  # noqa: E402
    PO_COLUMNS,
    SPEND_COLUMNS,
    SUPPLIER_COLUMNS,
    clean_suppliers,
    purchase_orders,
    spend_by_country,
)

pytestmark = pytest.mark.spark

SUPPLIERS = (
    "supplier_id string, supplier_name string, country string, city string, category string, "
    "payment_terms string, onboarded_date string, is_preferred string, "
    "annual_contract_value string, currency string"
)
POS = (
    "po_id string, supplier_id string, po_date string, amount string, currency string, "
    "status string, buyer_team string, line_items string, delivery_site string"
)


def _suppliers(spark, *rows):
    return clean_suppliers(spark.createDataFrame(list(rows), SUPPLIERS))


def _supplier(sid, country="USA", preferred="false", acv="1000.00", currency="USD"):
    return (sid, f"Supplier {sid}", country, "Chicago", "Logistics", "2/10 Net 30",
            "2024-03-01", preferred, acv, currency)


def _po(pid, sid, amount="100.00", currency="USD", status="received", day="2026-02-01"):
    return (pid, sid, day, amount, currency, status, "Finance", "3", "HQ")


def test_suppliers_are_typed_and_keep_country_as_typed(spark):
    out = _suppliers(
        spark,
        (" SP001 ", " Acme ", " United States ", "Austin", "IT services", "Net 45",
         "2023-05-01", "yes", "12,500.00", "gbp"),
    ).collect()
    assert out[0].asDict() == {
        "supplier_id": "SP001",
        "supplier_name": "Acme",
        "country": "United States",
        "city": "Austin",
        "category": "IT services",
        "payment_terms_days": 45,
        "onboarded_date": date(2023, 5, 1),
        "is_preferred": True,
        "annual_contract_usd": Decimal("15875.00"),
        "currency": "GBP",
    }


def test_suppliers_have_their_columns_in_order(spark):
    assert _suppliers(spark, _supplier("SP001")).columns == SUPPLIER_COLUMNS


def test_a_repeated_supplier_row_is_kept_once(spark):
    assert _suppliers(spark, _supplier("SP001"), _supplier(" SP001")).count() == 1


def test_discount_terms_read_the_net_days(spark):
    assert _suppliers(spark, _supplier("SP001")).collect()[0].payment_terms_days == 30


def test_a_po_gets_its_supplier_and_usd_amount(spark):
    suppliers = _suppliers(spark, _supplier("SP001", country="Japan", currency="JPY"))
    raw = spark.createDataFrame([_po("PO1", "SP001", "10,000.00", "JPY")], POS)
    row = purchase_orders(raw, suppliers).collect()[0]
    assert purchase_orders(raw, suppliers).columns == PO_COLUMNS
    assert (row.supplier_name, row.supplier_country) == ("Supplier SP001", "Japan")
    assert (row.amount, row.amount_usd) == (Decimal("10000.00"), Decimal("67.00"))
    assert (row.po_date, row.line_items, row.status) == (date(2026, 2, 1), 3, "received")


def test_a_repeated_po_row_is_kept_once(spark):
    suppliers = _suppliers(spark, _supplier("SP001"))
    raw = spark.createDataFrame([_po("PO1", "SP001"), _po("PO1 ", "SP001")], POS)
    assert purchase_orders(raw, suppliers).count() == 1


def test_spend_by_country_excludes_cancelled_and_counts_open(spark):
    suppliers = _suppliers(
        spark, _supplier("SP001", preferred="true"), _supplier("SP002"), _supplier("SP003", "Canada")
    )
    raw = spark.createDataFrame(
        [
            _po("PO1", "SP001", "100.00", day="2026-01-05"),
            _po("PO2", "SP002", "300.00", status="open", day="2026-03-10"),
            _po("PO3", "SP001", "999.00", status="cancelled"),
        ],
        POS,
    )
    report = spend_by_country(purchase_orders(raw, suppliers), suppliers)
    assert report.columns == SPEND_COLUMNS
    rows = {r.supplier_country: r for r in report.collect()}
    usa = rows["USA"]
    assert (usa.suppliers_count, usa.preferred_suppliers, usa.po_count) == (2, 1, 2)
    assert (usa.total_spend_usd, usa.avg_po_usd, usa.open_po_usd) == (
        Decimal("400.00"), Decimal("200.00"), Decimal("300.00")
    )
    assert (usa.first_po_date, usa.last_po_date) == (date(2026, 1, 5), date(2026, 3, 10))


def test_a_country_with_suppliers_but_no_pos_shows_zero_spend(spark):
    suppliers = _suppliers(spark, _supplier("SP003", "Canada"))
    raw = spark.createDataFrame([], POS)
    row = spend_by_country(purchase_orders(raw, suppliers), suppliers).collect()[0]
    assert (row.supplier_country, row.suppliers_count, row.po_count) == ("Canada", 1, 0)
    assert (row.total_spend_usd, row.open_po_usd) == (Decimal("0.00"), Decimal("0.00"))
    assert row.first_po_date is None

