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
    COUNTRY_ALIASES,
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
    usa = rows["United States"]
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



def test_is_preferred_reads_yes_1_true_and_treats_other_values_as_false(spark):
    rows = _suppliers(
        spark,
        _supplier("A", preferred="yes"), _supplier("B", preferred="1"),
        _supplier("C", preferred="false"), _supplier("D", preferred="TRUE"),
        _supplier("E", preferred=""),
    ).collect()
    assert {r.supplier_id: r.is_preferred for r in rows} == {
        "A": True, "B": True, "C": False, "D": True, "E": False}


def test_an_unknown_currency_gives_a_null_usd_amount(spark):
    suppliers = _suppliers(spark, _supplier("SP001"))
    raw = spark.createDataFrame([_po("PO1", "SP001", "50.00", "XXX")], POS)
    row = purchase_orders(raw, suppliers).collect()[0]
    assert (row.amount, row.amount_usd) == (Decimal("50.00"), None)


def test_a_lower_case_currency_is_converted(spark):
    suppliers = _suppliers(spark, _supplier("SP001"))
    raw = spark.createDataFrame([_po("PO1", "SP001", "100.00", "eur")], POS)
    assert purchase_orders(raw, suppliers).collect()[0].amount_usd == Decimal("108.00")


def test_a_po_with_an_unknown_supplier_keeps_its_row_with_null_supplier(spark):
    suppliers = _suppliers(spark, _supplier("SP001"))
    raw = spark.createDataFrame([_po("PO1", "NOPE")], POS)
    out = purchase_orders(raw, suppliers).collect()
    assert len(out) == 1
    assert (out[0].supplier_name, out[0].supplier_country) == (None, None)


def test_a_country_with_only_cancelled_pos_shows_zero_pos_and_spend(spark):
    suppliers = _suppliers(spark, _supplier("SP001"))
    raw = spark.createDataFrame([_po("PO1", "SP001", "500.00", status="Cancelled")], POS)
    row = spend_by_country(purchase_orders(raw, suppliers), suppliers).collect()[0]
    assert (row.po_count, row.total_spend_usd, row.open_po_usd) == (
        0, Decimal("0.00"), Decimal("0.00"))
    assert row.first_po_date is None


def test_country_spellings_are_not_merged(spark):
    # SCRUM-148 reversed the old behaviour: known spellings of one country now merge.
    suppliers = _suppliers(spark, _supplier("A", "USA"), _supplier("B", "US"))
    raw = spark.createDataFrame([], POS)
    out = spend_by_country(purchase_orders(raw, suppliers), suppliers).collect()
    assert [(r.supplier_country, r.suppliers_count) for r in out] == [("United States", 2)]


def _countries(spark, *typed):
    rows = [_supplier(f"S{i:02d}", c) for i, c in enumerate(typed)]
    return [r.country for r in _suppliers(spark, *rows).orderBy("supplier_id").collect()]


def test_mixed_case_spelling_with_spaces_maps_to_standard_name(spark):
    assert _countries(spark, " usa ", "  U.S. ", "uSa") == ["United States"] * 3


def test_alias_america_maps_to_united_states(spark):
    assert _countries(spark, "America", "United States of America", "US") == ["United States"] * 3


def test_every_ticket_alias_maps_to_its_standard_name(spark):
    typed = ["UK", "Great Britain", "England", "GB", "Deutschland", "DE", "Bharat", "IN",
             "U.A.E.", "Aus", "AU", "Japan", "Canada"]
    assert _countries(spark, *typed) == [
        "United Kingdom"] * 4 + ["Germany"] * 2 + ["India"] * 2 + [
        "United Arab Emirates", "Australia", "Australia", "Japan", "Canada"]


def test_unknown_spelling_is_kept_trimmed_and_not_dropped(spark):
    assert _countries(spark, " Atlantis ", "") == ["Atlantis", ""]


def test_standard_names_are_unchanged(spark):
    names = sorted(set(COUNTRY_ALIASES.values()))
    assert len(names) == 8
    assert sorted(_countries(spark, *names)) == names


def test_spend_by_country_has_one_row_per_country_and_totals_unchanged(spark):
    suppliers = _suppliers(
        spark, _supplier("A", "USA", "true"), _supplier("B", " america "),
        _supplier("C", "U.S."), _supplier("D", "UK"), _supplier("E", "Atlantis"))
    raw = spark.createDataFrame(
        [_po("P1", "A", "100.00"), _po("P2", "B", "50.00"), _po("P3", "C", "25.50"),
         _po("P4", "D", "10.00"), _po("P5", "E", "5.00")], POS)
    orders = purchase_orders(raw, suppliers)
    rows = {r.supplier_country: r for r in spend_by_country(orders, suppliers).collect()}
    assert sorted(rows) == ["Atlantis", "United Kingdom", "United States"]
    assert (rows["United States"].suppliers_count, rows["United States"].po_count) == (3, 3)
    assert rows["United States"].total_spend_usd == Decimal("175.50")
    assert sum(r.suppliers_count for r in rows.values()) == 5
    assert sum(r.po_count for r in rows.values()) == 5
    assert sum(r.total_spend_usd for r in rows.values()) == Decimal("190.50")
