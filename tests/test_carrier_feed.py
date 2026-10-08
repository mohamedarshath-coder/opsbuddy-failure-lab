"""Level 1: the carrier feed generator in notebooks/common/carrier_feed.py.
v1 and v2 describe the same 120 shipments; only Kestrel Express's weight and
freight text changes in v2, and every other carrier's rows are identical."""

import pytest

from notebooks.common.carrier_feed import CARRIERS, CHANGED_CARRIER, COLUMNS, SHIPMENTS, feed_rows

WEIGHT, FREIGHT = COLUMNS.index("weight"), COLUMNS.index("freight_charge")
CARRIER, WAREHOUSE = COLUMNS.index("carrier_name"), COLUMNS.index("warehouse_code")


def test_eight_text_columns_and_120_shipments_per_version():
    for version in ("v1", "v2"):
        rows = feed_rows(version)
        assert len(COLUMNS) == 8 and len(rows) == SHIPMENTS
        assert all(len(r) == 8 and all(isinstance(v, str) for v in r) for r in rows)


def test_v1_weight_and_freight_are_plain_numbers():
    for row in feed_rows("v1"):
        assert float(row[WEIGHT]) > 0 and float(row[FREIGHT]) > 0


def test_v2_changes_only_the_changed_carriers_weight_and_freight():
    for old, new in zip(feed_rows("v1"), feed_rows("v2")):
        if old[CARRIER] == CHANGED_CARRIER:
            assert new[WEIGHT].endswith((" kg", " g")) and new[FREIGHT].startswith("$")
            assert old[:WEIGHT] == new[:WEIGHT]
        else:
            assert old == new


def test_every_carrier_and_warehouse_appears():
    rows = feed_rows("v1")
    assert {r[CARRIER] for r in rows} == set(CARRIERS)
    assert len({r[WAREHOUSE] for r in rows}) == 12


def test_an_unknown_version_raises():
    with pytest.raises(ValueError):
        feed_rows("v3")
