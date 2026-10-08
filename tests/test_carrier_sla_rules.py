"""Level 1: the carrier delivery promise in notebooks/common/carrier_sla_rules.py,
which the carrier SLA report (notebook 53) uses for on_time_count and on_time_pct."""

import pytest

from notebooks.common.carrier_sla_rules import PROMISED_DAYS, is_on_time, promised_days


@pytest.mark.parametrize(
    ("carrier", "days"),
    [("Northwind Freight", 3), ("Kestrel Express", 2), ("Harbor Line", 5), ("Atlas Parcel", 4)],
)
def test_each_carrier_has_its_promise(carrier, days):
    assert promised_days(carrier) == days


@pytest.mark.parametrize(("carrier", "transit"), [("Kestrel Express", 2), ("Harbor Line", 5)])
def test_delivered_on_the_promised_day_is_on_time(carrier, transit):
    assert is_on_time(carrier, transit) is True


@pytest.mark.parametrize(("carrier", "transit"), [("Kestrel Express", 3), ("Atlas Parcel", 5)])
def test_one_day_over_the_promise_is_late(carrier, transit):
    assert is_on_time(carrier, transit) is False


def test_a_shipment_in_transit_is_neither_on_time_nor_late():
    assert is_on_time("Northwind Freight", None) is None


def test_an_unknown_carrier_raises():
    with pytest.raises(ValueError, match="no delivery promise"):
        is_on_time("Unknown Haulage", 1)


def test_negative_transit_days_raise():
    with pytest.raises(ValueError, match="negative"):
        is_on_time("Harbor Line", -1)


def test_every_promise_is_a_positive_whole_number_of_days():
    assert PROMISED_DAYS and all(isinstance(d, int) and d > 0 for d in PROMISED_DAYS.values())
