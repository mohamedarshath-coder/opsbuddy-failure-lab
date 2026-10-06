"""Level 1: the late-fee policy in notebooks/common/late_fee_rules.py.
$5 per day overdue, capped at $100 for domestic and $50 for international accounts."""

import pytest

from notebooks.common.late_fee_rules import DOMESTIC_CAP, INTERNATIONAL_CAP, calculate_late_fee


def test_no_days_overdue_means_no_fee():
    assert calculate_late_fee(0, "domestic") == 0.0
    assert calculate_late_fee(0, "international") == 0.0


@pytest.mark.parametrize(("days", "fee"), [(1, 5.0), (3, 15.0), (12, 60.0)])
def test_fee_is_five_dollars_per_day_below_the_cap(days, fee):
    assert calculate_late_fee(days, "domestic") == fee


@pytest.mark.parametrize(("days", "fee"), [(19, 95.0), (20, 100.0), (21, 100.0), (365, 100.0)])
def test_domestic_fee_stops_at_the_domestic_cap(days, fee):
    assert calculate_late_fee(days, "domestic") == fee


@pytest.mark.parametrize(("days", "fee"), [(9, 45.0), (10, 50.0), (11, 50.0), (365, 50.0)])
def test_international_fee_stops_at_the_lower_international_cap(days, fee):
    assert calculate_late_fee(days, "international") == fee


def test_international_cap_is_below_the_domestic_cap():
    assert INTERNATIONAL_CAP < DOMESTIC_CAP


def test_fee_is_a_float():
    assert isinstance(calculate_late_fee(4, "domestic"), float)
