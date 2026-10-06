"""Level 1: high-risk classification in notebooks/common/discrepancy_rules.py.
High risk when the discrepancy's size is above the tier's threshold:
100 for standard accounts, 300 for VIP accounts."""

import pytest

from notebooks.common.discrepancy_rules import (
    STANDARD_THRESHOLD,
    VIP_THRESHOLD,
    is_high_risk,
)


@pytest.mark.parametrize(
    ("discrepancy", "expected"),
    [(0.0, False), (99.99, False), (100.0, False), (100.01, True), (5000.0, True)],
)
def test_standard_threshold_is_exclusive_at_100(discrepancy, expected):
    assert is_high_risk(discrepancy, "standard") is expected


@pytest.mark.parametrize(
    ("discrepancy", "expected"),
    [(250.0, False), (300.0, False), (300.5, True)],
)
def test_vip_accounts_use_the_higher_threshold(discrepancy, expected):
    assert is_high_risk(discrepancy, "vip") is expected


@pytest.mark.parametrize(
    ("discrepancy", "tier", "expected"),
    [(-100.0, "standard", False), (-150.0, "standard", True), (-400.0, "vip", True)],
)
def test_negative_discrepancies_count_by_their_size(discrepancy, tier, expected):
    assert is_high_risk(discrepancy, tier) is expected


def test_standard_is_the_default_tier():
    assert is_high_risk(150.0) is True


def test_a_discrepancy_that_flags_a_standard_account_is_normal_for_vip():
    assert is_high_risk(200.0, "standard") is True
    assert is_high_risk(200.0, "vip") is False


def test_vip_threshold_is_above_the_standard_threshold():
    assert VIP_THRESHOLD > STANDARD_THRESHOLD
