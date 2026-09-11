"""Shared discrepancy-risk classification, used by the nightly high-risk account flagging job.
Centralized here so the risk threshold only needs to be maintained in one place."""

STANDARD_THRESHOLD = 100.0

# VIP accounts routinely carry larger balances, so a discrepancy that would be alarming on a
# standard account is unremarkable on a VIP one -- support previously escalated a false-positive
# VIP flag caused by applying the standard threshold uniformly. This default is a starting point;
# confirm the exact figure with the business before relying on it for real alerting.
VIP_THRESHOLD = 300.0


def is_high_risk(discrepancy: float, tier: str = "standard") -> bool:
    """A discrepancy is high-risk if its magnitude exceeds the threshold for the account's tier.

    VIP accounts use a higher threshold than standard accounts, since larger discrepancies are
    normal for them.
    """
    threshold = VIP_THRESHOLD if tier == "vip" else STANDARD_THRESHOLD
    return abs(discrepancy) > threshold
