"""Shared discrepancy-risk classification, used by the nightly high-risk account flagging job.
Centralized here so the risk threshold only needs to be maintained in one place."""


def is_high_risk(discrepancy: float) -> bool:
    """A discrepancy is high-risk if its magnitude exceeds the standard $100 threshold."""
    return abs(discrepancy) > 100.0
