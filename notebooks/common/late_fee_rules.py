"""Shared late-fee calculation logic, used by the nightly overdue-account fee assessment job.
Centralized here so the fee policy only needs to be maintained in one place."""


def calculate_late_fee(days_overdue: int) -> float:
    """Flat $5/day late fee, capped at $100."""
    return min(days_overdue * 5.0, 100.0)
