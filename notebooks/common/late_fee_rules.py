"""Shared late-fee calculation logic, used by the nightly overdue-account fee assessment job.
Centralized here so the fee policy only needs to be maintained in one place."""

DOMESTIC_CAP = 100.0
INTERNATIONAL_CAP = 50.0


def calculate_late_fee(days_overdue: int, region: str) -> float:
    """$5/day late fee, capped per region.

    Several jurisdictions this company operates in cap late fees below the US domestic
    maximum, so international accounts must use a lower cap than domestic accounts.
    """
    cap = INTERNATIONAL_CAP if region == "international" else DOMESTIC_CAP
    return min(days_overdue * 5.0, cap)
