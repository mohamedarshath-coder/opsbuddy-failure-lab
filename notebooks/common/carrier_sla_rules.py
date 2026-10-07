"""Delivery promise per carrier, used by the carrier SLA report (notebook 53).

A delivered shipment is on time when its transit days are within the carrier's
promised days. A shipment still in transit is neither on time nor late."""

PROMISED_DAYS = {
    "Northwind Freight": 3,
    "Kestrel Express": 2,
    "Harbor Line": 5,
    "Atlas Parcel": 4,
}


def promised_days(carrier: str) -> int:
    """Promised transit days for a carrier; an unknown carrier raises."""
    try:
        return PROMISED_DAYS[carrier]
    except KeyError:
        raise ValueError(f"no delivery promise for carrier: {carrier!r}") from None


def is_on_time(carrier: str, transit_days: int | None) -> bool | None:
    """True or False for a delivered shipment; None while it is in transit."""
    if transit_days is None:
        return None
    if transit_days < 0:
        raise ValueError(f"transit days cannot be negative: {transit_days}")
    return transit_days <= promised_days(carrier)
