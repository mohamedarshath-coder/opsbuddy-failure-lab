"""The raw carrier feed the shipment SLA pipeline reads (notebooks 50 to 53).

The carriers send one row per shipment, every field as text. `feed_rows("v1")` is
the format all four carriers have sent so far. `feed_rows("v2")` is the same 120
shipments after Kestrel Express changed its export: weights now carry a unit and
thousands separators ("1,235.95 kg", "31150 g") and freight a currency sign
("$1,184.20"). The other carriers are unchanged. Landing only: the pipeline does
not import this module."""

from datetime import date, timedelta

COLUMNS = [
    "feed_row_id", "shipment_ref", "warehouse_code", "carrier_name",
    "shipped_at", "delivered_at", "weight", "freight_charge",
]
CARRIERS = ["Northwind Freight", "Kestrel Express", "Harbor Line", "Atlas Parcel"]
WAREHOUSES = [f"WH{n:02d}" for n in range(1, 13)]
SHIPMENTS = 120
CHANGED_CARRIER = "Kestrel Express"


def _shipment(i: int) -> dict:
    shipped = date(2026, 9, 1) + timedelta(days=(i * 3) % 28)
    transit = 1 + (i * 5) % 6
    weight = round(0.25 + ((i * 1373) % 15000) / 10, 2)
    freight = round(15 + weight * 0.8 + (i % 9) * 3.25, 2)
    return {
        "ref": f"SH{i:05d}",
        "warehouse": WAREHOUSES[(i * 7) % len(WAREHOUSES)],
        "carrier": CARRIERS[i % len(CARRIERS)],
        "shipped": shipped,
        "delivered": None if i % 15 == 0 else shipped + timedelta(days=transit),
        "weight_kg": weight,
        "freight_usd": freight,
    }


def _v2_weight(kg: float) -> str:
    if kg < 50:  # small parcels are now sent in grams
        return f"{kg * 1000:.0f} g"
    return f"{kg:,.2f} kg"


def feed_rows(version: str = "v1") -> list[tuple]:
    if version not in ("v1", "v2"):
        raise ValueError(f"unknown feed version: {version!r}")
    rows = []
    for i in range(1, SHIPMENTS + 1):
        s = _shipment(i)
        changed = version == "v2" and s["carrier"] == CHANGED_CARRIER
        weight = _v2_weight(s["weight_kg"]) if changed else f"{s['weight_kg']:.2f}"
        freight = f"${s['freight_usd']:,.2f}" if changed else f"{s['freight_usd']:.2f}"
        rows.append(
            (
                f"F{i:05d}",
                s["ref"],
                s["warehouse"],
                s["carrier"],
                s["shipped"].isoformat(),
                s["delivered"].isoformat() if s["delivered"] else "",
                weight,
                freight,
            )
        )
    return rows
