"""Supplier spend demo: silver suppliers, silver purchase orders and the spend
by country report (notebooks 81-83).

The procurement system sends every field as text. Buyers type a supplier's
country by hand, so one country arrives in many spellings ("USA", "US",
"United States", "America", ...); silver passes it through as typed (trimmed
only), and the report shows one row per spelling. Amounts can carry thousands
separators ("43,044.04") and are converted to USD with the fixed rates below;
the supplier export can repeat a row. Spark expressions only, so they run on
the executors.

Lineage, as built:
  sp_suppliers         <- sp_suppliers_raw
  sp_purchase_orders   <- sp_purchase_orders_raw, sp_suppliers
  sp_spend_by_country  <- sp_purchase_orders, sp_suppliers"""

FX_TO_USD = {
    "USD": 1.0, "GBP": 1.27, "EUR": 1.08, "INR": 0.012,
    "JPY": 0.0067, "AED": 0.27, "CAD": 0.73, "AUD": 0.66,
}
PO_STATUSES = ["received", "open", "cancelled"]

SUPPLIER_COLUMNS = [
    "supplier_id", "supplier_name", "country", "city", "category", "payment_terms_days",
    "onboarded_date", "is_preferred", "annual_contract_usd", "currency",
]
PO_COLUMNS = [
    "po_id", "supplier_id", "supplier_name", "supplier_country", "po_date", "status",
    "buyer_team", "line_items", "amount", "amount_usd",
]
SPEND_COLUMNS = [
    "supplier_country", "suppliers_count", "preferred_suppliers", "po_count",
    "total_spend_usd", "avg_po_usd", "open_po_usd", "first_po_date", "last_po_date",
]


def _money(col):
    """A text amount ("43,044.04", " 99.5 ") as decimal(14,2)."""
    from pyspark.sql import functions as F

    return F.regexp_replace(F.trim(col), ",", "").cast("decimal(14,2)")


def _fx(currency_col):
    """The USD rate for a currency column (null for an unknown currency)."""
    from itertools import chain

    from pyspark.sql import functions as F

    rates = F.create_map(*[F.lit(x) for x in chain(*FX_TO_USD.items())])
    return rates[currency_col]


def _usd(amount_col, currency_col):
    return (amount_col * _fx(currency_col)).cast("decimal(14,2)")


def _first(df, key):
    """One row per `key` (trimmed): a repeated export keeps one copy."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(F.trim(key)).orderBy(F.trim(key))
    return df.withColumn("_n", F.row_number().over(w)).filter(F.col("_n") == 1).drop("_n")


def clean_suppliers(raw):
    """One row per supplier, typed. Country is passed through as typed (trimmed)."""
    from pyspark.sql import functions as F

    flag = F.lower(F.trim("is_preferred"))
    currency = F.upper(F.trim("currency"))
    return _first(raw, "supplier_id").select(
        F.trim("supplier_id").alias("supplier_id"),
        F.trim("supplier_name").alias("supplier_name"),
        F.trim("country").alias("country"),
        F.trim("city").alias("city"),
        F.trim("category").alias("category"),
        F.regexp_extract(F.trim("payment_terms"), r"Net (\d+)", 1).cast("int")
        .alias("payment_terms_days"),
        F.to_date(F.trim("onboarded_date")).alias("onboarded_date"),
        flag.isin("true", "yes", "1").alias("is_preferred"),
        _usd(_money("annual_contract_value"), currency).alias("annual_contract_usd"),
        currency.alias("currency"),
    )


def purchase_orders(raw, suppliers):
    """One row per PO, typed, with its supplier's name and country and the amount in USD."""
    from pyspark.sql import functions as F

    currency = F.upper(F.trim("currency"))
    po = _first(raw, "po_id").select(
        F.trim("po_id").alias("po_id"),
        F.trim("supplier_id").alias("supplier_id"),
        F.to_date(F.trim("po_date")).alias("po_date"),
        F.lower(F.trim("status")).alias("status"),
        F.trim("buyer_team").alias("buyer_team"),
        F.trim("line_items").cast("int").alias("line_items"),
        _money("amount").alias("amount"),
        _usd(_money("amount"), currency).alias("amount_usd"),
    )
    s = suppliers.select(
        "supplier_id", "supplier_name", F.col("country").alias("supplier_country")
    )
    return po.join(s, "supplier_id", "left").select(*PO_COLUMNS)


def spend_by_country(orders, suppliers):
    """One row per supplier country: its suppliers, and the spend on POs that were
    not cancelled (open POs counted separately too)."""
    from pyspark.sql import functions as F

    by_supplier = suppliers.groupBy(F.col("country").alias("supplier_country")).agg(
        F.count("*").cast("bigint").alias("suppliers_count"),
        F.count(F.when(F.col("is_preferred"), 1)).cast("bigint").alias("preferred_suppliers"),
    )
    live = orders.filter(F.col("status") != "cancelled")
    by_po = live.groupBy("supplier_country").agg(
        F.count("*").cast("bigint").alias("po_count"),
        F.sum("amount_usd").cast("decimal(16,2)").alias("total_spend_usd"),
        F.round(F.avg("amount_usd"), 2).cast("decimal(14,2)").alias("avg_po_usd"),
        F.coalesce(F.sum(F.when(F.col("status") == "open", F.col("amount_usd"))), F.lit(0))
        .cast("decimal(16,2)")
        .alias("open_po_usd"),
        F.min("po_date").alias("first_po_date"),
        F.max("po_date").alias("last_po_date"),
    )
    joined = by_supplier.join(by_po, "supplier_country", "left")
    return joined.select(
        "supplier_country",
        "suppliers_count",
        "preferred_suppliers",
        F.coalesce("po_count", F.lit(0)).cast("bigint").alias("po_count"),
        F.coalesce("total_spend_usd", F.lit(0)).cast("decimal(16,2)").alias("total_spend_usd"),
        "avg_po_usd",
        F.coalesce("open_po_usd", F.lit(0)).cast("decimal(16,2)").alias("open_po_usd"),
        "first_po_date",
        "last_po_date",
    )
