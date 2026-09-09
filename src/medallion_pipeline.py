"""Medallion Pipeline — Bronze → Silver → Gold transaction processing.

Spark Declarative Pipeline (SDP/DLT) implementation of the daily revenue
reconciliation pipeline.  Replaces the notebook-based pipeline/ files with
a single DLT module so the prod_mode_probe2 pipeline can resolve its
configured library at src/medallion_pipeline.py.

Layers
------
Bronze  daily_txn_raw              Synthetic raw transaction feed
Silver  daily_txn_extract          Today's slice from the raw feed
Silver  daily_txn_clean            Validated / cleaned transactions
Gold    daily_reconciliation_report  Per-account reconciliation vs. ledger
Gold    daily_reconciliation_summary Roll-up for finance morning review
"""

import random

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Bronze layer


@dlt.table(
    name="daily_txn_raw",
    comment="Bronze: synthetic raw transaction feed (charges and refunds).",
)
def daily_txn_raw():
    """Generate 1000 synthetic transactions.

    Refunds are represented as negative amount values, a normal
    expected part of any real payments feed.
    """
    random.seed(7)

    rows = []
    for i in range(1, 1001):
        account_id = f"ACC{random.randint(1, 200):04d}"
        if random.random() < 0.03:
            amount = -round(random.uniform(5, 250), 2)
            txn_type = "refund"
        else:
            amount = round(random.uniform(5, 500), 2)
            txn_type = "charge"
        rows.append((f"TXN{i:06d}", account_id, amount, txn_type, "2026-08-30"))

    return spark.createDataFrame(  # noqa: F821
        rows,
        ["transaction_id", "account_id", "amount", "txn_type", "txn_date"],
    )


# Silver layer - extract today's slice


@dlt.table(
    name="daily_txn_extract",
    comment="Silver: today's slice extracted from the raw feed.",
)
def daily_txn_extract():
    return dlt.read("daily_txn_raw").filter(F.col("txn_date") == "2026-08-30")


# Silver layer - validate and clean


@dlt.table(
    name="daily_txn_clean",
    comment="Silver: validated and cleaned transactions (nulls removed).",
)
@dlt.expect_or_drop(
    "non_refund_amount_positive",
    "(txn_type = 'refund') OR (amount >= 0)",
)
def daily_txn_clean():
    """Drop rows with null amounts; constrain non-refund amounts >= 0."""
    return dlt.read("daily_txn_extract").filter(F.col("amount").isNotNull())


# Gold layer - reconciliation report


@dlt.table(
    name="daily_reconciliation_report",
    comment="Gold: per-account reconciliation of transactions vs. ledger.",
)
def daily_reconciliation_report():
    """Join aggregated transaction totals against a synthetic accounting
    ledger and compute per-account discrepancies.  Only active accounts
    are included (finance excludes dormant accounts from the daily review).
    """
    txn_totals = (
        dlt.read("daily_txn_clean")
        .groupBy("account_id")
        .agg(F.sum("amount").alias("txn_total"))
    )

    # Synthetic ledger - in production this would be read from the
    # accounting system's own export table.
    ledger_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("ledger_balance", DoubleType(), False),
            StructField("is_active", BooleanType(), False),
        ]
    )
    ledger = spark.createDataFrame(  # noqa: F821
        [(f"ACC{i:04d}", float(i % 500), i % 5 != 0) for i in range(1, 201)],
        schema=ledger_schema,
    )

    return (
        txn_totals.join(ledger, on="account_id", how="left")
        .withColumn("discrepancy", F.col("txn_total") - F.col("ledger_balance"))
        .filter(F.col("is_active").eqNullSafe(True))
        .select("account_id", "txn_total", "ledger_balance", "discrepancy")
    )


# Gold layer - daily summary


@dlt.table(
    name="daily_reconciliation_summary",
    comment="Gold: daily roll-up of reconciliation for finance review.",
)
def daily_reconciliation_summary():
    report = dlt.read("daily_reconciliation_report")
    return report.agg(
        F.count("*").alias("accounts_checked"),
        F.sum(F.when(F.col("discrepancy") != 0, 1).otherwise(0)).alias(
            "accounts_with_discrepancy"
        ),
        F.sum("discrepancy").alias("net_discrepancy"),
        F.avg(F.abs("discrepancy")).alias("avg_abs_discrepancy"),
    )
