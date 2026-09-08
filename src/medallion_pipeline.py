"""
Medallion pipeline for prod_mode_probe2 — Bronze / Silver / Gold layers.

This Spark Declarative Pipeline implements a basic order-processing medallion
architecture deployed via a Declarative Automation Bundle into the `dev` catalog.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# ---------------------------------------------------------------------------
# Bronze layer — synthetic raw order data (self-contained, no external dependency)
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_amount", DoubleType(), True),
        StructField("discount_pct", DoubleType(), False),
        StructField("region", StringType(), False),
    ]
)

BRONZE_DATA = [
    (1, "CUST-001", 250.00, 0.10, "west"),
    (2, "CUST-002", 130.50, 0.05, "east"),
    (3, "CUST-003", 75.00, 0.15, "west"),
    (4, "CUST-004", 89.99, 0.00, "north"),
    (5, "CUST-005", 410.25, 0.20, "south"),
    (6, "CUST-001", 75.00, 0.10, "west"),
    (7, "CUST-006", 62.30, 0.05, "east"),
    (8, "CUST-003", 199.99, 0.15, "west"),
]


@dlt.table(comment="Bronze layer — raw order data")
def bronze_orders():
    return spark.createDataFrame(BRONZE_DATA, schema=BRONZE_SCHEMA)


# ---------------------------------------------------------------------------
# Silver layer — cleaned and standardised
# ---------------------------------------------------------------------------


@dlt.table(comment="Silver layer — cleaned orders with standardised column names")
def silver_orders():
    return dlt.read("bronze_orders").withColumnRenamed("customer_id", "cust_id").filter(F.col("order_id").isNotNull())


# ---------------------------------------------------------------------------
# Gold layer — customer-level aggregation
# ---------------------------------------------------------------------------


@dlt.table(comment="Gold layer — per-customer order summary")
def gold_customer_summary():
    return (
        dlt.read("silver_orders")
        .groupBy("cust_id")
        .agg(
            F.sum("order_amount").alias("total_order_amount"),
            F.avg("discount_pct").alias("avg_discount_pct"),
            F.count("order_id").alias("order_count"),
        )
    )


# ---------------------------------------------------------------------------
# Gold layer — discount-adjusted totals
# ---------------------------------------------------------------------------


@dlt.table(comment="Gold layer — discount-adjusted order totals")
def gold_adjusted_orders():
    return (
        dlt.read("bronze_orders")
        .withColumn(
            "adjusted_amount",
            F.coalesce(F.col("order_amount"), F.lit(0.0)) * (F.lit(1.0) - F.col("discount_pct")),
        )
        .select("order_id", "customer_id", "adjusted_amount")
    )
