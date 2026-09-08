"""
transforms_07_bronze_product_catalog.py -- extracted, testable transformation logic for
07_bronze_product_catalog.py.

The notebook itself is a thin orchestration layer:
    from transforms_07_bronze_product_catalog import filter_active_products
    raw_df = spark.table("dev.opsbuddy_test.raw_product_feed")
    active_df = filter_active_products(raw_df)   # <-- the actual logic, now testable
    active_df.write.mode("overwrite").saveAsTable("dev.opsbuddy_test.bronze_active_products")
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_active_products(df: DataFrame) -> DataFrame:
    """Keep only products marked active."""
    return (
        df.filter(F.col("is_active") == "true")
        .select("product_id", "product_name", "base_price")
    )
