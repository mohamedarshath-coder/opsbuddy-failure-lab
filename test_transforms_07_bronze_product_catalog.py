"""
test_transforms_07_bronze_product_catalog.py -- real unit tests for
transforms_07_bronze_product_catalog.py, using a LOCAL, in-memory SparkSession.

No Databricks cluster, no Unity Catalog, no real data, no network at all.

Run with: pytest test_transforms_07_bronze_product_catalog.py -v
Requires: pip install pyspark pytest
"""

import sys
sys.path.insert(0, ".")

import pytest
from pyspark.sql import SparkSession

from transforms_07_bronze_product_catalog import filter_active_products


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("transforms-07-unit-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_filters_uppercase_true_correctly(spark):
    df = spark.createDataFrame(
        [(1, "Widget", "TRUE", 9.99), (2, "Gadget", "FALSE", 19.99)],
        schema=["product_id", "product_name", "is_active", "base_price"],
    )
    result = filter_active_products(df).collect()

    assert len(result) == 1
    assert result[0]["product_id"] == 1


def test_case_insensitive_matching_is_the_actual_point_of_this_function(spark):
    """This is the specific bug this function must prevent -- confirmed against every case
    variant actually observed in the real bronze data across this project ('TRUE', 'true',
    'True' have all shown up)."""
    df = spark.createDataFrame(
        [(1, "A", "TRUE", 1.0), (2, "B", "true", 1.0), (3, "C", "True", 1.0), (4, "D", "FALSE", 1.0)],
        schema=["product_id", "product_name", "is_active", "base_price"],
    )
    result = filter_active_products(df).collect()

    matched_ids = {row["product_id"] for row in result}
    assert matched_ids == {1, 2, 3}, f"expected all three case variants to match, got {matched_ids}"


def test_only_expected_columns_are_returned(spark):
    df = spark.createDataFrame(
        [(1, "Widget", "TRUE", 9.99, "extra_column_value")],
        schema=["product_id", "product_name", "is_active", "base_price", "some_other_field"],
    )
    result = filter_active_products(df)

    assert set(result.columns) == {"product_id", "product_name", "base_price"}


def test_empty_input_produces_empty_output_not_an_error(spark):
    empty_df = spark.createDataFrame(
        [], schema="product_id int, product_name string, is_active string, base_price double"
    )
    result = filter_active_products(empty_df).collect()
    assert result == []
