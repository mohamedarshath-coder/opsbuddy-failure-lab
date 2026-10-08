"""Level 2: age band filter used by notebook 61 (customers_30_45)."""

import pytest

from notebooks.common.customer_age_band import filter_age_30_45

pytestmark = pytest.mark.spark
SCHEMA = "customer_id string, age int, email string"


def _ids(df):
    return sorted(r.customer_id for r in df.collect())


def test_ages_30_and_45_are_included(spark):
    df = spark.createDataFrame([("a", 30, None), ("b", 45, "x")], SCHEMA)
    assert _ids(filter_age_30_45(df)) == ["a", "b"]


def test_ages_29_and_46_are_excluded(spark):
    df = spark.createDataFrame([("a", 29, None), ("b", 46, None)], SCHEMA)
    assert _ids(filter_age_30_45(df)) == []


def test_null_age_is_excluded(spark):
    df = spark.createDataFrame([("a", None, None), ("b", 40, None)], SCHEMA)
    assert _ids(filter_age_30_45(df)) == ["b"]


def test_mixed_input_returns_exactly_in_range_rows(spark):
    rows = [("a", 29, None), ("b", 30, None), ("c", 37, None), ("d", 45, None), ("e", 46, None), ("f", None, None)]
    assert _ids(filter_age_30_45(spark.createDataFrame(rows, SCHEMA))) == ["b", "c", "d"]


def test_columns_order_types_and_values_unchanged(spark):
    df = spark.createDataFrame([("a", 33, "e@x.com")], SCHEMA)
    out = filter_age_30_45(df)
    assert out.schema == df.schema
    assert out.collect() == df.collect()


def test_no_duplicate_ids_after_running_twice(spark):
    df = spark.createDataFrame([("a", 33, None), ("b", 50, None)], SCHEMA)
    once = filter_age_30_45(df)
    twice = filter_age_30_45(once)
    assert _ids(twice) == _ids(once) == ["a"]


def test_empty_input_gives_empty_output_with_schema(spark):
    df = spark.createDataFrame([], SCHEMA)
    out = filter_age_30_45(df)
    assert out.count() == 0
    assert out.schema == df.schema
