"""Age band filter for dev.academy.customers_30_45 (notebook 61)."""

MIN_AGE = 30
MAX_AGE = 45


def filter_age_30_45(df):
    """Customers aged 30 to 45 inclusive; a null age is dropped. Columns untouched."""
    from pyspark.sql import functions as F

    return df.filter(F.col("age").between(MIN_AGE, MAX_AGE))
