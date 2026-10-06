"""Shared test setup (DataSakshi D103).

Level 1 tests import plain Python modules (notebooks/common/*.py, transforms_*.py)
and need nothing else. Level 2 tests take the `spark` fixture: a small local
SparkSession on this machine, never a Databricks cluster. On a laptop without
Java or PySpark those tests are skipped with the reason; in CI (CI=true) a
missing Spark fails the run instead, so they can never be skipped silently.
"""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    try:
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master("local[1]")
            .appName("unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # no Java or no PySpark here
        if os.environ.get("CI") == "true":
            raise
        pytest.skip(f"local Spark is not available here: {type(exc).__name__}: {exc}")
    yield session
    session.stop()
