# Unit tests

Standard for this repo (DataSakshi D103). Unit tests check the pipeline's
**logic** with small, made-up examples. They run on GitHub in seconds,
**before** anything is deployed to Databricks:

1. the agent writes and runs them with its change;
2. the push guard (`.github/scripts/unit_test_guard.py`) runs them again and
   refuses the push if any fails, a test was deleted or skipped, or changed
   logic has no test;
3. CI (`deploy_pipeline.yml`) runs them first; red means no bundle deploy.

## Levels

| Level | What | How |
|---|---|---|
| 1 | Plain Python logic: mappings, rules, calculations, dates | `tests/test_<module>.py`, no Spark |
| 2 | DataFrame logic: joins, aggregations, filters, windows, dedup | mark `@pytest.mark.spark`, take the `spark` fixture, build a few rows with `spark.createDataFrame` |

The QA bundle run (level 3) and DataSakshi's data checks (level 4) cover what
unit tests cannot: Databricks-only features, real data, the job as a whole.

## Rules

- Logic lives in importable `.py` modules (`notebooks/common/`, `transforms_*.py`);
  a notebook only reads, calls the function and writes.
- One test file per module: `tests/test_<module>.py`.
- At least one test per acceptance criterion, named after it.
- Cover the edge cases that apply: every branch, boundaries (at, just under,
  just over), null and blank, unknown values, unmatched joins, duplicate keys,
  division by zero, empty input, rounding, dates at month or year ends.
- A bug fix comes with a test that fails on the old code.
- Made-up data only: no real customer data, no Databricks connection.
- Never delete, skip or weaken an existing test.
- A change with no logic (config, rename, passthrough) needs no test; say why.

Run them: `pip install -r requirements-dev.txt && pytest`.
