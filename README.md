# opsbuddy-failure-lab

Ten real, independently-failing Databricks notebooks, each simulating a genuinely different
production failure category — built to test `opsbuddy-fix` against real Databricks notebook
telemetry (a direct Python stack trace via `get_run_output`) instead of a dbt wrapper's swallowed
exit code. Every failure is a real operation that actually breaks when run — nothing here
raises a fake exception just to look like a failure.

| # | Notebook | Category | What actually breaks |
|---|---|---|---|
| 1 | `01_null_pointer_email_domain_extractor.py` | Null Pointer / NoneType | `.split()` called on a `None` email value with no null check |
| 2 | `02_oom_full_customer_history_export.py` | OOM / Executor Lost | Explicit `crossJoin` of two 50k-row DataFrames (2.5B rows) then `.collect()` |
| 3 | `03_data_not_found_loyalty_enrichment.py` | Data Not Found at Source | Reads from a mount path that no longer exists |
| 4 | `04_permission_denied_system_catalog_write.py` | Permission / Access Denied | Writes to the reserved, read-only `system` catalog |
| 5 | `05_dependency_error_geo_enrichment.py` | Dependency / Library Import Error | Imports `geopy`, never installed on this cluster |
| 6 | `06_data_skew_order_events_by_customer.py` | Data Skew / Partition Explosion | 95% of 2M rows collapse onto one key, then `collect_list` per key |
| 7 | `07_schema_mismatch_regional_summary.py` | Schema Mismatch | References a column dropped two cells earlier in the same notebook |
| 8 | `08_config_error_payment_gateway_sync.py` | Config / Secret Reference Error | `dbutils.secrets.get` against a secret scope that doesn't exist |
| 9 | `09_accidental_cartesian_order_items.py` | Accidental Cartesian Join | `.join()` with no condition — a genuinely missing `on=`, not an intentional cross join |
| 10 | `10_broadcast_oom_product_catalog_join.py` | Broadcast Join OOM | Explicit `broadcast()` hint on a catalog table that outgrew the broadcast threshold |

## Setting these up as Databricks jobs

Each notebook is meant to be its own separate job (one failure per run, one incident per job) —
not one job with ten tasks. For each notebook:

1. Create a new job in the Databricks UI.
2. Task type: **Notebook**.
3. Source: **Git provider** — point it at this repo (`https://github.com/<org>/opsbuddy-failure-lab`),
   branch `main`, notebook path `notebooks/<filename>` (e.g. `notebooks/01_null_pointer_email_domain_extractor.py`).
4. Attach it to an existing all-purpose cluster (or a small job cluster — most of these don't need
   a large cluster to fail; #2, #6, #9, and #10 are the ones most sensitive to cluster size).
5. Run once manually to confirm it fails, then note the job ID for `opsbuddy-fix`.

No `dbt`, no Snowflake, no external data — every notebook is fully self-contained with synthetic
data generated inline, so nothing here depends on the `insightops` demo project at all.

## `pipeline/` — a real multi-task job with a genuine upstream failure

Unlike the 10 standalone notebooks above, this is **one job with four dependent tasks**, each
writing a real Delta table the next task reads — testing two things the standalone notebooks
can't: a genuine **Upstream Task Dependency Failure** (when one task fails, its downstream tasks
never run and are reported as upstream-failed, not independently broken), and **real re-run
safety** (every write is `overwrite`-mode against durable tables, not just an in-memory print).

| Task | Notebook | Depends on |
|---|---|---|
| `extract_daily_transactions` | `pipeline/01_extract_daily_transactions.py` | — |
| `validate_and_clean_transactions` | `pipeline/02_validate_and_clean_transactions.py` | `extract_daily_transactions` |
| `reconcile_with_ledger` | `pipeline/03_reconcile_with_ledger.py` | `validate_and_clean_transactions` |
| `publish_summary` | `pipeline/04_publish_summary.py` | `reconcile_with_ledger` |

**Task 2's original bug (fixed, kept for history)**: task 2 used to write the "clean"
transactions table with a Delta `CHECK (amount >= 0)` constraint — written back when this feed
only ever carried positive charges. Refunds (legitimate, negative `amount` values) were added to
the upstream feed later as a real business requirement, and this constraint was never revisited,
so every refund failed the write. **SCRUM-80 fixed this** — not by deleting the constraint
(which would've silently let genuinely bad data back in), but by narrowing its assumption:
`CHECK (txn_type = 'refund' OR amount >= 0)`. Task 2 is clean now; this is documented here as
the pipeline's history, not a currently-reproducible bug.

**Two new, independent bugs now live in tasks 3 and 4** — built to test a *second* failure
surfacing only after an *earlier* one in the same pipeline gets fixed, i.e. "fix #1, re-run,
discover #2" rather than one incident per pipeline:

- **Task 3** (`03_reconcile_with_ledger.py`): filters the reconciliation output to
  `is_active == True` — a column that was never actually added to either `txn_totals` or the
  synthetic `ledger` DataFrame. Fails with `AnalysisException: cannot resolve column 'is_active'`
  — a genuinely common real bug shape: someone adds a business-motivated filter assuming a column
  exists that was never actually threaded through upstream.
- **Task 4** (`04_publish_summary.py`): computes `F.avg(F.abs("discrepency"))` — misspelled
  (the real column is `discrepancy`). Fails with `AnalysisException: cannot resolve column
  'discrepency'`. Independent root cause from task 3's bug (a plain typo, not a missing column),
  and only reachable once task 3 is fixed and rerun — before that, task 4 never runs at all
  (reported `UPSTREAM_FAILED`).

### Setting this one up

Create **one job** with **four tasks**, each:
- Source: **Git provider**, same repo/branch as above
- Path: `pipeline/<filename>` (e.g. `pipeline/01_extract_daily_transactions.py`)
- **Depends on**: wire task 2 → depends on task 1, task 3 → depends on task 2, task 4 → depends
  on task 3 (linear chain)
- Base parameter `target_schema` (optional, defaults to `default` if unset) — set this if your
  workspace doesn't allow writes to the classic `default` (hive_metastore) database; point it at
  any Unity Catalog catalog/schema your cluster's identity can create tables in instead.

Run the job once to confirm task 2 fails and tasks 3/4 show as upstream-failed, then note the
run ID for `opsbuddy-fix`.
