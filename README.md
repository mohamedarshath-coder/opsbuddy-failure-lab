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

## `notebooks/11_shared_module_bug_vip_risk_threshold.py` — a genuine multi-file root cause

Unlike notebooks 1–10, this one is **not** self-contained — it imports
`notebooks/common/discrepancy_rules.py`, a small shared module. That's the point: it tests
whether a fix lands as a single-file patch to whichever file happens to appear in the stack
trace, or as the multi-file change the root cause actually requires.

**What actually breaks**: the notebook builds a synthetic reconciliation snapshot with both
`standard` and `vip` tier accounts, flags high-risk ones via the shared `is_high_risk()`
function, then asserts a standing business invariant — no VIP account should ever get flagged by
the standard threshold, since larger discrepancies are normal for VIP accounts (support
escalated a false-positive VIP flag once before). `is_high_risk()` only ever applies one
hardcoded $100 threshold with no way to vary it, so a VIP account with a $150 discrepancy trips
it anyway. Fails with a plain `AssertionError`, not an `AnalysisException` — the code runs fine,
the *business logic* is wrong.

**Why the fix genuinely needs two files, not one:**
- `notebooks/common/discrepancy_rules.py` — `is_high_risk(discrepancy)` has no `threshold`
  parameter at all; it must gain one (e.g. `is_high_risk(discrepancy, threshold=100.0)`) before
  a caller can possibly ask for anything other than the hardcoded default.
- `notebooks/11_shared_module_bug_vip_risk_threshold.py` — even after the shared function can
  accept a threshold, the notebook still calls it uniformly with no tier awareness at all; it
  must be updated to pass a higher threshold (e.g. `500.0`) for `tier == "vip"` rows.

Neither change alone resolves the incident: extending the shared function without updating the
caller changes nothing (nobody passes the new argument), and the caller can't pass a threshold
the function doesn't yet accept. A correct fix's `AFFECTED_FILES` should name both.

**Setting this one up**: same as notebooks 1–10 — its own job, one task, Git provider source,
notebook path `notebooks/11_shared_module_bug_vip_risk_threshold.py`. No `target_schema`
parameter needed (it writes to the fixed `default.high_risk_accounts_demo` table). Run once to
confirm the `AssertionError`, then note the run ID for `opsbuddy-fix`.

## `notebooks/12_multiline_bug_rolling_revenue_metrics.py` — the same mistake repeated on multiple lines

Different dimension from notebook 11: not multiple *files*, multiple *lines* within one file, all
carrying the identical mistake. Tests whether a fix catches every occurrence, or just the one
line the stack trace happens to point at.

**What actually breaks**: the notebook renames `txn_date` to `transaction_date` early on (a
real, business-motivated standardization to match the rest of the reporting layer's schema), then
computes three related rolling-window metrics (`rolling_revenue`, `rolling_order_count`,
`rolling_avg_order_value`) — each with its own separately-written `Window.partitionBy("store_id")
.orderBy("txn_date")`, still referencing the pre-rename column name. Execution stops at the
*first* one (`rolling_revenue`) with `UNRESOLVED_COLUMN.WITH_SUGGESTION: txn_date`, so the stack
trace only ever points at that one line — but the identical `orderBy("txn_date")` mistake is
also sitting, un-triggered, in the other two `.withColumn` blocks further down the same
expression.

**Why this is a real test of fix completeness, not just fix correctness**: a fix that patches
only the line the exception names (the first `orderBy("txn_date")`) is enough to make *this run*
succeed — the notebook would go on to fail on `rolling_order_count`'s identical bug the next time
it runs, i.e. exactly the "fix #1, re-run, discover #2" shape from `pipeline/`, except here it's
the *same* line of code copy-pasted three times, not three independent bugs. A complete fix finds
and corrects all three `orderBy("txn_date")` occurrences in one pass, because a root-cause
analysis that actually reads the whole file (not just the line the traceback names) should
recognize the same mistake repeated, not just the one instance that happened to execute first.

**Setting this one up**: same pattern as notebooks 1–11 — its own job, one task, Git provider
source, notebook path `notebooks/12_multiline_bug_rolling_revenue_metrics.py`. No `target_schema`
parameter needed (synthetic data only, no table writes). Run once to confirm the
`UNRESOLVED_COLUMN.WITH_SUGGESTION: txn_date` failure, then note the run ID for `opsbuddy-fix`.

## `pipeline/` — a real multi-task job with a genuine upstream failure

Unlike the 10 standalone notebooks above, this is **two separate jobs** mirroring a real
production shape: an upstream ingestion job owned by a different team, and a four-task
reconciliation job that reads what it lands — testing three things the standalone notebooks
can't: a genuine **cross-job data lineage lead** (the reconciliation job's failures trace back,
one hop upstream, to a table a *different* job produced — not just tables within the same job), a
genuine **Upstream Task Dependency Failure** (when one task fails, its downstream tasks never run
and are reported as upstream-failed, not independently broken), and **real re-run safety** (every
write is `overwrite`-mode against durable tables, not just an in-memory print).

**Job A — `daily_txn_ingestion`** (separate job, one task):

| Task | Notebook | Depends on |
|---|---|---|
| `ingest_raw_transactions` | `pipeline/00_ingest_raw_transactions.py` | — |

Owned, in the real-world analogy, by the upstream payments-ingestion team — it lands
`daily_txn_raw` on its own schedule. The reconciliation job below only ever reads this table; it
never writes to it.

**Job B — `daily_revenue_reconciliation`** (four dependent tasks):

| Task | Notebook | Depends on |
|---|---|---|
| `extract_daily_transactions` | `pipeline/01_extract_daily_transactions.py` | — |
| `validate_and_clean_transactions` | `pipeline/02_validate_and_clean_transactions.py` | `extract_daily_transactions` |
| `reconcile_with_ledger` | `pipeline/03_reconcile_with_ledger.py` | `validate_and_clean_transactions` |
| `publish_summary` | `pipeline/04_publish_summary.py` | `reconcile_with_ledger` |

`extract_daily_transactions` reads `daily_txn_raw` (Job A's output) and writes today's slice to
`daily_txn_extract`, which `validate_and_clean_transactions` reads in turn — so
`get_table_lineage`'s `upstream_producers`, walked one hop back from anything task 1 touches,
correctly names **Job A**, a different job, not this one. `reconcile_with_ledger` also reads
`daily_txn_raw` directly (a real row-count sanity check, see below) before its own bug, so that
same cross-job lineage lead surfaces even on the task 3 incident below — not only on a
successful task 1 run.

**Task 2's original bug (fixed, kept for history)**: task 2 used to write the "clean"
transactions table with a Delta `CHECK (amount >= 0)` constraint — written back when this feed
only ever carried positive charges. Refunds (legitimate, negative `amount` values) were added to
the upstream feed later as a real business requirement, and this constraint was never revisited,
so every refund failed the write. **SCRUM-80 fixed this** — not by deleting the constraint
(which would've silently let genuinely bad data back in), but by narrowing its assumption:
`CHECK (txn_type = 'refund' OR amount >= 0)`. Task 2 is clean now; this is documented here as
the pipeline's history, not a currently-reproducible bug.

**Task 3 status**: `03_reconcile_with_ledger.py`'s `is_active` filter was originally designed as
a bug (the column missing from the synthetic `ledger` DataFrame) — that's since been corrected in
the actual code (`is_active` is properly added to `ledger` and survives the join), so task 3 runs
clean now. Left documented here as history, not a currently-reproducible bug — don't expect
`AnalysisException: cannot resolve column 'is_active'` from a fresh run.

**Task 4** (`04_publish_summary.py`): computes `F.avg(F.abs("discrepency"))` — misspelled (the
real column is `discrepancy`). Fails with `AnalysisException: cannot resolve column
'discrepency'`. With task 3 clean, this is the pipeline's one remaining independent bug, reached
directly (no upstream failure blocking it).

### Setting this one up

Create **two jobs**. Every task in both, same settings:
- Source: **Git provider**, same repo/branch as above
- Path: `pipeline/<filename>` (e.g. `pipeline/01_extract_daily_transactions.py`)
- Base parameter `target_schema` (optional, defaults to `default` if unset) — set this if your
  workspace doesn't allow writes to the classic `default` (hive_metastore) database; point it at
  any Unity Catalog catalog/schema your cluster's identity can create tables in instead (use the
  **same** value on both jobs — Job B reads tables Job A writes).

**Job A — `daily_txn_ingestion`**: one task, `ingest_raw_transactions` →
`pipeline/00_ingest_raw_transactions.py`, no dependencies.

**Job B — `daily_revenue_reconciliation`**: four tasks, `extract_daily_transactions` →
`validate_and_clean_transactions` → `reconcile_with_ledger` → `publish_summary`, each depending
on the one before it (linear chain).

Run Job A once first (it needs to have landed `daily_txn_raw` before Job B's task 1 can read it),
then run Job B. With task 2's original bug already fixed (see above) and task 3 clean, tasks 1-3
succeed and task 4 fails on the `discrepency` typo. Note Job B's run ID for `opsbuddy-fix`.
