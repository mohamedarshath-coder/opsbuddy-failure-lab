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
