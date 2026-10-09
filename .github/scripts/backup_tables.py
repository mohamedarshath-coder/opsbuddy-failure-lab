"""Pre-deploy table copies for DataSakshi rollbacks (DataSakshi D93).

Run by deploy-prod before the affected jobs. For every governed table (the
`table:` of each check spec in specs/), it takes a Delta DEEP CLONE into
<catalog>.<BACKUP_SCHEMA>.<schema>__<table>__<merge sha[:12]>. A deep clone has
its own data files, so a rollback can restore from it long after Delta's file
retention (7 days by default) has made time travel impossible. DataSakshi finds
the copy by that name and checks its history (CLONE of the same table at the
version to restore) before using it.

Only the tables this release's jobs write are copied when JOBS (the selected
jobs, one per line) is set and every one of those jobs' written tables can be
read from its code; otherwise every governed table is, as before. A read-only
source is not copied: the release does not change it. A DLT materialized view
or streaming table cannot be deep cloned, so it gets a plain snapshot copy
(CREATE TABLE ... AS SELECT); a view holds no data and is skipped.

A table that does not exist yet (created by this release) is skipped. Copies
older than KEEP_DAYS are dropped. Any other failure fails the deploy before the
jobs run, so a release never changes data without its copy.

Environment: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
BACKUP_SCHEMA, MERGE_SHA, optional CATALOG_MAP ("logical=real,..."), KEEP_DAYS, JOBS.
`--dry-run` prints the statements instead of running them.
"""

import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

import yaml

NAME = re.compile(r"^[A-Za-z0-9_]+$")
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


def governed_tables(spec_dir: str, catalog_map: dict[str, str]) -> list[str]:
    tables = set()
    for path in sorted(glob.glob(os.path.join(spec_dir, "*.y*ml"))):
        with open(path, encoding="utf-8") as f:
            spec = yaml.safe_load(f) or {}
        table = str(spec.get("table", ""))
        parts = table.split(".")
        if len(parts) != 3 or not all(NAME.match(p) for p in parts):
            raise SystemExit(f"{path}: table {table!r} is not catalog.schema.table")
        parts[0] = catalog_map.get(parts[0], parts[0])
        tables.add(".".join(parts).lower())
    return sorted(tables)


def backup_name(table: str, schema: str, sha: str) -> str:
    catalog, table_schema, name = table.split(".")
    return f"{catalog}.{schema.lower()}.{table_schema}__{name}__{sha[:12].lower()}"


class Sql:
    def __init__(self, host: str, token: str, warehouse: str, dry_run: bool):
        self.host, self.token, self.warehouse, self.dry_run = host.rstrip("/"), token, warehouse, dry_run

    def _call(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(
            self.host + path,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} {path}: HTTP {exc.code} {exc.read().decode()[:500]}")

    def run(self, statement: str) -> list[list]:
        print(f"SQL: {statement}", flush=True)
        if self.dry_run:
            return []
        result = self._call(
            "POST",
            "/api/2.0/sql/statements",
            {"warehouse_id": self.warehouse, "statement": statement, "wait_timeout": "30s"},
        )
        deadline = time.time() + 1800
        while result["status"]["state"] not in TERMINAL and time.time() < deadline:
            time.sleep(5)
            result = self._call("GET", f"/api/2.0/sql/statements/{result['statement_id']}")
        state = result["status"]["state"]
        if state != "SUCCEEDED":
            error = result["status"].get("error", {}).get("message", "")
            raise SystemExit(f"statement ended {state}: {error}")
        return (result.get("result") or {}).get("data_array") or []


def written_by(jobs: list[str], yml: str = "databricks.yml") -> set[str] | None:
    """Table names (last segment) the given jobs write, or None when any of them has no
    write the code shows (e.g. a table name in a variable): then copy everything."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import select_qa_jobs  # the same reading of the jobs' code as the job selection

    with open(yml, "r", encoding="utf-8") as f:
        defs = select_qa_jobs.bundle_resources(yaml.safe_load(f) or {})
    names: set[str] = set()
    for job in jobs:
        writes, _ = select_qa_jobs.job_tables(defs.get(job) or {})
        if not writes:
            print(f"{job}: its written tables are not visible in its code; copying every governed table.")
            return None
        names |= writes
    return names


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    env = os.environ
    schema = env["BACKUP_SCHEMA"]
    sha = env["MERGE_SHA"]
    keep_days = int(env.get("KEEP_DAYS", "30"))
    if not NAME.match(schema) or not re.match(r"^[0-9a-f]{40}$", sha):
        raise SystemExit("BACKUP_SCHEMA must be a plain name and MERGE_SHA a full commit SHA")
    catalog_map = dict(
        pair.split("=", 1) for pair in env.get("CATALOG_MAP", "").split(",") if "=" in pair
    )
    sql = Sql(
        env.get("DATABRICKS_HOST", "https://example"),
        env.get("DATABRICKS_TOKEN", ""),
        env.get("DATABRICKS_WAREHOUSE_ID", "dry-run"),
        dry_run,
    )
    tables = governed_tables("specs", catalog_map)
    jobs = [j.strip() for j in env.get("JOBS", "").splitlines() if j.strip()]
    written = written_by(jobs) if jobs else None
    if written is not None:
        skipped = [t for t in tables if t.split(".")[-1].lower() not in written]
        tables = [t for t in tables if t.split(".")[-1].lower() in written]
        if skipped:
            print(f"Not changed by this release, so not copied: {', '.join(skipped)}")
    if not tables:
        print("No governed table is written by this release: nothing to copy.")
        return
    for catalog in sorted({t.split(".")[0] for t in tables}):
        sql.run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    for table in tables:
        catalog, table_schema, name = table.split(".")
        found = sql.run(
            f"SELECT table_type FROM {catalog}.information_schema.tables "
            f"WHERE lower(table_schema) = '{table_schema.lower()}' "
            f"AND lower(table_name) = '{name.lower()}'"
        )
        if not dry_run and not found:
            print(f"{table} does not exist yet (this release creates it): no copy.")
            continue
        kind = str(found[0][0]).upper() if found else "MANAGED"
        if kind == "VIEW":
            print(f"{table} is a view (no data of its own): no copy.")
            continue
        target = backup_name(table, schema, sha)
        if kind in ("MATERIALIZED_VIEW", "STREAMING_TABLE"):
            # DLT tables cannot be deep cloned; a snapshot keeps the data for a rollback.
            sql.run(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {table}")
            print(f"Copied {table} ({kind.lower()}, snapshot)", flush=True)
        else:
            sql.run(f"CREATE OR REPLACE TABLE {target} DEEP CLONE {table}")
            print(f"Copied {table}", flush=True)
    for catalog in sorted({t.split(".")[0] for t in tables}):
        old = sql.run(
            f"SELECT table_name FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema.lower()}' "
            f"AND created < current_timestamp() - INTERVAL {keep_days} DAYS"
        )
        for (name,) in old:
            if NAME.match(name or ""):
                sql.run(f"DROP TABLE IF EXISTS {catalog}.{schema}.{name}")


if __name__ == "__main__":
    main()
