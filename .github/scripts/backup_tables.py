"""Pre-deploy table copies for DataSakshi rollbacks (DataSakshi D93).

Run by deploy-prod before the affected jobs. For every governed table (the
`table:` of each check spec in specs/), it takes a Delta DEEP CLONE into
<catalog>.<BACKUP_SCHEMA>.<schema>__<table>__<merge sha[:12]>. A deep clone has
its own data files, so a rollback can restore from it long after Delta's file
retention (7 days by default) has made time travel impossible. DataSakshi finds
the copy by that name and checks its history (CLONE of the same table at the
version to restore) before using it.

A table that does not exist yet (created by this release) is skipped. Copies
older than KEEP_DAYS are dropped. Any other failure fails the deploy before the
jobs run, so a release never changes data without its copy.

Environment: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
BACKUP_SCHEMA, MERGE_SHA, optional CATALOG_MAP ("logical=real,..."), KEEP_DAYS.
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
    if not tables:
        print("No governed tables (no check specs): nothing to copy.")
        return
    for catalog in sorted({t.split(".")[0] for t in tables}):
        sql.run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    for table in tables:
        catalog, table_schema, name = table.split(".")
        exists = sql.run(f"SHOW TABLES IN {catalog}.{table_schema} LIKE '{name}'")
        if not dry_run and not exists:
            print(f"{table} does not exist yet (this release creates it): no copy.")
            continue
        sql.run(f"CREATE OR REPLACE TABLE {backup_name(table, schema, sha)} DEEP CLONE {table}")
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
