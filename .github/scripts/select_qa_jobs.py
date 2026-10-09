#!/usr/bin/env python3
"""select_qa_jobs.py -- decide which databricks.yml bundle jobs test-qa actually needs to run
for THIS PR, instead of always running every job regardless of which files changed.

WHY THIS EXISTS: the first version of this dynamic-discovery step ran every bundle job on every
PR that touched pipeline/**, notebooks/**, or databricks.yml -- confirmed live as a real problem:
a PR fixing notebooks/14_malformed_numeric_udf_crash.py (not in the bundle at all) still caused
test-qa to fail, because it re-ran vip_risk_threshold_flagging (a genuinely broken, UNRELATED job
also in the bundle) and reported that failure as if it were about the actual PR's change. Running
only the jobs whose files this PR actually touched fixes that false-negative.

SELECTION RULE -- exact-file match, PLUS each job's own notebook source is actually parsed for
real local import statements, resolved to real repo files. Two earlier, narrower versions of this
rule were tried and confirmed wrong before this one shipped:

  1. Whole-directory grouping (any "notebooks/**" change selects every job with a notebook under
     "notebooks/") -- confirmed wrong against the real PR #31 scenario
     (notebooks/14_malformed_numeric_udf_crash.py changed): it still incorrectly selected
     vip_risk_threshold_flagging, reproducing the exact false failure this script exists to fix,
     because both files happen to share the same top-level "notebooks" directory despite having
     nothing to do with each other.
  2. A hardcoded "common/" folder special-case (only files under a directory literally named
     "common" count as shared dependencies) -- this correctly caught
     notebooks/common/discrepancy_rules.py, but is blind to a plain same-directory import like
     transforms_07_bronze_product_catalog.py (imported by 07_bronze_product_catalog.py, no
     "common/" involved at all) -- confirmed a real gap when bronze_product_catalog was added to
     the bundle, not hypothetical.

Real import parsing (this version) generalizes both of the above correctly and doesn't need a
new special case for the next shared-file pattern this repo invents: it reads each job's own
notebook source, extracts `import X` / `from X import ...` lines, resolves X to a candidate repo
file (dots -> slashes, + ".py"), and includes it in that job's dependency set ONLY if that file
actually exists in the repo -- which is what naturally excludes stdlib/pyspark/third-party
imports (foo.py never exists locally for those) without needing an explicit blocklist.

If databricks.yml itself changed, ALL jobs are selected unconditionally -- a structural change to
the bundle (a renamed job, a new task, a changed parameter) isn't safely scoped by file identity
at all, so this falls back to full coverage rather than guessing which jobs are actually affected.
"""

import os
import re
import subprocess
import sys

import yaml

_IMPORT_PATTERN = re.compile(
    r"^\s*(?:from\s+([\w.]+)\s+import\s+|import\s+([\w.]+))", re.MULTILINE
)


def normalize(path: str) -> str:
    """'./pipeline/00_ingest_raw_transactions.py' -> 'pipeline/00_ingest_raw_transactions.py'.
    Strips a leading './' so bundle-style paths compare equal to git's own (no './') paths."""
    return path[2:] if path.startswith("./") else path


def resolve_local_imports(notebook_path: str, repo_root: str = ".") -> set:
    """Read `notebook_path`'s real source and return the set of repo-relative .py files it
    actually imports locally -- e.g. 'from notebooks.common.discrepancy_rules import X' inside
    notebooks/11_....py resolves to {'notebooks/common/discrepancy_rules.py'} because that file
    genuinely exists in the repo; 'from pyspark.sql import functions' does NOT resolve to
    anything, because 'pyspark/sql.py' does not exist locally -- that's what filters out
    stdlib/third-party imports without an explicit blocklist. Returns an empty set (not an
    error) if the notebook file itself can't be read -- a missing/renamed file is a real problem
    but not this function's job to report."""
    full_path = os.path.join(repo_root, normalize(notebook_path))
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            source = f.read()
    except OSError:
        return set()

    resolved = set()
    for match in _IMPORT_PATTERN.finditer(source):
        module = match.group(1) or match.group(2)
        if not module:
            continue
        candidate = module.replace(".", "/") + ".py"
        if os.path.isfile(os.path.join(repo_root, candidate)):
            resolved.add(candidate)
    return resolved


# --- running order (DataSakshi: bronze -> silver -> gold) ---

_WRITE_PATTERN = re.compile(
    r"""saveAsTable\(\s*f?["']([^"']+)["']|@dlt\.(?:table|view)\(\s*name\s*=\s*f?["']([^"']+)["']"""
)
_READ_PATTERN = re.compile(
    r"""\.table\(\s*f?["']([^"']+)["']|dlt\.read(?:_stream)?\(\s*f?["']([^"']+)["']"""
)


def _names(pattern, source: str) -> set:
    return {_table_name(a or b) for a, b in pattern.findall(source)}


def _table_name(text: str) -> str:
    """'{target_schema}.cc2_orders' or 'dev.academy.customers' -> 'cc2_orders' / 'customers':
    the last name segment, so a QA and a prod schema compare equal."""
    return text.replace("}", ".").split(".")[-1].strip().lower()


def job_tables(job_def: dict, repo_root: str = ".") -> tuple:
    """(tables the job writes, tables it reads), by last name segment, from the source of
    its notebook tasks and the local modules they import. A table named through a variable
    (spark.table(feed_table)) is not seen; use the datasakshi_after tag for that."""
    writes, reads = set(), set()
    for path in resource_paths(job_def):
        files = {normalize(path)} | resolve_local_imports(path, repo_root)
        for rel in files:
            try:
                with open(os.path.join(repo_root, rel), "r", encoding="utf-8") as f:
                    source = f.read()
            except OSError:
                continue
            writes |= _names(_WRITE_PATTERN, source)
            reads |= _names(_READ_PATTERN, source)
    return writes, reads - writes


def run_order(selected: list, spec: dict, repo_root: str = ".") -> list:
    """`selected` sorted so a job runs after every selected job it depends on: one that
    writes a table it reads, or one named in its `datasakshi_after` tag (comma-separated
    job keys). Ties keep alphabetical order; a cycle keeps the remaining jobs alphabetical
    and says so."""
    defs = bundle_resources(spec)
    tables = {name: job_tables(defs.get(name) or {}, repo_root) for name in selected}
    after = {name: set() for name in selected}
    for name in selected:
        tag = str(((defs.get(name) or {}).get("tags") or {}).get("datasakshi_after") or "")
        after[name] |= {t.strip() for t in tag.split(",") if t.strip() in after and t.strip() != name}
        _, reads = tables[name]
        for other in selected:
            if other != name and tables[other][0] & reads:
                after[name].add(other)
    order, done = [], set()
    remaining = sorted(selected)
    while remaining:
        ready = [n for n in remaining if after[n] <= done]
        if not ready:
            print(f"::warning::job dependency cycle among {remaining}; running them alphabetically",
                  file=sys.stderr)
            ready = remaining[:1]
        order.append(ready[0])
        done.add(ready[0])
        remaining.remove(ready[0])
    for name in order:
        if after[name]:
            print(f"  {name} runs after {sorted(after[name])}", file=sys.stderr)
    return order


def resource_paths(resource: dict) -> list:
    """The source files a job or a DLT pipeline runs: each job task's notebook_path, or
    each pipeline library's notebook/file path."""
    paths = []
    for task in resource.get("tasks", []) or []:
        path = (task.get("notebook_task") or {}).get("notebook_path")
        if path:
            paths.append(path)
    for library in resource.get("libraries", []) or []:
        for kind in ("notebook", "file"):
            path = (library.get(kind) or {}).get("path")
            if path:
                paths.append(path)
    return paths


def bundle_resources(spec: dict) -> dict:
    """Every runnable bundle resource by key: jobs and DLT pipelines (both run with
    `databricks bundle run <key>`)."""
    resources = spec.get("resources", {}) or {}
    out = dict(resources.get("jobs", {}) or {})
    for key, pipeline in (resources.get("pipelines", {}) or {}).items():
        out.setdefault(key, pipeline)
    return out


def load_jobs(databricks_yml_path: str, repo_root: str = ".") -> dict:
    """job or pipeline key -> set of files it actually depends on: its own notebook and
    library paths, plus every real local import each of those resolves to."""
    with open(databricks_yml_path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    jobs = {}
    for job_name, job_def in bundle_resources(spec).items():
        paths = set()
        for path in resource_paths(job_def):
            path = normalize(path)
            paths.add(path)
            paths |= resolve_local_imports(path, repo_root)
        jobs[job_name] = paths
    return jobs


def yml_at(ref: str) -> dict | None:
    """databricks.yml as it was at `ref` (None if it did not exist or cannot be read)."""
    result = subprocess.run(
        ["git", "show", f"{ref}:databricks.yml"], capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    try:
        return yaml.safe_load(result.stdout) or {}
    except yaml.YAMLError:
        return None


def jobs_changed_in_yml(base_ref: str, head_ref: str):
    """When databricks.yml changed: the set of jobs whose own definition is new or different,
    or None when anything outside resources.jobs changed (variables, targets, bundle, includes),
    which may affect every job and so needs full coverage. Comparing parsed YAML means comments
    and formatting never select a job."""
    before, after = yml_at(base_ref), yml_at(head_ref)
    if before is None or after is None:
        return None
    names = changed_variables(before, after)
    if names is None:
        return None
    old = bundle_resources(before)
    new = bundle_resources(after)
    changed = {name for name, job in new.items() if old.get(name) != job}
    # A job that reads a changed variable runs differently even if its own definition did
    # not change; a newly added variable is read by no existing job, so it selects none.
    for name, job in new.items():
        text = yaml.safe_dump(job)
        if any("${var." + var + "}" in text for var in names):
            changed.add(name)
    return changed


def changed_variables(before: dict, after: dict):
    """The names of bundle variables whose value differs (top-level `variables` or any
    `targets.<t>.variables`), or None when anything else outside resources.jobs changed
    (bundle, include, workspace, other resources, a target's other settings), which may
    affect every job and so still needs full coverage."""
    def split(spec):
        rest = dict(spec)
        resources = dict(rest.pop("resources", {}) or {})
        resources.pop("jobs", None)
        resources.pop("pipelines", None)
        variables = rest.pop("variables", {}) or {}
        targets, target_vars = {}, {}
        for tname, target in (rest.pop("targets", {}) or {}).items():
            target = dict(target or {})
            target_vars[tname] = target.pop("variables", {}) or {}
            targets[tname] = target
        return rest, resources, targets, variables, target_vars
    b_rest, b_res, b_targets, b_vars, b_tvars = split(before)
    a_rest, a_res, a_targets, a_vars, a_tvars = split(after)
    if (b_rest, b_res, b_targets) != (a_rest, a_res, a_targets):
        return None
    names = {k for k in set(b_vars) | set(a_vars) if b_vars.get(k) != a_vars.get(k)}
    for tname in set(b_tvars) | set(a_tvars):
        bv, av = b_tvars.get(tname, {}), a_tvars.get(tname, {})
        names |= {k for k in set(bv) | set(av) if bv.get(k) != av.get(k)}
    return names


def changed_files(base_ref: str, head_ref: str) -> list:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_ref}...{head_ref}"],
        capture_output=True, text=True, check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def main():
    if len(sys.argv) != 3:
        print("usage: select_qa_jobs.py <base_ref> <head_ref>", file=sys.stderr)
        sys.exit(2)
    base_ref, head_ref = sys.argv[1], sys.argv[2]

    jobs = load_jobs("databricks.yml")
    if not jobs:
        print("::error::No jobs found under resources.jobs in databricks.yml", file=sys.stderr)
        sys.exit(1)
    for job_name, deps in jobs.items():
        print(f"  {job_name} depends on: {sorted(deps)}", file=sys.stderr)

    files = changed_files(base_ref, head_ref)
    print(f"Changed files: {files}", file=sys.stderr)

    changed = {normalize(f) for f in files}
    by_files = {job_name for job_name, deps in jobs.items() if deps & changed}
    if "databricks.yml" in files:
        by_yml = jobs_changed_in_yml(base_ref, head_ref)
        if by_yml is None:
            print(
                "databricks.yml changed outside resources.jobs and variables -- running ALL "
                "bundle jobs "
                "(structural change, not safely scoped by file identity)",
                file=sys.stderr,
            )
            selected = sorted(jobs.keys())
        else:
            print(f"databricks.yml changed these job definitions: {sorted(by_yml)}",
                  file=sys.stderr)
            selected = sorted((by_files | by_yml) & set(jobs))
    else:
        selected = sorted(by_files)

    if len(selected) > 1:
        with open("databricks.yml", "r", encoding="utf-8") as f:
            selected = run_order(selected, yaml.safe_load(f) or {})

    if not selected:
        print(
            "No bundle job depends on any changed file -- nothing relevant to qa-test for "
            "this PR (e.g. a docs-only or genuinely unrelated notebooks/ file change).",
            file=sys.stderr,
        )
    else:
        print(f"Selected jobs for qa: {selected}", file=sys.stderr)

    # stdout: one job name per line, meant to be captured by the calling workflow step.
    for job_name in selected:
        print(job_name)


if __name__ == "__main__":
    main()
