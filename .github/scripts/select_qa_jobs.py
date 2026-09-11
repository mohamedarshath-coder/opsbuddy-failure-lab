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


def load_jobs(databricks_yml_path: str, repo_root: str = ".") -> dict:
    """job_name -> set of files this job actually depends on: its own notebook_path values,
    plus every real local import each of those notebooks resolves to."""
    with open(databricks_yml_path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    jobs = {}
    for job_name, job_def in (spec.get("resources", {}).get("jobs", {}) or {}).items():
        paths = set()
        for task in job_def.get("tasks", []) or []:
            notebook_task = task.get("notebook_task") or {}
            path = notebook_task.get("notebook_path")
            if path:
                path = normalize(path)
                paths.add(path)
                paths |= resolve_local_imports(path, repo_root)
        jobs[job_name] = paths
    return jobs


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

    if "databricks.yml" in files:
        print(
            "databricks.yml itself changed -- running ALL bundle jobs "
            "(structural change, not safely scoped by file identity)",
            file=sys.stderr,
        )
        selected = sorted(jobs.keys())
    else:
        changed = {normalize(f) for f in files}
        selected = sorted(
            job_name for job_name, deps in jobs.items() if deps & changed
        )

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
