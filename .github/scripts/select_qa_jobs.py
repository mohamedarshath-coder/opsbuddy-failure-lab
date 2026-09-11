#!/usr/bin/env python3
"""select_qa_jobs.py -- decide which databricks.yml bundle jobs test-qa actually needs to run
for THIS PR, instead of always running every job regardless of which files changed.

WHY THIS EXISTS: the first version of this dynamic-discovery step ran every bundle job on every
PR that touched pipeline/**, notebooks/**, or databricks.yml -- confirmed live as a real problem:
a PR fixing notebooks/14_malformed_numeric_udf_crash.py (not in the bundle at all) still caused
test-qa to fail, because it re-ran vip_risk_threshold_flagging (a genuinely broken, UNRELATED job
also in the bundle) and reported that failure as if it were about the actual PR's change. Running
only the jobs whose files this PR actually touched fixes that false-negative.

SELECTION RULE -- exact-file match, plus one specific rule for shared modules, NOT whole-
directory grouping. An earlier version of this script grouped by top-level directory (e.g. any
"notebooks/**" change selects every job with a notebook under "notebooks/") -- confirmed WRONG
before it ever shipped: tested directly against the real PR #31 scenario
(notebooks/14_malformed_numeric_udf_crash.py changed) and it still incorrectly selected
vip_risk_threshold_flagging, reproducing the exact false failure this script exists to fix,
because both files happen to share the same top-level "notebooks" directory despite having
nothing to do with each other. The actual rule needed is narrower:

  1. Exact match -- the changed file IS one of the job's own notebook_path values.
  2. Shared-module match -- the changed file lives under a directory literally named "common"
     (e.g. "notebooks/common/discrepancy_rules.py"), AND that "common" directory shares the same
     top-level parent as the job's own notebook_path (e.g. both under "notebooks/"). This is
     deliberately narrow to the "common/" convention this repo already uses for shared modules
     (see notebooks/common/discrepancy_rules.py, imported by
     notebooks/11_shared_module_bug_vip_risk_threshold.py) -- not "same top-level directory" in
     general, which is what caused the false positive above.

If databricks.yml itself changed, ALL jobs are selected unconditionally -- a structural change to
the bundle (a renamed job, a new task, a changed parameter) isn't safely scoped by file identity
at all, so this falls back to full coverage rather than guessing which jobs are actually affected.
"""

import subprocess
import sys

import yaml


def normalize(path: str) -> str:
    """'./pipeline/00_ingest_raw_transactions.py' -> 'pipeline/00_ingest_raw_transactions.py'.
    Strips a leading './' so bundle-style paths compare equal to git's own (no './') paths."""
    return path[2:] if path.startswith("./") else path


def top_level_dir(path: str) -> str:
    parts = normalize(path).split("/")
    return parts[0] if parts else path


def is_under_common(path: str) -> bool:
    """True for any path with a directory literally named 'common' in it, e.g.
    'notebooks/common/discrepancy_rules.py' -- this repo's actual convention for a shared module
    imported by more than one notebook (see notebooks/common/discrepancy_rules.py, imported by
    notebooks/11_shared_module_bug_vip_risk_threshold.py). Deliberately specific to this naming
    convention, not "anything else in the same top-level folder" -- see the module docstring for
    why that broader rule was tried and confirmed wrong."""
    return "common" in normalize(path).split("/")[:-1]


def load_jobs(databricks_yml_path: str) -> dict:
    """job_name -> set of that job's own notebook_path values (normalized, exact paths -- not
    directories)."""
    with open(databricks_yml_path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    jobs = {}
    for job_name, job_def in (spec.get("resources", {}).get("jobs", {}) or {}).items():
        paths = set()
        for task in job_def.get("tasks", []) or []:
            notebook_task = task.get("notebook_task") or {}
            path = notebook_task.get("notebook_path")
            if path:
                paths.add(normalize(path))
        jobs[job_name] = paths
    return jobs


def job_is_affected(job_notebook_paths: set, changed_file: str) -> bool:
    """Rule 1: exact match. Rule 2: changed_file is a shared 'common' module under the same
    top-level directory as one of this job's own notebooks."""
    changed_file = normalize(changed_file)
    if changed_file in job_notebook_paths:
        return True
    if is_under_common(changed_file):
        changed_top = top_level_dir(changed_file)
        if any(top_level_dir(p) == changed_top for p in job_notebook_paths):
            return True
    return False


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

    files = changed_files(base_ref, head_ref)
    print(f"Changed files: {files}", file=sys.stderr)

    if "databricks.yml" in files:
        print(
            "databricks.yml itself changed -- running ALL bundle jobs "
            "(structural change, not safely scoped by directory)",
            file=sys.stderr,
        )
        selected = sorted(jobs.keys())
    else:
        selected = sorted(
            job_name
            for job_name, notebook_paths in jobs.items()
            if any(job_is_affected(notebook_paths, f) for f in files)
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
