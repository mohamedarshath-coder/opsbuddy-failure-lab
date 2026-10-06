"""Unit test guard for the DataSakshi agent's push (DataSakshi D103).

Runs in agent.yml after the agent's code stage and before the push, always
taken from main. It does not trust the agent's own report: it checks the
commits between START_SHA and HEAD and runs the unit tests itself.

The push is refused when:
- any unit test fails or errors (pytest is run here, with CI=true, so Spark
  tests cannot be skipped);
- an existing test file under tests/ was deleted;
- a test was switched off (skip, skipif, xfail added under tests/);
- a logic module changed (a .py file that is not a Databricks notebook, not a
  test and not under .github/) without its tests/test_<module>.py being added
  or changed in the same commits, unless the agent gave tests.exempt_reason.

A repo without tests/ is not set up for unit tests yet: that is recorded,
not blocked.

Writes OUT_DIR/unit_tests.json for DataSakshi and exits non-zero to stop the push.
Environment: START_SHA, OUT_DIR. `--dry-run` prints the result without pytest.
"""

import base64
import gzip
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

NOTEBOOK_HEADER = "# Databricks notebook source"
SWITCHED_OFF = re.compile(r"pytest\.mark\.(skip|skipif|xfail)\b|pytest\.(skip|xfail)\(")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def changed_files(start: str) -> list[tuple[str, str, str | None]]:
    """(status letter, path, old path for a rename) between start and HEAD."""
    rows = []
    for line in git("diff", "--name-status", "-M", start, "HEAD").splitlines():
        parts = line.split("\t")
        status = parts[0][0]
        if status == "R":
            rows.append((status, parts[2], parts[1]))
        else:
            rows.append((status, parts[1], None))
    return rows


def is_notebook(path: str) -> bool:
    try:
        with open(path, encoding="utf-8") as f:
            return f.readline().strip() == NOTEBOOK_HEADER
    except OSError:
        return False


def is_logic_module(path: str) -> bool:
    return (
        path.endswith(".py")
        and not path.startswith(("tests/", ".github/"))
        and Path(path).name != "conftest.py"
        and not is_notebook(path)
    )


def agent_exempt_reason(out_dir: Path) -> str | None:
    """The agent's own tests.exempt_reason, from the monitor's report."""
    try:
        raw = gzip.decompress(base64.b64decode((out_dir / "report.b64").read_text()))
        result = json.loads(raw).get("result") or {}
        reason = ((result.get("structured_output") or {}).get("tests") or {}).get("exempt_reason")
    except (OSError, ValueError):
        return None
    return reason.strip() if isinstance(reason, str) and reason.strip() else None


def check_changes(start: str, out_dir: Path) -> dict:
    rows = changed_files(start)
    touched_tests = {p for s, p, _ in rows if p.startswith("tests/") and s in "AMR"}
    deleted = [p for s, p, _ in rows if s == "D" and p.startswith("tests/") and p.endswith(".py")]
    deleted += [old for s, p, old in rows if s == "R" and old.startswith("tests/") and not p.startswith("tests/")]
    added_lines = [
        line[1:]
        for line in git("diff", "-U0", start, "HEAD", "--", "tests").splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]
    switched_off = [line.strip() for line in added_lines if SWITCHED_OFF.search(line)]
    modules = sorted(p for s, p, _ in rows if s in "AMR" and is_logic_module(p))
    untested = [m for m in modules if f"tests/test_{Path(m).stem}.py" not in touched_tests]
    exempt = agent_exempt_reason(out_dir) if untested else None
    return {
        "changed_modules": modules,
        "test_files_changed": sorted(touched_tests),
        "deleted_tests": deleted,
        "skips_added": switched_off,
        "untested_modules": untested,
        "exempt_reason": exempt,
    }


def run_pytest(out_dir: Path) -> dict:
    junit = out_dir / "junit.xml"
    env = {**os.environ, "CI": "true"}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"--junitxml={junit}"],
        capture_output=True,
        text=True,
        env=env,
    )
    counts = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    try:
        root = ET.parse(junit).getroot()
        for suite in [root] if root.tag == "testsuite" else list(root):
            for key in counts:
                counts[key] += int(suite.get(key, 0))
    except (OSError, ET.ParseError):
        pass
    failed = counts["failures"] + counts["errors"]
    return {
        "exit_code": proc.returncode,
        "passed": counts["tests"] - failed - counts["skipped"],
        "failed": failed,
        "skipped": counts["skipped"],
        "output_tail": (proc.stdout + proc.stderr)[-3000:],
    }


def main() -> int:
    out_dir = Path(os.environ["OUT_DIR"])
    start = os.environ["START_SHA"]
    if not re.fullmatch(r"[0-9a-f]{40}", start):
        raise SystemExit("START_SHA must be a full commit SHA")
    result: dict = {"guard": "unit_test_guard", "version": 1}
    if not Path("tests").is_dir():
        result.update(status="not_set_up", detail="the repo has no tests/ folder yet")
    else:
        result.update(check_changes(start, out_dir))
        problems = []
        if result["deleted_tests"]:
            problems.append(f"test files deleted: {', '.join(result['deleted_tests'])}")
        if result["skips_added"]:
            problems.append(f"tests switched off: {'; '.join(result['skips_added'])}")
        if result["untested_modules"] and not result["exempt_reason"]:
            problems.append(
                "logic changed without its unit tests: "
                + ", ".join(f"{m} (expected tests/test_{Path(m).stem}.py)" for m in result["untested_modules"])
            )
        if "--dry-run" not in sys.argv:
            result["pytest"] = run_pytest(out_dir)
            p = result["pytest"]
            if p["exit_code"] != 0 or p["failed"]:
                problems.append(f"unit tests failed: {p['failed']} failed, {p['passed']} passed")
            elif p["passed"] == 0:
                problems.append("no unit test ran")
        result["problems"] = problems
        result["status"] = "failed" if problems else "passed"
        result["detail"] = "; ".join(problems) or "unit tests passed"
    (out_dir / "unit_tests.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "pytest"}, indent=2))
    if result.get("pytest"):
        print(result["pytest"]["output_tail"])
    if result["status"] == "failed":
        print(f"::error::unit test guard: {result['detail']}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
