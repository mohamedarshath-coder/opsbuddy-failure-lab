"""Level 1: every check spec in specs/ is well formed, so a broken spec is caught
on GitHub before DataSakshi tries to run it against Databricks."""

import re
from pathlib import Path

import pytest
import yaml

SPECS = sorted((Path(__file__).resolve().parent.parent / "specs").glob("*.yaml"))
NAME = re.compile(r"^[A-Za-z0-9_]+$")


def test_there_are_specs():
    assert SPECS


@pytest.mark.parametrize("path", SPECS, ids=lambda p: p.name)
def test_spec_is_well_formed(path):
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    parts = str(spec["table"]).split(".")
    assert len(parts) == 3 and all(NAME.match(p) for p in parts), "table is catalog.schema.table"
    ids = [check["id"] for check in spec["checks"]]
    assert len(ids) == len(set(ids)), "check ids are unique"
    for check in spec["checks"]:
        assert check.get("type"), f"{check['id']} has a type"
    missing = set(spec.get("required", [])) - set(ids)
    assert not missing, f"required names checks that do not exist: {sorted(missing)}"
