"""Level 1: the source spellings used by the customer country pipeline
(notebooks/common/country_spellings.py) are complete and unambiguous, and match
the countries the check spec accepts."""

from pathlib import Path

import yaml

from notebooks.common.country_spellings import SPELLINGS

SPECS = Path(__file__).resolve().parent.parent / "specs"


def _accepted_iso_codes() -> list[str]:
    spec = yaml.safe_load((SPECS / "cc_customers.yaml").read_text(encoding="utf-8"))
    [check] = [c for c in spec["checks"] if c["id"] == "country_is_iso_code"]
    return check["values"]


def test_one_spelling_list_per_accepted_country():
    assert len(SPELLINGS) == len(_accepted_iso_codes()) == 26


def test_every_country_has_spellings_and_none_is_blank():
    for spellings in SPELLINGS:
        assert spellings, "a country has no spellings"
        for spelling in spellings:
            assert isinstance(spelling, str) and spelling.strip(), f"blank spelling in {spellings}"


def test_no_spelling_belongs_to_two_countries():
    owner: dict[str, int] = {}
    for index, spellings in enumerate(SPELLINGS):
        for spelling in spellings:
            key = spelling.strip()
            assert owner.setdefault(key, index) == index, f"{key!r} is listed for two countries"


def test_no_country_lists_the_same_spelling_twice():
    for spellings in SPELLINGS:
        keys = [s.strip() for s in spellings]
        assert len(keys) == len(set(keys)), f"duplicate spelling in {spellings}"
