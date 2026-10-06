"""Level 1: country standardization in notebooks/common/country_iso.py (SCRUM-135).

Acceptance criteria:
1. Every source spelling becomes the ISO 3166-1 alpha-3 code of its country, with
   no leading or trailing spaces.
2. One code per country, so the revenue report has one row per country, and the
   codes are exactly the ones the check spec accepts.
Unknown or blank spellings must fail loudly, never pass through or become null."""

from pathlib import Path

import pytest
import yaml

from notebooks.common.country_iso import ISO3, to_iso3
from notebooks.common.country_spellings import SPELLINGS

SPECS = Path(__file__).resolve().parent.parent / "specs"


def _accepted(table: str) -> list[str]:
    spec = yaml.safe_load((SPECS / f"{table}.yaml").read_text(encoding="utf-8"))
    [check] = [c for c in spec["checks"] if c["id"] == "country_is_iso_code"]
    return check["values"]


# --- criterion 1: every spelling becomes its ISO code ---


def test_every_source_spelling_maps_to_its_countrys_code():
    for code, spellings in zip(ISO3, SPELLINGS):
        for spelling in spellings:
            assert to_iso3(spelling) == code, f"{spelling!r} should map to {code}"


@pytest.mark.parametrize(
    ("spelling", "code"),
    [
        ("USA", "USA"),
        ("U.S.A.", "USA"),
        ("America", "USA"),
        ("united states of america", "USA"),
        ("England", "GBR"),
        ("Great Britain", "GBR"),
        ("Deutschland", "DEU"),
        ("Holland", "NLD"),
        ("Bharat", "IND"),
        ("KSA", "SAU"),
        ("Korea, Republic of", "KOR"),
        ("U.A.E.", "ARE"),
        ("Aus", "AUS"),
    ],
)
def test_abbreviations_local_and_old_names_map(spelling, code):
    assert to_iso3(spelling) == code


@pytest.mark.parametrize(("spelling", "code"), [(" us ", "USA"), ("canada ", "CAN"), ("\tUK\n", "GBR")])
def test_leading_and_trailing_spaces_are_ignored(spelling, code):
    assert to_iso3(spelling) == code


def test_codes_have_no_spaces_and_are_three_upper_case_letters():
    for code in {to_iso3(s) for spellings in SPELLINGS for s in spellings}:
        assert len(code) == 3 and code.isalpha() and code.isupper(), code


# --- criterion 2: one code per country, the codes the check spec accepts ---


def test_one_code_per_country():
    assert len(ISO3) == len(set(ISO3)) == len(SPELLINGS) == 26


@pytest.mark.parametrize("table", ["cc_customers", "cc_orders", "cc_revenue_by_country"])
def test_codes_are_exactly_the_ones_the_check_spec_accepts(table):
    assert set(ISO3) == set(_accepted(table))


def test_every_spelling_of_one_country_gives_the_same_code():
    for spellings in SPELLINGS:
        assert len({to_iso3(s) for s in spellings}) == 1, spellings


# --- bad input fails loudly ---


@pytest.mark.parametrize("raw", ["Atlantis", "Untied States", "XX"])
def test_an_unknown_spelling_raises(raw):
    with pytest.raises(ValueError, match="unknown country spelling"):
        to_iso3(raw)


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_blank_or_missing_raises_instead_of_becoming_null(raw):
    with pytest.raises(ValueError):
        to_iso3(raw)


def test_a_spelling_not_in_the_list_is_not_guessed_from_its_case():
    # The list is the contract: an unlisted variant fails so it can be added, not guessed.
    with pytest.raises(ValueError):
        to_iso3("UNITED STATES")
