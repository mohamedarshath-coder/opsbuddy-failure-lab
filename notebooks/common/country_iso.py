"""Maps every source spelling in country_spellings.SPELLINGS to its ISO 3166-1 alpha-3 code."""

from notebooks.common.country_spellings import SPELLINGS

# Same order as SPELLINGS: one code per country list.
ISO3 = [
    "USA", "CAN", "MEX", "BRA", "ARG", "GBR", "IRL", "FRA", "DEU", "ESP", "ITA", "NLD", "CHE",
    "SWE", "IND", "CHN", "JPN", "KOR", "SGP", "ARE", "SAU", "ZAF", "NGA", "EGY", "AUS", "NZL",
]
assert len(ISO3) == len(SPELLINGS), "ISO3 and SPELLINGS must have one entry per country"

_LOOKUP = {}
for _code, _names in zip(ISO3, SPELLINGS):
    for _name in _names:
        _key = _name.strip()
        assert _LOOKUP.get(_key, _code) == _code, f"spelling {_key!r} maps to two countries"
        _LOOKUP[_key] = _code


def to_iso3(raw):
    """Return the alpha-3 code for a source spelling; unknown spellings raise ValueError."""
    try:
        return _LOOKUP[raw.strip()]
    except (KeyError, AttributeError):
        raise ValueError(f"unknown country spelling: {raw!r}") from None
