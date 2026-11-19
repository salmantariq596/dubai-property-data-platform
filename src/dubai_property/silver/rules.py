"""Silver rules as plain data: fields, types, patterns, thresholds and area aliases.

Nothing here imports Spark, so every rule can be reviewed and unit-tested
without a cluster. :mod:`dubai_property.silver.transform` turns these into
Spark expressions.

The regular expressions are shared by Spark (Java regex) and the unit tests
(Python ``re``). They stay within the syntax both engines read the same way.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# --- units and thresholds --------------------------------------------------------

SQFT_PER_SQM = 10.7639
AED_PER_USD = 3.6725  # fixed peg

# Price per sqft outside this range is flagged as an outlier, never deleted.
PRICE_PER_SQFT_MIN = 100
PRICE_PER_SQFT_MAX = 20_000

MONEY_TYPE = "DECIMAL(18,2)"

# --- fields ------------------------------------------------------------------------

KINDS = ("string", "int", "decimal", "date", "bool")


@dataclass(frozen=True)
class Field:
    """One silver column read from bronze.

    ``required`` fields must be present; a value that is present but cannot be
    cast to ``kind`` always fails, required or not. Either way the row goes to
    quarantine with the rule it broke.
    """

    name: str
    kind: str = "string"
    required: bool = False

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"{self.name}: unknown kind {self.kind!r}, expected one of {KINDS}")


def _strings(*names: str) -> tuple[Field, ...]:
    return tuple(Field(n) for n in names)


# Expected DLD schema (docs/data_dictionary.md). Columns missing from a file
# arrive as nulls, so the silver schema stays the same whatever the source sends.
TRANSACTION_FIELDS: tuple[Field, ...] = (
    Field("transaction_id", required=True),
    Field("procedure_id", "int"),
    Field("trans_group_id", "int"),
    *_strings("trans_group_en", "trans_group_ar", "procedure_name_en", "procedure_name_ar"),
    Field("instance_date", "date", required=True),
    *_strings(
        "property_type_en",
        "property_type_ar",
        "property_sub_type_en",
        "property_sub_type_ar",
        "property_usage_en",
        "property_usage_ar",
        "reg_type_en",
        "reg_type_ar",
    ),
    Field("area_id", "int"),
    *_strings(
        "area_name_en",
        "area_name_ar",
        "building_name_en",
        "building_name_ar",
        "project_number",
        "project_name_en",
        "project_name_ar",
        "master_project_en",
        "master_project_ar",
        "nearest_landmark_en",
        "nearest_metro_en",
        "nearest_mall_en",
        "rooms_en",
        "rooms_ar",
    ),
    Field("has_parking", "bool"),
    Field("procedure_area", "decimal"),
    Field("actual_worth", "decimal", required=True),
    Field("meter_sale_price", "decimal"),
    Field("rent_value", "decimal"),
    Field("meter_rent_price", "decimal"),
    Field("no_of_parties_role_1", "int"),
    Field("no_of_parties_role_2", "int"),
    Field("no_of_parties_role_3", "int"),
)

RENT_FIELDS: tuple[Field, ...] = (
    Field("contract_id", required=True),
    Field("contract_reg_type_en"),
    Field("contract_start_date", "date", required=True),
    Field("contract_end_date", "date", required=True),
    Field("contract_amount", "decimal"),
    Field("annual_amount", "decimal"),
    *_strings("ejari_property_type_en", "ejari_property_sub_type_en", "property_usage_en"),
    Field("area_id", "int"),
    *_strings("area_name_en", "area_name_ar", "project_name_en", "master_project_en"),
    Field("actual_area", "decimal"),
    Field("tenant_type_en"),
    Field("no_of_prop", "int"),
    Field("is_free_hold", "bool"),
)

FIELDS: dict[str, tuple[Field, ...]] = {
    "transactions": TRANSACTION_FIELDS,
    "rents": RENT_FIELDS,
}


def fields_for(dataset: str) -> tuple[Field, ...]:
    try:
        return FIELDS[dataset]
    except KeyError:
        raise KeyError(f"No silver rules for {dataset!r}. Known: {sorted(FIELDS)}") from None


# --- parsing patterns ------------------------------------------------------------------

WHITESPACE = r"[\s ]+"  # includes the non-breaking space common in exports
NUMBER_NOISE = r"[,\s ]"  # thousands separators and stray spaces: "920,000"

# Only two date layouts are accepted, both unambiguous: ISO (2024-01-05) and
# day-first (05-01-2024, 5/1/2024, 05.01.2024). A trailing time is ignored.
# Month-first dates are never guessed.
ISO_DATE = r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T].*)?$"
DAY_FIRST_DATE = r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})(?:[ T].*)?$"

BEDROOMS = r"(?i)^(\d{1,2})\s*b\s*/\s*r"  # "2 B/R"
STUDIO = r"(?i)^studio"
OFF_PLAN = r"(?i)off[\s-]*plan"  # reg_type_en: "Off-Plan Properties"
PRE_REGISTRATION = r"(?i)pre[\s-]*reg"  # procedure_name_en: "Sell - Pre registration"

TRUE_VALUES = ("true", "t", "yes", "y", "1")
FALSE_VALUES = ("false", "f", "no", "n", "0")

# --- area names --------------------------------------------------------------------------

# Arabic spelling varies in harmless ways: diacritics, the tatweel stretch
# character, hamza forms of alef, and final yaa / taa marbuta. These are
# folded together for matching only; stored names keep their original spelling.
ARABIC_STRIP = "[ً-ْـ]"
ARABIC_FROM = "أإآٱىة"
ARABIC_TO = "اااايه"

# Variant (any case or spacing) -> canonical English area name. Extend this as
# real files show new variants; profiling reports list the names actually seen.
AREA_ALIASES: dict[str, str] = {
    "JVC": "Jumeirah Village Circle",
}

_WHITESPACE_RE = re.compile(WHITESPACE, re.ASCII)
_ARABIC_STRIP_RE = re.compile(ARABIC_STRIP)
_ARABIC_TABLE = str.maketrans(ARABIC_FROM, ARABIC_TO)


def clean_text(value: str | None) -> str | None:
    """Collapse whitespace and trim; empty becomes None (same as Spark)."""
    if value is None:
        return None
    text = _WHITESPACE_RE.sub(" ", value).strip(" ")
    return text or None


def normalize_en(value: str | None) -> str | None:
    """Matching form of an English name: ``"  Business  Bay "`` -> ``"business bay"``."""
    text = clean_text(value)
    return text.lower() if text else None


def normalize_ar(value: str | None) -> str | None:
    """Matching form of an Arabic name, folding harmless spelling differences."""
    text = clean_text(value)
    if text is None:
        return None
    return _ARABIC_STRIP_RE.sub("", text).translate(_ARABIC_TABLE) or None


def alias_keys(aliases: dict[str, str] = AREA_ALIASES) -> dict[str, str]:
    """Normalised variant -> area key. Canonical names map to themselves."""
    keys: dict[str, str] = {}
    for variant, canonical in aliases.items():
        key = normalize_en(canonical)
        keys[normalize_en(variant)] = key
        keys[key] = key
    return keys


def alias_display(aliases: dict[str, str] = AREA_ALIASES) -> dict[str, str]:
    """Area key -> the English name to show for it."""
    return {normalize_en(canonical): canonical for canonical in aliases.values()}
