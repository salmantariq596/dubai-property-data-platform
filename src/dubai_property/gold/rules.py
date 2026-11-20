"""Gold rules as plain data and small pure functions. No Spark.

The Spark code in :mod:`dubai_property.gold` mirrors these definitions, and the
Spark tests check that both agree.
"""

from __future__ import annotations

from datetime import date

from dubai_property.silver.rules import clean_text

# --- calendar ----------------------------------------------------------------------

DATE_START = date(2000, 1, 1)
DATE_END = date(2030, 12, 31)

# The UAE moved its weekend from Friday-Saturday to Saturday-Sunday on 1 January 2022.
WEEKEND_CHANGE = date(2022, 1, 1)
WEEKEND_BEFORE = (5, 6)  # ISO weekday numbers: Friday, Saturday
WEEKEND_FROM = (6, 7)  # Saturday, Sunday


def date_key(day: date) -> int:
    """``2024-01-05`` -> ``20240105``: readable, sortable, joins as an int."""
    return day.year * 10_000 + day.month * 100 + day.day


def is_weekend(day: date) -> bool:
    days = WEEKEND_BEFORE if day < WEEKEND_CHANGE else WEEKEND_FROM
    return day.isoweekday() in days


# --- SCD Type 2 ---------------------------------------------------------------------

# A key's first version is valid from the start of time, so facts dated before
# the warehouse first saw the key still find it. Later versions start on the
# date the change was detected.
VALID_FROM_FIRST = date(1900, 1, 1)
VALID_TO_OPEN = date(9999, 12, 31)

# --- surrogate keys -------------------------------------------------------------------

UNKNOWN_SK = -1  # facts whose area, type or project is missing point here
ALL_TYPES_SK = 0  # agg_area_monthly roll-up row across every property type
UNKNOWN_LABEL = "Unknown"
ALL_TYPES_LABEL = "All types"

# --- facts and aggregates ------------------------------------------------------------------

SALES_GROUP = "sales"  # trans_group_en; mortgages and gifts are not sales

# Gross yield is reported only when both medians rest on at least this many
# records for the same area, property type and month. Fewer, and one unusual
# contract or sale moves the yield by whole percentage points.
MIN_RECORDS_FOR_YIELD = 10

# --- property types ----------------------------------------------------------------------

# Sales and Ejari name property types differently. Map variants (any case or
# spacing) to the sales-side name, so both datasets share one dim_property_type
# row and rental yield compares like with like. Extend after profiling real files.
PROPERTY_TYPE_ALIASES: dict[str, str] = {}
PROPERTY_SUB_TYPE_ALIASES: dict[str, str] = {
    "Apartment": "Flat",
}


def alias_map(aliases: dict[str, str]) -> dict[str, str]:
    """Lower-cased variant -> lower-cased canonical name."""
    out: dict[str, str] = {}
    for variant, canonical in aliases.items():
        target = clean_text(canonical).lower()
        out[clean_text(variant).lower()] = target
        out[target] = target
    return out


def property_type_key(
    property_type: str | None,
    sub_type: str | None,
    type_aliases: dict[str, str] = PROPERTY_TYPE_ALIASES,
    sub_type_aliases: dict[str, str] = PROPERTY_SUB_TYPE_ALIASES,
) -> str | None:
    """``("Unit", " apartment ")`` -> ``"unit|flat"``; None when the type is unknown."""
    kind = clean_text(property_type)
    if kind is None:
        return None
    kind = kind.lower()
    kind = alias_map(type_aliases).get(kind, kind)
    sub = clean_text(sub_type)
    sub = sub.lower() if sub else ""
    sub = alias_map(sub_type_aliases).get(sub, sub)
    return f"{kind}|{sub}"


def project_key(name: str | None) -> str | None:
    """Projects join across datasets by their normalised English name."""
    text = clean_text(name)
    return text.lower() if text else None


# --- physical layout ---------------------------------------------------------------------


def create_clustered_table_sql(table: str, view: str, cluster_by: list[str]) -> str:
    """Replace ``table`` with the rows of ``view``, liquid-clustered by ``cluster_by``."""
    columns = ", ".join(f"`{c}`" for c in cluster_by)
    return f"CREATE OR REPLACE TABLE {table} CLUSTER BY ({columns}) AS SELECT * FROM {view}"
