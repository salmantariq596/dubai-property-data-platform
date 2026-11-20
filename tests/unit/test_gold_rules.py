"""Gold rules as plain data: no Spark needed."""

from __future__ import annotations

from datetime import date

import pytest

from dubai_property.config import GOLD_TABLES, GoldSettings
from dubai_property.gold import rules

# --- calendar ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "key"),
    [(date(2024, 1, 5), 20240105), (date(2000, 1, 1), 20000101), (date(2030, 12, 31), 20301231)],
)
def test_date_key(day, key):
    assert rules.date_key(day) == key


def test_date_keys_sort_like_dates():
    days = [date(2023, 12, 31), date(2024, 1, 1), date(2024, 2, 29), date(2024, 10, 1)]
    assert [rules.date_key(d) for d in days] == sorted(rules.date_key(d) for d in days)


@pytest.mark.parametrize(
    ("day", "weekend"),
    [
        (date(2021, 12, 31), True),  # Friday, old weekend
        (date(2021, 12, 25), True),  # Saturday, both weekends
        (date(2021, 12, 26), False),  # Sunday was a working day until 2022
        (date(2022, 1, 1), True),  # Saturday, first day of the new weekend
        (date(2022, 1, 2), True),  # Sunday, new weekend
        (date(2022, 1, 7), False),  # Friday became a working day
        (date(2024, 1, 3), False),  # Wednesday
    ],
)
def test_uae_weekend_changed_in_2022(day, weekend):
    assert rules.is_weekend(day) is weekend


def test_calendar_covers_the_plan_range():
    first_day, last_day = rules.DATE_START, rules.DATE_END
    assert first_day == date(2000, 1, 1)
    assert last_day == date(2030, 12, 31)


# --- SCD Type 2 and reserved keys ------------------------------------------------------


def test_first_version_covers_all_history_and_stays_open():
    assert rules.VALID_FROM_FIRST < rules.DATE_START
    assert rules.VALID_TO_OPEN > rules.DATE_END


def test_reserved_surrogate_keys_are_distinct():
    assert rules.UNKNOWN_SK != rules.ALL_TYPES_SK
    assert rules.UNKNOWN_LABEL != rules.ALL_TYPES_LABEL


def test_yield_needs_a_meaningful_sample():
    assert rules.MIN_RECORDS_FOR_YIELD >= 5


# --- property types and projects ----------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "sub", "key"),
    [
        ("Unit", "Flat", "unit|flat"),
        (" UNIT ", " flat ", "unit|flat"),
        ("Unit", "Apartment", "unit|flat"),  # Ejari wording mapped to the sales name
        ("Land", None, "land|"),
        (None, "Flat", None),
        ("  ", "Flat", None),
    ],
)
def test_property_type_key(kind, sub, key):
    assert rules.property_type_key(kind, sub) == key


def test_property_type_aliases_apply_to_both_levels():
    key = rules.property_type_key(
        "Apartment Building",
        "Studio Flat",
        type_aliases={"Apartment Building": "Building"},
        sub_type_aliases={"Studio Flat": "Flat"},
    )
    assert key == "building|flat"


def test_alias_map_lowercases_and_maps_canonical_to_itself():
    assert rules.alias_map({" Apartment ": "Flat"}) == {"apartment": "flat", "flat": "flat"}


@pytest.mark.parametrize(
    ("name", "key"), [("Marina  Gate", "marina gate"), ("MARINA GATE ", "marina gate"), ("", None)]
)
def test_project_key(name, key):
    assert rules.project_key(name) == key


# --- physical layout and settings ------------------------------------------------------------


def test_clustered_table_sql():
    sql = rules.create_clustered_table_sql("dubai.gold.fact_sales", "src", ["date_key"])
    assert sql == (
        "CREATE OR REPLACE TABLE dubai.gold.fact_sales CLUSTER BY (`date_key`) AS SELECT * FROM src"
    )


def test_gold_settings():
    s = GoldSettings.for_databricks("dubai")
    assert s.cluster_facts is True
    assert s.table("fact_sales") == "dubai.gold.fact_sales"
    assert s.silver_table("areas") == "dubai.silver.areas"
    assert GoldSettings("c").cluster_facts is False  # local default: plain tables


def test_unknown_gold_table_is_rejected():
    with pytest.raises(KeyError, match="fact_sales"):
        GoldSettings("c").table("fact_sale")


def test_gold_tables_match_the_plan():
    assert set(GOLD_TABLES) == {
        "dim_date",
        "dim_area",
        "dim_property_type",
        "dim_project",
        "fact_sales",
        "fact_rents",
        "agg_area_monthly",
    }
