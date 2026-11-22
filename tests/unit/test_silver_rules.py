"""Silver rules as plain data: no Spark needed."""

from __future__ import annotations

import re

import pytest

from dubai_property.config import DATASETS, SilverSettings, get_dataset
from dubai_property.silver import rules

# --- fields ------------------------------------------------------------------------


@pytest.mark.parametrize("dataset", sorted(rules.FIELDS))
def test_business_key_is_a_required_string(dataset):
    key = get_dataset(dataset).business_key
    field = {f.name: f for f in rules.fields_for(dataset)}[key]
    assert field.required
    assert field.kind == "string"  # keys like "00123" must keep their zeros


@pytest.mark.parametrize("dataset", sorted(rules.FIELDS))
def test_field_names_are_unique_and_never_metadata(dataset):
    names = [f.name for f in rules.fields_for(dataset)]
    assert len(names) == len(set(names))
    assert not any(n.startswith("_") for n in names)  # "_" is reserved for pipeline columns


@pytest.mark.parametrize("dataset", sorted(rules.FIELDS))
def test_source_aliases_rename_known_fields_without_shadowing_any(dataset):
    field_names = {f.name for f in rules.fields_for(dataset)}
    aliases_by_field = rules.aliases_for(dataset)
    aliases = [a for names in aliases_by_field.values() for a in names]
    assert set(aliases_by_field) <= field_names
    assert set(rules.defaults_for(dataset)) <= field_names
    assert len(aliases) == len(set(aliases)), "one source column feeds one field"
    assert not set(aliases) & field_names, "an alias must not hide a DLD column"


def test_aliases_are_snake_case_as_bronze_stores_them():
    """Bronze renames headers once (annualAmountAed -> annual_amount_aed); aliases use that form."""
    from dubai_property.utils.naming import to_snake_case

    for dataset in rules.SOURCE_ALIASES:
        for names in rules.aliases_for(dataset).values():
            assert all(to_snake_case(name) == name for name in names)


def test_only_known_datasets_have_aliases_or_defaults():
    assert set(rules.SOURCE_ALIASES) <= set(rules.FIELDS)
    assert set(rules.MISSING_COLUMN_DEFAULTS) <= set(rules.FIELDS)
    assert rules.aliases_for("unknown") == {} and rules.defaults_for("unknown") == {}


@pytest.mark.parametrize(
    ("rooms", "bedrooms"), [("0", "0"), ("3", "3"), ("12", "12"), ("2 B/R", None)]
)
def test_bedroom_count_pattern(rooms, bedrooms):
    match = re.match(rules.BEDROOM_COUNT, rooms)
    assert (match.group(1) if match else None) == bedrooms


def test_fields_feed_the_measures_silver_derives():
    sales = {f.name: f.kind for f in rules.TRANSACTION_FIELDS}
    assert sales["instance_date"] == "date"
    assert sales["actual_worth"] == sales["procedure_area"] == "decimal"
    rents = {f.name: f.kind for f in rules.RENT_FIELDS}
    assert rents["contract_start_date"] == rents["contract_end_date"] == "date"
    assert rents["annual_amount"] == rents["contract_amount"] == "decimal"


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown kind"):
        rules.Field("x", "money")


def test_unknown_dataset_names_the_known_ones():
    with pytest.raises(KeyError, match="transactions"):
        rules.fields_for("units")


def test_every_silver_dataset_has_rules():
    assert {n for n, d in DATASETS.items() if d.silver_table} == set(rules.FIELDS)


# --- patterns (shared with Spark) ------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "pattern", "groups"),
    [
        ("2024-01-05", rules.ISO_DATE, ("2024", "01", "05")),
        ("2024-1-5 00:00:00", rules.ISO_DATE, ("2024", "1", "5")),
        ("05-01-2024", rules.DAY_FIRST_DATE, ("05", "01", "2024")),
        ("5/1/2024", rules.DAY_FIRST_DATE, ("5", "1", "2024")),
        ("05.01.2024 10:30", rules.DAY_FIRST_DATE, ("05", "01", "2024")),
    ],
)
def test_date_patterns_capture_parts(text, pattern, groups):
    assert re.match(pattern, text).groups() == groups


@pytest.mark.parametrize("text", ["n/a", "Jan 5 2024", "2024/01/05", "05-01-24", "20240105"])
def test_unsupported_date_layouts_match_nothing(text):
    assert not re.match(rules.ISO_DATE, text)
    assert not re.match(rules.DAY_FIRST_DATE, text)


def test_number_noise_removes_thousands_separators():
    assert re.sub(rules.NUMBER_NOISE, "", "1,250,000") == "1250000"
    assert re.sub(rules.NUMBER_NOISE, "", " 920 000 ") == "920000"


@pytest.mark.parametrize(
    ("rooms", "bedrooms"), [("2 B/R", "2"), ("3 b/r", "3"), ("10B/R", "10"), ("PENTHOUSE", None)]
)
def test_bedrooms_pattern(rooms, bedrooms):
    match = re.match(rules.BEDROOMS, rooms)
    assert (match.group(1) if match else None) == bedrooms


def test_studio_pattern():
    assert re.match(rules.STUDIO, "Studio")
    assert not re.match(rules.STUDIO, "1 B/R")


@pytest.mark.parametrize(
    ("text", "pattern", "matches"),
    [
        ("Off-Plan Properties", rules.OFF_PLAN, True),
        ("Off Plan", rules.OFF_PLAN, True),
        ("Existing Properties", rules.OFF_PLAN, False),
        ("Sell - Pre registration", rules.PRE_REGISTRATION, True),
        ("Sell - Pre-Registration", rules.PRE_REGISTRATION, True),
        ("Sell", rules.PRE_REGISTRATION, False),
    ],
)
def test_offplan_patterns(text, pattern, matches):
    assert bool(re.search(pattern, text)) is matches


def test_boolean_words_do_not_overlap():
    assert not set(rules.TRUE_VALUES) & set(rules.FALSE_VALUES)
    assert all(v == v.lower() for v in rules.TRUE_VALUES + rules.FALSE_VALUES)


# --- area names ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  Business   Bay ", "business bay"),
        ("Business Bay", "business bay"),
        ("BUSINESS BAY", "business bay"),
        ("   ", None),
        (None, None),
    ],
)
def test_normalize_en(raw, expected):
    assert rules.normalize_en(raw) == expected


def test_arabic_spelling_variants_normalise_to_one_form():
    final_yaa = rules.normalize_ar("الخليج التجارى")  # alef maksura, as in DLD files
    assert final_yaa == rules.normalize_ar("الخليج التجاري")
    assert final_yaa == rules.normalize_ar("الخليـــج  التجارى")  # tatweel and double space
    assert rules.normalize_ar("أبراج") == rules.normalize_ar("ابراج")  # hamza on alef
    assert rules.normalize_ar("مرسى دبي") != rules.normalize_ar("الخليج التجاري")


def test_alias_keys_map_variants_and_canonical_names():
    keys = rules.alias_keys(
        {"JVC": "Jumeirah Village Circle", " J.V.C ": "Jumeirah Village Circle"}
    )
    assert keys["jvc"] == "jumeirah village circle"
    assert keys["j.v.c"] == "jumeirah village circle"
    assert keys["jumeirah village circle"] == "jumeirah village circle"
    assert rules.alias_display({"JVC": "Jumeirah Village Circle"}) == {
        "jumeirah village circle": "Jumeirah Village Circle"
    }


# --- units and thresholds -----------------------------------------------------------------


def test_units_and_outlier_thresholds_match_the_plan():
    hundred_sqm_in_sqft = 100 * rules.SQFT_PER_SQM
    usd = 3_672_500 / rules.AED_PER_USD
    assert hundred_sqm_in_sqft == pytest.approx(1076.39)
    assert usd == pytest.approx(1_000_000)
    assert (rules.PRICE_PER_SQFT_MIN, rules.PRICE_PER_SQFT_MAX) == (100, 20_000)


# --- settings -----------------------------------------------------------------------------


def test_silver_settings_table_names():
    s = SilverSettings.for_databricks("dubai")
    assert s.bronze_table(get_dataset("rents")) == "dubai.bronze.dld_rents_raw"
    assert s.silver_table(get_dataset("transactions")) == "dubai.silver.transactions"
    assert s.quarantine_table == "dubai.silver.quarantine"
    assert s.areas_table == "dubai.silver.areas"
    assert s.processed_loads_table == "dubai.silver.processed_loads"


def test_reference_datasets_have_no_silver_table_yet():
    with pytest.raises(ValueError, match="no silver table"):
        SilverSettings("c").silver_table(get_dataset("units"))
