"""Silver transformations on a local SparkSession. No tables: DataFrames in, out."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from builders import bronze_frame, by_key, fixture_batch, rent, sale

from dubai_property.silver import areas, rules
from dubai_property.silver.transform import output_columns, transform

pytestmark = pytest.mark.spark


def rejected(frames):
    return {row["business_key"]: row["failed_rules"] for row in frames.quarantine.collect()}


# --- the fixture: every planted problem handled -----------------------------------


def test_fixture_splits_good_and_rejected_rows(spark):
    frames = transform(fixture_batch(spark), "transactions")

    assert sorted(by_key(frames.valid)) == [
        "SYN-0001",
        "SYN-0002",
        "SYN-0003",  # delivered twice, kept once
        "SYN-0004",
        "SYN-0005",
        "SYN-0006",
        "SYN-0009",
        "SYN-0010",  # zero size and price: kept, flagged
        "SYN-0011",
    ]
    assert rejected(frames) == {
        "SYN-0007": ["invalid_instance_date"],  # 31-02-2024
        "SYN-0008": ["invalid_actual_worth"],  # "n/a"
    }
    assert frames.accepted.count() == 10


def test_values_are_cast_with_explicit_types(spark):
    valid = transform(fixture_batch(spark), "transactions").valid
    types = dict(valid.dtypes)
    assert types["transaction_id"] == "string"
    assert types["instance_date"] == "date"
    assert types["actual_worth"] == types["procedure_area"] == "decimal(18,2)"
    assert types["area_id"] == "int"
    assert types["has_parking"] == "boolean"

    rows = by_key(valid)
    assert rows["SYN-0002"]["actual_worth"] == Decimal("920000.00")  # "920,000"
    assert rows["SYN-0001"]["instance_date"] == date(2024, 1, 5)  # day first
    assert rows["SYN-0004"]["meter_sale_price"] == Decimal("17500.00")


def test_columns_missing_from_the_file_arrive_as_nulls(spark):
    valid = transform(fixture_batch(spark), "transactions").valid
    assert valid.columns == output_columns("transactions")
    row = by_key(valid)["SYN-0001"]
    assert row["rooms_en"] is None
    assert row["project_name_en"] is None
    assert row["bedrooms"] is None


# --- casting and rules -------------------------------------------------------------------


def test_only_unambiguous_date_layouts_are_accepted(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                sale("ISO", instance_date="2024-01-05"),
                sale("SLASH", instance_date="5/1/2024"),
                sale("DOTS", instance_date="05.01.2024"),
                sale("WITH_TIME", instance_date="2024-01-05 00:00:00"),
                sale("FEB_31", instance_date="31-02-2024"),
                sale("MONTH_FIRST", instance_date="01-13-2024"),
                sale("WORDS", instance_date="yesterday"),
                sale("EMPTY", instance_date=""),
            ],
        ),
        "transactions",
    )
    dates = {k: r["instance_date"] for k, r in by_key(frames.valid).items()}
    assert dates == dict.fromkeys(["ISO", "SLASH", "DOTS", "WITH_TIME"], date(2024, 1, 5))
    assert rejected(frames) == {
        "FEB_31": ["invalid_instance_date"],
        "MONTH_FIRST": ["invalid_instance_date"],
        "WORDS": ["invalid_instance_date"],
        "EMPTY": ["missing_instance_date"],
    }


def test_bedrooms_and_booleans(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                sale("TWO_BED", rooms_en="2 B/R", has_parking="1"),
                sale("STUDIO", rooms_en="Studio", has_parking="f"),
                sale("OTHER", rooms_en="PENTHOUSE", has_parking=None),
                sale("BAD_FLAG", has_parking="maybe"),
            ],
        ),
        "transactions",
    )
    rows = by_key(frames.valid)
    assert (rows["TWO_BED"]["bedrooms"], rows["TWO_BED"]["has_parking"]) == (2, True)
    assert (rows["STUDIO"]["bedrooms"], rows["STUDIO"]["has_parking"]) == (0, False)
    assert (rows["OTHER"]["bedrooms"], rows["OTHER"]["has_parking"]) == (None, None)
    assert rejected(frames) == {"BAD_FLAG": ["invalid_has_parking"]}


def publisher_sale(transaction_id, **overrides):
    """A row from Dubai Real Estate Data's sales export: their names, no trans_group."""
    row = {
        "transaction_id": transaction_id,
        "instance_date": "2025-03-14",
        "reg_type_label": "Off-Plan",
        "area_id": "355",
        "area_name_en": "Business Bay",
        "project_name": "Peninsula Four",
        "master_project_name": "Business Bay",
        "building_name": "Tower B",
        "property_type_label": "Unit",
        "property_sub_type": "Flat",
        "usage_label": "Residential",
        "rooms_bucket": "2",
        "has_parking": "true",
        "area_sqm": "100.0",
        "price_aed": "2500000",
        "price_psf": "2322.58",
        "procedure_id": "102",
        "procedure_name_en": "Sell - Pre registration",
        "is_pre_registration": "true",
        "quality_flags": "0",
    }
    row.update(overrides)
    return row


def test_a_publishers_column_names_are_read_as_their_dld_names(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                publisher_sale("PUB-1"),
                publisher_sale(
                    "PUB-STUDIO",
                    rooms_bucket="0",
                    reg_type_label="Ready",
                    procedure_id="11",
                    procedure_name_en="Sell",
                    is_pre_registration="false",
                ),
                publisher_sale("PUB-NO-ROOMS", rooms_bucket=None),
            ],
        ),
        "transactions",
    )
    assert rejected(frames) == {}
    rows = by_key(frames.valid)
    row = rows["PUB-1"]
    assert row["actual_worth"] == Decimal("2500000.00")
    assert row["procedure_area"] == Decimal("100.00")
    assert row["instance_date"] == date(2025, 3, 14)
    assert (row["property_type_en"], row["property_sub_type_en"]) == ("Unit", "Flat")
    assert (row["property_usage_en"], row["reg_type_en"]) == ("Residential", "Off-Plan")
    assert (row["project_name_en"], row["master_project_en"]) == ("Peninsula Four", "Business Bay")
    assert row["building_name_en"] == "Tower B"
    assert row["trans_group_en"] == "Sales"  # sales-only extract: no trans_group column
    assert (row["is_offplan"], row["bedrooms"], row["has_parking"]) == (True, 2, True)
    assert row["price_per_sqft"] == Decimal("2322.58")  # silver's own figure matches theirs
    assert (rows["PUB-STUDIO"]["bedrooms"], rows["PUB-STUDIO"]["is_offplan"]) == (0, False)
    assert rows["PUB-NO-ROOMS"]["bedrooms"] is None


def test_a_dld_export_keeps_its_own_transaction_group(spark):
    """The sales default applies only when the column is absent, never to its values."""
    frames = transform(
        bronze_frame(
            spark,
            [sale("MORTGAGE", trans_group_en="Mortgages"), sale("BLANK", trans_group_en=None)],
        ),
        "transactions",
    )
    rows = by_key(frames.valid)
    assert rows["MORTGAGE"]["trans_group_en"] == "Mortgages"
    assert rows["BLANK"]["trans_group_en"] is None


def test_rescued_and_keyless_rows_are_quarantined(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                sale("RESCUED", _rescued_data='{"extra_column":"x"}'),
                sale("   "),
                sale("GOOD"),
            ],
        ),
        "transactions",
    )
    assert list(by_key(frames.valid)) == ["GOOD"]
    assert rejected(frames) == {
        "RESCUED": ["rescued_data"],
        None: ["missing_transaction_id"],
    }


def test_quarantine_keeps_raw_values_and_lineage(spark):
    quarantine = transform(fixture_batch(spark), "transactions").quarantine
    row = next(r for r in quarantine.collect() if r["business_key"] == "SYN-0008")

    assert json.loads(row["raw_record"])["actual_worth"] == "n/a"
    assert row["dataset"] == "transactions"
    assert len(row["record_hash"]) == 64
    assert row["_load_id"] == "load-1"
    assert row["_source_file"].endswith("sample_transactions.csv")
    assert quarantine.columns[:4] == ["dataset", "business_key", "failed_rules", "raw_record"]


# --- deduplication ----------------------------------------------------------------------------


def test_latest_ingested_version_wins(spark):
    later = datetime(2026, 11, 2, 9, 0)
    frames = transform(
        bronze_frame(
            spark,
            [
                sale("CHANGED", actual_worth="1000000"),
                sale("CHANGED", actual_worth="1200000", _ingested_at=later, _load_id="load-2"),
                sale("REPEATED"),
                sale("REPEATED"),
            ],
        ),
        "transactions",
    )
    rows = by_key(frames.valid)
    assert frames.valid.count() == 2
    assert rows["CHANGED"]["actual_worth"] == Decimal("1200000.00")
    assert rows["CHANGED"]["_load_id"] == "load-2"
    assert frames.accepted.count() == 4


# --- derived measures ----------------------------------------------------------------------------


def test_size_price_per_sqft_and_usd(spark):
    row = by_key(transform(fixture_batch(spark), "transactions").valid)["SYN-0001"]
    sqft = 85.5 * rules.SQFT_PER_SQM
    assert float(row["size_sqft"]) == pytest.approx(sqft, abs=0.005)
    assert float(row["price_per_sqft"]) == pytest.approx(1_450_000 / sqft, abs=0.005)
    assert float(row["value_usd"]) == pytest.approx(1_450_000 / rules.AED_PER_USD, abs=0.005)
    assert row["is_outlier"] is False


def test_outliers_are_flagged_not_dropped(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                sale("CHEAP", procedure_area="100", actual_worth="50000"),
                sale("DEAR", procedure_area="10", actual_worth="5000000"),
                sale("ZERO_SIZE", procedure_area="0"),
                sale("NO_SIZE", procedure_area=None),
                sale("ZERO_VALUE", actual_worth="0"),
                sale("NORMAL"),
            ],
        ),
        "transactions",
    )
    rows = by_key(frames.valid)
    assert {k: r["outlier_reason"] for k, r in rows.items()} == {
        "CHEAP": "price_per_sqft_below_min",
        "DEAR": "price_per_sqft_above_max",
        "ZERO_SIZE": "missing_size",
        "NO_SIZE": "missing_size",
        "ZERO_VALUE": "non_positive_value",
        "NORMAL": None,
    }
    assert {k for k, r in rows.items() if r["is_outlier"]} == set(rows) - {"NORMAL"}
    assert rows["ZERO_SIZE"]["price_per_sqft"] is None
    assert rows["ZERO_VALUE"]["price_per_sqft"] is None


def test_offplan_from_registration_type_or_procedure(spark):
    fixture = by_key(transform(fixture_batch(spark), "transactions").valid)
    assert fixture["SYN-0002"]["is_offplan"] is True
    assert fixture["SYN-0009"]["is_offplan"] is True
    assert fixture["SYN-0001"]["is_offplan"] is False

    rows = by_key(
        transform(
            bronze_frame(
                spark,
                [
                    sale("PRE_REG", procedure_name_en="Sell - Pre-Registration"),
                    sale("UNKNOWN", reg_type_en=None, procedure_name_en=None),
                ],
            ),
            "transactions",
        ).valid
    )
    assert rows["PRE_REG"]["is_offplan"] is True
    assert rows["UNKNOWN"]["is_offplan"] is None


# --- areas ------------------------------------------------------------------------------------


def test_area_names_are_standardised(spark):
    rows = by_key(transform(fixture_batch(spark), "transactions").valid)
    business_bay = [rows[k] for k in ("SYN-0001", "SYN-0006", "SYN-0011")]
    assert {r["area_key"] for r in business_bay} == {"business bay"}
    assert {r["area_name_en"] for r in business_bay} == {"Business Bay"}

    assert rows["SYN-0011"]["area_name_en_source"] == "Business Bay"  # trailing space gone
    assert rows["SYN-0006"]["area_name_en_source"] is None  # English filled from Arabic
    assert rows["SYN-0006"]["is_area_mapped"] is True
    assert rows["SYN-0003"]["area_key"] == "dubai marina"


def test_arabic_spelling_variants_map_to_one_area(spark):
    rows = by_key(
        transform(
            bronze_frame(
                spark,
                [
                    sale("BOTH_NAMES", area_name_ar="الخليج التجارى"),
                    sale("ARABIC_ONLY", area_name_en=None, area_name_ar="الخليج التجاري"),
                ],
            ),
            "transactions",
        ).valid
    )
    assert rows["ARABIC_ONLY"]["area_key"] == "business bay"
    assert rows["ARABIC_ONLY"]["area_name_en"] == "Business Bay"
    assert rows["ARABIC_ONLY"]["area_name_ar"] == rows["BOTH_NAMES"]["area_name_ar"]


def test_alias_maps_an_abbreviation(spark):
    rows = by_key(
        transform(
            bronze_frame(spark, [sale("ABBREVIATED", area_name_en=" jvc ", area_name_ar=None)]),
            "transactions",
        ).valid
    )
    row = rows["ABBREVIATED"]
    assert (row["area_key"], row["area_name_en"]) == (
        "jumeirah village circle",
        "Jumeirah Village Circle",
    )
    assert row["area_name_en_source"] == "jvc"


def test_known_areas_keep_their_names(spark):
    known = spark.createDataFrame(
        [("business bay", "Business Bay", "الخليج التجارى", ["Business Bay"], ["الخليج التجارى"])],
        "area_key string, area_name_en string, area_name_ar string, "
        "area_name_en_variants array<string>, area_name_ar_variants array<string>",
    )
    rows = by_key(
        transform(
            bronze_frame(
                spark,
                [
                    sale("SHOUTED", area_name_en="BUSINESS BAY", area_name_ar=None),
                    sale("ARABIC_ONLY", area_name_en=None, area_name_ar="الخليج التجاري"),
                ],
            ),
            "transactions",
            known_areas=known,
        ).valid
    )
    for row in rows.values():
        assert (row["area_key"], row["area_name_en"]) == ("business bay", "Business Bay")


def test_rows_without_an_area_are_kept_and_flagged(spark):
    rows = by_key(
        transform(
            bronze_frame(spark, [sale("NOWHERE", area_name_en=None, area_name_ar=None)]),
            "transactions",
        ).valid
    )
    assert rows["NOWHERE"]["area_key"] is None
    assert rows["NOWHERE"]["is_area_mapped"] is False


def test_spark_and_python_normalisation_agree(spark):
    names = ["  Business   Bay ", "الخليـــج  التجارى", "أبراج", "JVC", "", None]
    df = spark.createDataFrame([(n,) for n in names], "name string")
    got = df.select(
        "name",
        areas.normalize_en(df["name"]).alias("en"),
        areas.normalize_ar(df["name"]).alias("ar"),
    ).collect()
    for row in got:
        assert row["en"] == rules.normalize_en(row["name"])
        assert row["ar"] == rules.normalize_ar(row["name"])


# --- rents ------------------------------------------------------------------------------------


def test_rents_are_annualised_typed_and_checked(spark):
    frames = transform(
        bronze_frame(
            spark,
            [
                rent("FULL_YEAR"),
                rent(
                    "HALF_YEAR",
                    annual_amount=None,
                    contract_amount="60000",
                    contract_end_date="30-06-2024",
                ),
                rent("MULTI_UNIT", no_of_prop="3"),
                rent("BACKWARDS", contract_end_date="31-12-2023"),
                rent("NO_AMOUNT", annual_amount=None, contract_amount=None),
            ],
        ),
        "rents",
    )
    rows = by_key(frames.valid, "contract_id")

    full = rows["FULL_YEAR"]
    assert full["annual_rent_aed"] == Decimal("120000.00")
    assert full["annual_rent_source"] == "annual_amount"
    assert full["contract_days"] == 366  # 2024 is a leap year
    assert float(full["size_sqft"]) == pytest.approx(1076.39)
    assert full["area_key"] == "dubai marina"
    assert full["is_multi_unit"] is False

    half = rows["HALF_YEAR"]
    assert half["contract_days"] == 182
    assert float(half["annual_rent_aed"]) == pytest.approx(60_000 * 365 / 182, abs=0.005)
    assert half["annual_rent_source"] == "contract_amount"

    assert rows["MULTI_UNIT"]["is_multi_unit"] is True
    assert rejected(frames) == {
        "BACKWARDS": ["end_before_start"],
        "NO_AMOUNT": ["missing_rent_amount"],
    }
