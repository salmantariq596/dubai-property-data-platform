"""Gold dimensions, facts and aggregates on a local SparkSession. No tables."""

from __future__ import annotations

from datetime import date, timedelta
from statistics import median

import pytest
from builders import rent, sale, silver_frame

from dubai_property.gold import dimensions, facts, rules
from dubai_property.gold.pipeline import empty_silver
from dubai_property.silver.transform import output_columns

pytestmark = pytest.mark.spark

AREAS_SCHEMA = "area_key string, area_name_en string, area_name_ar string"


def areas_snapshot(spark, rows):
    return spark.createDataFrame(rows, AREAS_SCHEMA)


def as_rows(df):
    return sorted(tuple(r) for r in df.collect())


def versions(dim, key):
    return sorted((r for r in dim.collect() if r["area_key"] == key), key=lambda r: r["valid_from"])


def model(spark, sales_rows, rent_rows, dim_area=None):
    """Silver frames plus every dimension built from them."""
    transactions = silver_frame(spark, sales_rows, "transactions")
    rents = silver_frame(spark, rent_rows, "rents") if rent_rows else empty_silver(spark, "rents")
    if dim_area is None:
        snapshot = transactions.select("area_key", "area_name_en", "area_name_ar").unionByName(
            rents.select("area_key", "area_name_en", "area_name_ar")
        )
        dim_area = dimensions.scd2_area(None, snapshot, date(2026, 11, 20))
    return {
        "transactions": transactions,
        "rents": rents,
        "dim_area": dim_area,
        "dim_property_type": dimensions.build_dim_property_type(transactions, rents),
        "dim_project": dimensions.build_dim_project(transactions, rents),
    }


def dims(m):
    return m["dim_area"], m["dim_property_type"], m["dim_project"]


# --- dim_date -----------------------------------------------------------------------------


def test_dim_date_covers_2000_to_2030_with_the_uae_weekend(spark):
    rows = dimensions.build_dim_date(spark).collect()
    assert len(rows) == (rules.DATE_END - rules.DATE_START).days + 1 == 11_323

    for row in rows:  # Spark and the Python definition agree on every day
        assert row["date_key"] == rules.date_key(row["date"])
        assert row["is_weekend"] is rules.is_weekend(row["date"])

    day = {r["date_key"]: r for r in rows}[20240105]
    assert (day["year"], day["quarter"], day["month"], day["month_key"]) == (2024, 1, 1, 202401)
    assert (day["month_name"], day["day_name"], day["iso_weekday"]) == ("January", "Friday", 5)
    assert day["month_start"] == date(2024, 1, 1)


# --- dim_property_type and dim_project ----------------------------------------------------------


def test_property_types_are_shared_by_sales_and_rents(spark):
    m = model(
        spark,
        [
            sale("S1", property_type_en="Unit", property_sub_type_en="Flat"),
            sale("S2", property_type_en="Villa", property_sub_type_en="Villa"),
            sale("S3", property_type_en="Land", property_sub_type_en=None),
        ],
        [
            rent("R1", ejari_property_type_en="Unit", ejari_property_sub_type_en="Apartment"),
            rent("R2", ejari_property_type_en="Villa", ejari_property_sub_type_en="Villa"),
            rent("R3", ejari_property_type_en="Unit", ejari_property_sub_type_en="Office"),
        ],
    )
    dim = {r["property_type_key"]: r for r in m["dim_property_type"].collect()}

    assert (dim["unit|flat"]["in_sales"], dim["unit|flat"]["in_rents"]) == (True, True)
    assert dim["unit|flat"]["property_sub_type"] == "Flat"  # sales spelling preferred
    assert (dim["villa|villa"]["in_sales"], dim["villa|villa"]["in_rents"]) == (True, True)
    assert (dim["land|"]["in_sales"], dim["land|"]["in_rents"]) == (True, False)
    assert (dim["unit|office"]["in_sales"], dim["unit|office"]["in_rents"]) == (False, True)

    reserved = {
        r["property_type_sk"]: r["property_type"]
        for r in m["dim_property_type"].collect()
        if r["property_type_key"] is None
    }
    assert reserved == {rules.UNKNOWN_SK: "Unknown", rules.ALL_TYPES_SK: "All types"}
    sks = [r["property_type_sk"] for r in m["dim_property_type"].collect()]
    assert len(sks) == len(set(sks))


def test_surrogate_keys_are_stable_across_rebuilds(spark):
    rows = [sale("S1", property_type_en="Unit", property_sub_type_en="Flat")]
    first = model(spark, rows, [])
    again = model(spark, rows, [])
    for name in ("dim_property_type", "dim_project", "dim_area"):
        assert as_rows(first[name]) == as_rows(again[name])


def test_spark_and_python_property_type_keys_agree(spark):
    pairs = [("Unit", "Flat"), (" UNIT ", "apartment"), ("Land", None), (None, "Flat"), ("", "")]
    df = spark.createDataFrame(pairs, "kind string, sub string")
    got = df.select(
        "kind", "sub", dimensions.property_type_key_column(df["kind"], df["sub"]).alias("key")
    ).collect()
    for row in got:
        assert row["key"] == rules.property_type_key(row["kind"], row["sub"])


def test_projects_match_across_datasets_by_name(spark):
    m = model(
        spark,
        [sale("S1", project_name_en="Marina Gate", project_number="P-1")],
        [rent("R1", project_name_en="MARINA  GATE ")],
    )
    projects = {r["project_key"]: r for r in m["dim_project"].collect()}
    gate = projects["marina gate"]
    assert (gate["project_name_en"], gate["project_number"]) == ("Marina Gate", "P-1")
    assert (gate["in_sales"], gate["in_rents"]) == (True, True)
    assert gate["developer_name"] is None  # filled once the projects dataset is cleaned
    assert projects[None]["project_sk"] == rules.UNKNOWN_SK


# --- dim_area: SCD Type 2 -----------------------------------------------------------------------

DAY_1 = date(2026, 11, 20)
DAY_2 = date(2026, 12, 1)


def test_first_load_opens_one_version_per_area(spark):
    dim = dimensions.scd2_area(
        None, areas_snapshot(spark, [("business bay", "Business Bay", "الخليج التجارى")]), DAY_1
    )
    [bay] = versions(dim, "business bay")
    assert (bay["valid_from"], bay["valid_to"], bay["is_current"]) == (
        rules.VALID_FROM_FIRST,
        rules.VALID_TO_OPEN,
        True,
    )
    [unknown] = [r for r in dim.collect() if r["area_key"] is None]
    assert (unknown["area_sk"], unknown["area_name_en"]) == (rules.UNKNOWN_SK, "Unknown")


def test_same_snapshot_twice_changes_nothing(spark):
    snapshot = areas_snapshot(spark, [("business bay", "Business Bay", "الخليج التجارى")])
    first = dimensions.scd2_area(None, snapshot, DAY_1)
    again = dimensions.scd2_area(first, snapshot, DAY_2)
    assert as_rows(again) == as_rows(first)


def test_a_name_change_closes_the_old_version_and_opens_a_new_one(spark):
    first = dimensions.scd2_area(
        None, areas_snapshot(spark, [("business bay", "Business Bay", "الخليج التجارى")]), DAY_1
    )
    renamed = areas_snapshot(spark, [("business bay", "Business Bay District", "الخليج التجارى")])
    second = dimensions.scd2_area(first, renamed, DAY_2)

    old, new = versions(second, "business bay")
    assert (old["area_name_en"], old["is_current"]) == ("Business Bay", False)
    assert old["valid_to"] == DAY_2 - timedelta(days=1)
    assert (new["area_name_en"], new["is_current"]) == ("Business Bay District", True)
    assert (new["valid_from"], new["valid_to"]) == (DAY_2, rules.VALID_TO_OPEN)
    assert old["area_sk"] != new["area_sk"]


def test_a_second_change_on_the_same_day_updates_that_days_version(spark):
    first = dimensions.scd2_area(
        None, areas_snapshot(spark, [("business bay", "Business Bay", None)]), DAY_1
    )
    morning = dimensions.scd2_area(
        first, areas_snapshot(spark, [("business bay", "Business Bay District", None)]), DAY_2
    )
    evening = dimensions.scd2_area(
        morning, areas_snapshot(spark, [("business bay", "Business Bay Dist.", None)]), DAY_2
    )
    old, new = versions(evening, "business bay")
    assert new["area_name_en"] == "Business Bay Dist."
    assert new["area_sk"] == versions(morning, "business bay")[1]["area_sk"]
    assert old["valid_to"] == DAY_2 - timedelta(days=1)


def test_new_areas_start_at_the_beginning_and_missing_areas_are_kept(spark):
    first = dimensions.scd2_area(
        None, areas_snapshot(spark, [("business bay", "Business Bay", None)]), DAY_1
    )
    later = dimensions.scd2_area(
        first, areas_snapshot(spark, [("dubai marina", "Dubai Marina", None)]), DAY_2
    )
    [marina] = versions(later, "dubai marina")
    assert marina["valid_from"] == rules.VALID_FROM_FIRST  # old sales still find it
    [bay] = versions(later, "business bay")
    assert bay["is_current"] is True  # not delivered this time, still kept


# --- facts --------------------------------------------------------------------------------------


def test_fact_sales_holds_sales_only_with_dimension_keys(spark):
    m = model(
        spark,
        [
            sale(
                "SALE", property_type_en="Unit", property_sub_type_en="Flat", project_name_en="Gate"
            ),
            sale("MORTGAGE", trans_group_en="Mortgages"),
            sale("GIFT", trans_group_en="Gifts"),
            sale("NO_AREA", area_name_en=None, area_name_ar=None),
        ],
        [],
    )
    fact = facts.build_fact_sales(m["transactions"], *dims(m))
    rows = {r["transaction_id"]: r for r in fact.collect()}

    assert set(rows) == {"SALE", "NO_AREA"}
    assert fact.columns == facts.FACT_SALES_COLUMNS

    sale_row = rows["SALE"]
    area_sk = versions(m["dim_area"], "business bay")[0]["area_sk"]
    type_sk = {r["property_type_key"]: r for r in m["dim_property_type"].collect()}["unit|flat"]
    project_sk = {r["project_key"]: r for r in m["dim_project"].collect()}["gate"]
    assert sale_row["date_key"] == 20240105
    assert sale_row["area_sk"] == area_sk
    assert sale_row["property_type_sk"] == type_sk["property_type_sk"]
    assert sale_row["project_sk"] == project_sk["project_sk"]
    assert float(sale_row["value_aed"]) == 1_500_000

    missing = rows["NO_AREA"]
    assert (missing["area_sk"], missing["property_type_sk"], missing["project_sk"]) == (
        rules.UNKNOWN_SK,
        rules.UNKNOWN_SK,
        rules.UNKNOWN_SK,
    )


def test_facts_join_the_area_version_valid_on_their_own_date(spark):
    snapshot = areas_snapshot(spark, [("business bay", "Business Bay", "الخليج التجارى")])
    renamed = areas_snapshot(spark, [("business bay", "Business Bay District", "الخليج التجارى")])
    dim_area = dimensions.scd2_area(
        dimensions.scd2_area(None, snapshot, DAY_1), renamed, date(2024, 7, 1)
    )
    m = model(
        spark,
        [sale("BEFORE", instance_date="05-01-2024"), sale("AFTER", instance_date="01-08-2024")],
        [],
        dim_area=dim_area,
    )
    rows = {
        r["transaction_id"]: r
        for r in facts.build_fact_sales(m["transactions"], *dims(m)).collect()
    }
    old, new = versions(dim_area, "business bay")
    assert rows["BEFORE"]["area_sk"] == old["area_sk"]
    assert rows["AFTER"]["area_sk"] == new["area_sk"]


def test_fact_rents_share_property_types_with_sales(spark):
    m = model(
        spark,
        [sale("S1", property_type_en="Unit", property_sub_type_en="Flat")],
        [rent("R1", ejari_property_type_en="Unit", ejari_property_sub_type_en="Apartment")],
    )
    sales = facts.build_fact_sales(m["transactions"], *dims(m)).first()
    rents = facts.build_fact_rents(m["rents"], *dims(m))
    row = rents.first()

    assert rents.columns == facts.FACT_RENTS_COLUMNS
    assert row["property_type_sk"] == sales["property_type_sk"]
    assert (row["date_key"], row["end_date_key"]) == (20240101, 20241231)
    assert row["area_sk"] != rules.UNKNOWN_SK


def test_empty_silver_has_the_silver_schema(spark):
    rents = empty_silver(spark, "rents")
    assert rents.columns == output_columns("rents")
    assert rents.count() == 0


# --- agg_area_monthly -----------------------------------------------------------------------------


def _market(spark):
    """Business Bay flats: three priced sales and an outlier in Jan 2024, one sale in
    Jan 2023, three single-unit rents and one multi-unit rent in Jan 2024."""
    flat = {"property_type_en": "Unit", "property_sub_type_en": "Flat"}
    ejari_flat = {"ejari_property_type_en": "Unit", "ejari_property_sub_type_en": "Flat"}
    bay = {"area_name_en": "Business Bay", "area_name_ar": "الخليج التجارى"}
    sales = [
        sale("A", actual_worth="1000000", **flat),
        sale("B", actual_worth="1500000", **flat),
        sale("C", actual_worth="2000000", reg_type_en="Off-Plan Properties", **flat),
        sale("OUTLIER", procedure_area="0", **flat),
        sale("LAST_YEAR", instance_date="10-01-2023", actual_worth="1200000", **flat),
        sale("VILLA", property_type_en="Villa", property_sub_type_en="Villa"),
    ]
    rents = [
        rent("R1", annual_amount="90000", contract_amount="90000", **ejari_flat, **bay),
        rent("R2", annual_amount="100000", contract_amount="100000", **ejari_flat, **bay),
        rent("R3", annual_amount="110000", contract_amount="110000", **ejari_flat, **bay),
        rent("MULTI", annual_amount="900000", no_of_prop="10", **ejari_flat, **bay),
    ]
    m = model(spark, sales, rents)
    return (
        facts.build_fact_sales(m["transactions"], *dims(m)),
        facts.build_fact_rents(m["rents"], *dims(m)),
        m,
    )


def _cell(agg, month_key, type_sk):
    [row] = [
        r
        for r in agg.collect()
        if r["month_key"] == month_key
        and r["area_key"] == "business bay"
        and r["property_type_sk"] == type_sk
    ]
    return row


def test_monthly_medians_counts_and_yield(spark):
    fact_sales, fact_rents, m = _market(spark)
    agg = facts.build_agg_area_monthly(fact_sales, fact_rents, m["dim_area"], min_records=3)
    flat_sk = fact_rents.first()["property_type_sk"]
    row = _cell(agg, 202401, flat_sk)

    priced = [
        float(r["price_per_sqft"])
        for r in fact_sales.collect()
        if r["transaction_id"] in {"A", "B", "C"}
    ]
    assert row["sales_count"] == 4  # outlier counted
    assert row["priced_sales_count"] == 3  # but left out of medians
    assert row["offplan_sales_count"] == 1
    assert row["median_price_per_sqft"] == pytest.approx(median(priced), abs=0.01)
    assert row["median_sale_price_aed"] == pytest.approx(1_500_000)
    assert (row["rent_count"], row["priced_rent_count"]) == (4, 3)  # multi-unit left out
    assert row["median_annual_rent_aed"] == pytest.approx(100_000)
    assert row["gross_yield_pct"] == pytest.approx(100_000 / 1_500_000 * 100, abs=0.01)
    assert row["area_name_en"] == "Business Bay"
    assert row["month_start"] == date(2024, 1, 1)


def test_yield_is_withheld_below_the_minimum_sample(spark):
    fact_sales, fact_rents, m = _market(spark)
    agg = facts.build_agg_area_monthly(fact_sales, fact_rents, m["dim_area"], min_records=4)
    row = _cell(agg, 202401, fact_rents.first()["property_type_sk"])
    assert row["gross_yield_pct"] is None
    assert row["median_price_per_sqft"] is not None  # medians are still reported


def test_year_on_year_change_and_all_types_rollup(spark):
    fact_sales, fact_rents, m = _market(spark)
    agg = facts.build_agg_area_monthly(fact_sales, fact_rents, m["dim_area"], min_records=3)
    flat_sk = fact_rents.first()["property_type_sk"]

    now, last_year = _cell(agg, 202401, flat_sk), _cell(agg, 202301, flat_sk)
    expected = (now["median_price_per_sqft"] / last_year["median_price_per_sqft"] - 1) * 100
    assert now["median_price_per_sqft_yoy_pct"] == pytest.approx(expected, abs=0.01)
    assert last_year["median_price_per_sqft_yoy_pct"] is None  # nothing a year earlier

    everything = _cell(agg, 202401, rules.ALL_TYPES_SK)
    assert everything["sales_count"] == 5  # four flats and a villa
    assert everything["rent_count"] == 4
    assert set(agg.columns) == set(facts.AGG_COLUMNS)
