"""Gold end to end: bronze -> silver -> gold tables. Needs Delta (runs in CI)."""

from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dubai_property.bronze import ingest
from dubai_property.config import get_dataset
from dubai_property.gold import pipeline, rules
from dubai_property.silver import pipeline as silver_pipeline

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample_transactions.csv"
FIRST_RUN = date(2026, 11, 20)
RENAMED_ON = date(2026, 12, 1)

# Fixture sales that pass silver: everything with trans_group Sales except the
# two quarantined rows (SYN-0007 bad date, SYN-0008 "n/a" value).
FIXTURE_SALES = 7


@pytest.fixture(autouse=True)
def _delta_only(requires_delta):
    """Gold reads silver, which needs Delta MERGE."""


@pytest.fixture
def silver_ready(spark, settings, silver_settings):
    folder = Path(settings.landing_path(get_dataset("transactions"))) / "load_date=2026-11-01"
    folder.mkdir(parents=True)
    shutil.copy(FIXTURE, folder / "transactions.csv")
    ingest.run_ingestion(spark, settings, "transactions")
    silver_pipeline.run_silver(spark, silver_settings, "transactions")


def table(spark, gold_settings, name):
    return spark.table(gold_settings.table(name))


def area_versions(spark, gold_settings, key):
    return (
        table(spark, gold_settings, "dim_area")
        .where(F.col("area_key") == key)
        .orderBy("valid_from")
        .collect()
    )


def test_gold_is_built_from_silver(spark, gold_settings, silver_ready):
    result = pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN)

    assert result.status == pipeline.SUCCESS
    assert result.rows["fact_sales"] == FIXTURE_SALES
    assert result.rows["fact_rents"] == 0  # no rents loaded yet: gold still builds
    assert table(spark, gold_settings, "dim_date").count() == 11_323

    areas = spark.table(f"{gold_settings.catalog}.{gold_settings.silver_schema}.areas").count()
    assert result.rows["dim_area"] == areas + 1  # plus the Unknown row

    agg = table(spark, gold_settings, "agg_area_monthly")
    january = agg.where(
        (F.col("month_key") == 202401)
        & (F.col("area_key") == "business bay")
        & (F.col("property_type_sk") == rules.ALL_TYPES_SK)
    ).first()
    assert january["sales_count"] == 3  # SYN-0001, SYN-0006, SYN-0011

    orphans = (
        table(spark, gold_settings, "fact_sales")
        .join(table(spark, gold_settings, "dim_area"), "area_sk", "left_anti")
        .count()
    )
    assert orphans == 0  # every fact finds its area row


def test_rerun_changes_nothing(spark, gold_settings, silver_ready):
    pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN)
    first = sorted(tuple(r) for r in table(spark, gold_settings, "dim_area").collect())
    again = pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN + timedelta(days=1))

    assert sorted(tuple(r) for r in table(spark, gold_settings, "dim_area").collect()) == first
    assert again.rows["fact_sales"] == FIXTURE_SALES


def test_area_rename_keeps_history_and_old_facts(
    spark, silver_settings, gold_settings, silver_ready
):
    pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN)
    spark.sql(
        f"UPDATE {silver_settings.areas_table} SET area_name_en = 'Business Bay District' "
        "WHERE area_key = 'business bay'"
    )
    pipeline.run_gold(spark, gold_settings, as_of=RENAMED_ON)

    old, new = area_versions(spark, gold_settings, "business bay")
    assert (old["area_name_en"], old["is_current"]) == ("Business Bay", False)
    assert (new["area_name_en"], new["valid_from"]) == ("Business Bay District", RENAMED_ON)

    # January 2024 sales happened under the old name, so they keep the old version
    sale_sks = {
        r["area_sk"]
        for r in table(spark, gold_settings, "fact_sales")
        .where(F.col("area_key") == "business bay")
        .collect()
    }
    assert sale_sks == {old["area_sk"]}

    # The monthly aggregate reports areas under their current name
    names = {
        r["area_name_en"]
        for r in table(spark, gold_settings, "agg_area_monthly")
        .where(F.col("area_key") == "business bay")
        .collect()
    }
    assert names == {"Business Bay District"}


def test_optimize_and_benchmark(spark, gold_settings, silver_ready):
    pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN)
    optimized = pipeline.optimize_facts(spark, gold_settings)
    assert optimized == [gold_settings.table(t) for t in pipeline.FACT_TABLES]

    seconds = pipeline.time_query(
        spark, f"SELECT count(*) FROM {gold_settings.table('fact_sales')}", repeats=2
    )
    assert 0 <= seconds < 60
    assert table(spark, gold_settings, "fact_sales").count() == FIXTURE_SALES


def test_without_silver_there_is_nothing_to_build(spark, gold_settings):
    result = pipeline.run_gold(spark, gold_settings, as_of=FIRST_RUN)
    assert result.status == pipeline.NO_SILVER
