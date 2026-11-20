"""Power BI reads gold: the model's columns and types must match what gold produces,
and the Parquet export must round-trip. Runs with or without Delta."""

from __future__ import annotations

from datetime import date

import pytest
from builders import rent, sale, silver_frame

from dubai_property.gold import dimensions, facts
from dubai_property.gold.export import export_gold
from powerbi_model import read_model, tmdl_type

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def gold_frames(spark_env):
    """Every gold table, built from a few silver rows exactly as the pipeline builds them."""
    spark = spark_env[0]
    transactions = silver_frame(
        spark, [sale("S1", property_type_en="Unit", property_sub_type_en="Flat")], "transactions"
    )
    rents = silver_frame(spark, [rent("R1")], "rents")
    snapshot = transactions.select("area_key", "area_name_en", "area_name_ar").unionByName(
        rents.select("area_key", "area_name_en", "area_name_ar")
    )
    dim_area = dimensions.scd2_area(None, snapshot, date(2026, 11, 20)).localCheckpoint()
    dim_type = dimensions.build_dim_property_type(transactions, rents).localCheckpoint()
    dim_project = dimensions.build_dim_project(transactions, rents).localCheckpoint()
    fact_sales = facts.build_fact_sales(
        transactions, dim_area, dim_type, dim_project
    ).localCheckpoint()
    fact_rents = facts.build_fact_rents(rents, dim_area, dim_type, dim_project).localCheckpoint()
    return {
        "dim_date": dimensions.build_dim_date(spark, date(2024, 1, 1), date(2024, 1, 31)),
        "dim_area": dim_area,
        "dim_property_type": dim_type,
        "dim_project": dim_project,
        "fact_sales": fact_sales,
        "fact_rents": fact_rents,
        "agg_area_monthly": facts.build_agg_area_monthly(fact_sales, fact_rents, dim_area),
    }


@pytest.mark.parametrize("name", sorted(read_model()))
def test_model_columns_match_gold(gold_frames, name):
    model = read_model()[name]
    loaded = {c.name: c.data_type for c in model.columns.values() if not c.expression}
    produced = {column: tmdl_type(dtype) for column, dtype in gold_frames[name].dtypes}
    assert loaded == produced


def test_export_writes_one_parquet_folder_per_table(spark, gold_settings, tmp_path):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_settings.catalog}.{gold_settings.gold_schema}")
    dim_date = dimensions.build_dim_date(spark, date(2024, 1, 1), date(2024, 12, 31))
    dim_date.write.format(gold_settings.table_format).saveAsTable(gold_settings.table("dim_date"))

    exported = export_gold(spark, gold_settings, str(tmp_path))

    assert exported == {"dim_date": 366}  # tables not built yet are skipped
    back = spark.read.parquet(str(tmp_path / "dim_date"))
    assert back.count() == 366
    assert dict(back.dtypes) == dict(dim_date.dtypes)
    assert len(list((tmp_path / "dim_date").glob("*.parquet"))) == 1
