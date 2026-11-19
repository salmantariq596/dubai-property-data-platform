"""Silver end to end: bronze tables in, MERGE into silver. Needs Delta (runs in CI)."""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dubai_property.bronze import ingest
from dubai_property.config import get_dataset
from dubai_property.silver import pipeline

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample_transactions.csv"
TRANSACTIONS = get_dataset("transactions")
HEADER = FIXTURE.read_text(encoding="utf-8").splitlines()[0]

# A later delivery: SYN-0001 re-registered at a new price, plus one new sale
CORRECTION = "\n".join(
    [
        HEADER,
        "SYN-0001,05-01-2024,Sales,Sell,Unit,Flat,Existing Properties,"
        "Business Bay,الخليج التجارى,85.5,1500000,17543.86",
        "SYN-0100,20-01-2024,Sales,Sell,Unit,Flat,Existing Properties,"
        "Dubai Marina,مرسى دبي,100.0,2000000,20000.00",
    ]
)


@pytest.fixture(autouse=True)
def _delta_only(requires_delta):
    """Every test here MERGEs, which needs Delta."""


def load_bronze(spark, settings, load_date: str, text: str | None = None) -> str:
    folder = Path(settings.landing_path(TRANSACTIONS)) / f"load_date={load_date}"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / "transactions.csv"
    if text is None:
        shutil.copy(FIXTURE, target)
    else:
        target.write_text(text + "\n", encoding="utf-8")
    return ingest.run_ingestion(spark, settings, "transactions").load_id


def silver(spark, silver_settings):
    return spark.table(silver_settings.silver_table(TRANSACTIONS))


def worth(spark, silver_settings, transaction_id):
    [row] = (
        silver(spark, silver_settings)
        .where(F.col("transaction_id") == transaction_id)
        .select("actual_worth")
        .collect()
    )
    return row["actual_worth"]


def test_first_run_fills_silver_quarantine_areas_and_log(spark, settings, silver_settings):
    load_id = load_bronze(spark, settings, "2026-11-01")
    result = pipeline.run_silver(spark, silver_settings, "transactions")

    assert result.status == pipeline.SUCCESS
    assert result.load_ids == (load_id,)
    assert (result.rows_read, result.rows_quarantined) == (12, 2)
    assert (result.rows_merged, result.rows_duplicate) == (9, 1)

    assert silver(spark, silver_settings).count() == 9
    assert spark.table(silver_settings.quarantine_table).count() == 2

    areas = {r["area_key"]: r for r in spark.table(silver_settings.areas_table).collect()}
    assert areas["business bay"]["area_name_en"] == "Business Bay"
    assert areas["business bay"]["transactions"] == 3
    assert "Business Bay" in areas["business bay"]["area_name_en_variants"]

    [logged] = spark.table(silver_settings.processed_loads_table).collect()
    assert (logged["load_id"], logged["rows_read"], logged["rows_quarantined"]) == (
        load_id,
        12,
        2,
    )


def test_rerun_processes_nothing(spark, settings, silver_settings):
    load_bronze(spark, settings, "2026-11-01")
    pipeline.run_silver(spark, silver_settings, "transactions")
    again = pipeline.run_silver(spark, silver_settings, "transactions")

    assert again.status == pipeline.NO_NEW_LOADS
    assert silver(spark, silver_settings).count() == 9
    assert spark.table(silver_settings.processed_loads_table).count() == 1


def test_new_load_updates_changed_keys_and_adds_new_ones(spark, settings, silver_settings):
    load_bronze(spark, settings, "2026-11-01")
    pipeline.run_silver(spark, silver_settings, "transactions")

    correction = load_bronze(spark, settings, "2026-11-02", CORRECTION)
    result = pipeline.run_silver(spark, silver_settings, "transactions")

    assert result.load_ids == (correction,)
    assert silver(spark, silver_settings).count() == 10
    assert worth(spark, silver_settings, "SYN-0001") == Decimal("1500000.00")
    assert worth(spark, silver_settings, "SYN-0100") == Decimal("2000000.00")


def test_reprocessing_a_load_adds_no_duplicates(spark, settings, silver_settings):
    """A run that dies after MERGE but before logging is simply run again."""
    load_bronze(spark, settings, "2026-11-01")
    pipeline.run_silver(spark, silver_settings, "transactions")
    spark.sql(f"DELETE FROM {silver_settings.processed_loads_table}")

    replay = pipeline.run_silver(spark, silver_settings, "transactions")

    assert replay.status == pipeline.SUCCESS
    assert silver(spark, silver_settings).count() == 9
    assert spark.table(silver_settings.quarantine_table).count() == 2


def test_replaying_an_old_load_never_overwrites_newer_data(spark, settings, silver_settings):
    first = load_bronze(spark, settings, "2026-11-01")
    load_bronze(spark, settings, "2026-11-02", CORRECTION)
    pipeline.run_silver(spark, silver_settings, "transactions")
    assert worth(spark, silver_settings, "SYN-0001") == Decimal("1500000.00")

    spark.sql(f"DELETE FROM {silver_settings.processed_loads_table} WHERE load_id = '{first}'")
    pipeline.run_silver(spark, silver_settings, "transactions")  # only the older load

    assert worth(spark, silver_settings, "SYN-0001") == Decimal("1500000.00")
    assert silver(spark, silver_settings).count() == 10


def test_missing_bronze_table_is_not_an_error(spark, silver_settings):
    result = pipeline.run_silver(spark, silver_settings, "rents")
    assert result.status == pipeline.NO_NEW_LOADS
