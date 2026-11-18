"""End-to-end bronze tests on a real local Spark session."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dubai_property.bronze import ingest, load_log
from dubai_property.config import get_dataset

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample_transactions.csv"
TRANSACTIONS = get_dataset("transactions")


def land(settings, load_date: str, name: str = "transactions.csv", text: str | None = None):
    """Drop a file into the landing zone, the way ADF will in production."""
    folder = Path(settings.landing_path(TRANSACTIONS)) / f"load_date={load_date}"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / name
    if text is None:
        shutil.copy(FIXTURE, target)
    else:
        target.write_text(text, encoding="utf-8")
    return target


def bronze(spark, settings):
    return spark.table(settings.table(TRANSACTIONS.bronze_table))


def log_rows(spark, settings, **filters):
    df = spark.table(settings.load_log_table)
    for column, value in filters.items():
        df = df.where(F.col(column) == value)
    return df.collect()


# --- the core promise: load once, never twice -----------------------------------


def test_first_run_loads_every_row_with_metadata(spark, settings):
    land(settings, "2026-11-01")
    result = ingest.run_ingestion(spark, settings, "transactions")

    assert result.status == load_log.SUCCESS
    assert (result.files, result.rows_read) == (1, 12)

    df = bronze(spark, settings)
    assert df.count() == 12
    assert set(ingest.METADATA_COLUMNS + [ingest.RESCUED_COLUMN]) <= set(df.columns)

    row = df.select("_load_id", "_load_date", "_source_file").distinct().collect()
    assert len(row) == 1
    assert row[0]["_load_id"] == result.load_id
    assert row[0]["_load_date"] == date(2026, 11, 1)
    assert row[0]["_source_file"].endswith("load_date=2026-11-01/transactions.csv")

    [entry] = log_rows(spark, settings, load_id=result.load_id)
    assert entry["status"] == load_log.SUCCESS
    assert entry["rows_read"] == entry["rows_written"] == 12


def test_running_twice_adds_no_duplicates(spark, settings):
    land(settings, "2026-11-01")
    first = ingest.run_ingestion(spark, settings, "transactions")
    second = ingest.run_ingestion(spark, settings, "transactions")

    assert bronze(spark, settings).count() == 12
    assert second.status == load_log.NO_NEW_FILES
    assert first.load_id != second.load_id
    [entry] = log_rows(spark, settings, load_id=second.load_id)
    assert entry["status"] == load_log.NO_NEW_FILES


def test_only_new_files_are_loaded(spark, settings):
    land(settings, "2026-11-01")
    ingest.run_ingestion(spark, settings, "transactions")

    land(settings, "2026-11-02", name="extra.csv")
    second = ingest.run_ingestion(spark, settings, "transactions")

    assert (second.files, second.rows_read) == (1, 12)
    by_day = dict(
        bronze(spark, settings)
        .groupBy("_load_date")
        .count()
        .select("_load_date", "count")
        .collect()
    )
    assert by_day == {date(2026, 11, 1): 12, date(2026, 11, 2): 12}


def test_empty_landing_zone_is_logged_not_failed(spark, settings):
    result = ingest.run_ingestion(spark, settings, "transactions")
    assert result.status == load_log.NO_NEW_FILES
    assert log_rows(spark, settings, load_id=result.load_id)[0]["status"] == load_log.NO_NEW_FILES


# --- bronze keeps the data exactly as delivered -----------------------------------


def test_values_are_kept_as_raw_strings(spark, settings):
    land(settings, "2026-11-01")
    ingest.run_ingestion(spark, settings, "transactions")
    df = bronze(spark, settings)

    data_columns = [c for c in df.columns if not c.startswith("_")]
    assert all(dtype == "string" for name, dtype in df.dtypes if name in data_columns)

    worth = {r["actual_worth"] for r in df.select("actual_worth").collect()}
    assert {"920,000", "n/a", "0"} <= worth  # nothing parsed, nothing dropped
    assert df.where(F.col("instance_date") == "31-02-2024").count() == 1  # bad date kept


def test_messy_headers_quotes_newlines_and_arabic(spark, settings):
    csv = (
        "﻿Transaction ID,Actual Worth (AED),Building Name,area_name_ar\n"
        'T-1,"1,250,000","Tower ""A"", Marina",مرسى دبي\n'
        'T-2,00980,"Line one\nline two",الخليج التجارى\n'
    )
    land(settings, "2026-11-01", name="messy.csv", text=csv)
    ingest.run_ingestion(spark, settings, "transactions")

    rows = {
        r["transaction_id"]: r
        for r in bronze(spark, settings)
        .select("transaction_id", "actual_worth_aed", "building_name", "area_name_ar")
        .collect()
    }
    assert rows["T-1"]["actual_worth_aed"] == "1,250,000"
    assert rows["T-1"]["building_name"] == 'Tower "A", Marina'  # doubled quote handled
    assert rows["T-2"]["actual_worth_aed"] == "00980"  # leading zeros kept
    assert rows["T-2"]["building_name"] == "Line one\nline two"
    assert rows["T-2"]["area_name_ar"] == "الخليج التجارى"


# --- failures are visible ------------------------------------------------------------


def test_failed_run_is_logged_and_raised(spark, settings, monkeypatch):
    land(settings, "2026-11-01")

    def broken_writer(*_args):
        def write_batch(batch_df, batch_id):
            raise ValueError("storage unavailable")

        return write_batch

    monkeypatch.setattr(ingest, "make_batch_writer", broken_writer)
    with pytest.raises(Exception, match="storage unavailable"):
        ingest.run_ingestion(spark, settings, "transactions", load_id="failing-load")

    [entry] = log_rows(spark, settings, load_id="failing-load")
    assert entry["status"] == load_log.FAILED
    assert "storage unavailable" in entry["error_message"]


# --- Delta-only guarantees (run in CI) --------------------------------------------------


def _static_batch(spark, settings, load_id):
    """The same rows a micro-batch would carry, built without streaming."""
    raw = spark.read.options(**ingest.CSV_OPTIONS).option("recursiveFileLookup", "true")
    return ingest.add_metadata_columns(raw.csv(settings.landing_path(TRANSACTIONS)), load_id)


def test_retried_batch_is_not_written_twice(spark, settings, requires_delta):
    """A crash after the write but before the checkpoint commit replays the batch."""
    land(settings, "2026-11-01")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.bronze_schema}")
    load_log.ensure_load_log_table(spark, settings.load_log_table, settings.table_format)

    write = ingest.make_batch_writer(settings, TRANSACTIONS, "run-1")
    write(_static_batch(spark, settings, "run-1"), 0)
    write(_static_batch(spark, settings, "run-1"), 0)  # same run retries batch 0

    retry = ingest.make_batch_writer(settings, TRANSACTIONS, "run-2")
    retry(_static_batch(spark, settings, "run-2"), 0)  # next run replays batch 0

    assert bronze(spark, settings).count() == 12
    assert len(log_rows(spark, settings)) == 1


def test_bronze_table_rejects_deletes(spark, settings, requires_delta):
    land(settings, "2026-11-01")
    ingest.run_ingestion(spark, settings, "transactions")
    table = settings.table(TRANSACTIONS.bronze_table)

    with pytest.raises(Exception, match="(?i)append"):
        spark.sql(f"DELETE FROM {table} WHERE transaction_id = 'SYN-0001'")
    assert bronze(spark, settings).count() == 12
