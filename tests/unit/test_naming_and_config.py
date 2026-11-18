from datetime import datetime

import pytest

from dubai_property.bronze import load_log
from dubai_property.config import DATASETS, BronzeSettings, get_dataset
from dubai_property.utils.naming import sanitize_column_names, to_snake_case

# --- column names ------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("transaction_id", "transaction_id"),
        ("Actual Worth (AED)", "actual_worth_aed"),
        ("  area_name_en ", "area_name_en"),
        ("﻿transaction_id", "transaction_id"),
        ("procedureArea", "procedure_area"),
        ("No. of Parties", "no_of_parties"),
        ("_rescued_data", "rescued_data"),  # leading _ is reserved for metadata
        ("rent;value=x", "rent_value_x"),  # characters Delta rejects
    ],
)
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_duplicates_blanks_and_digits():
    assert sanitize_column_names(["Area", "area", "", "2023 price", "AREA"]) == [
        "area",
        "area_2",
        "column_3",
        "c_2023_price",
        "area_3",
    ]


def test_arabic_only_header_gets_positional_name():
    assert sanitize_column_names(["transaction_id", "اسم المنطقة"]) == [
        "transaction_id",
        "column_2",
    ]


# --- config ------------------------------------------------------------------


def test_databricks_paths():
    s = BronzeSettings.for_databricks("dubai")
    ds = get_dataset("transactions")
    assert s.landing_path(ds) == "/Volumes/dubai/raw/landing/transactions"
    assert s.checkpoint_path(ds) == "/Volumes/dubai/bronze/checkpoints/transactions"
    assert s.table(ds.bronze_table) == "dubai.bronze.dld_transactions_raw"
    assert s.load_log_table == "dubai.bronze.load_log"


def test_app_id_changes_with_checkpoint_generation():
    ds = get_dataset("rents")
    v1 = BronzeSettings("c", "/l", "/c").write_app_id(ds)
    v2 = BronzeSettings("c", "/l", "/c", checkpoint_generation="v2").write_app_id(ds)
    assert v1 != v2


def test_unknown_dataset_and_source_rejected():
    with pytest.raises(KeyError, match="Known"):
        get_dataset("listings")
    with pytest.raises(ValueError, match="source"):
        BronzeSettings("c", "/l", "/c", source="kafka")


def test_every_dataset_has_distinct_bronze_table():
    tables = [d.bronze_table for d in DATASETS.values()]
    assert len(tables) == len(set(tables))


# --- load log ------------------------------------------------------------------


def test_load_log_entry_row_matches_ddl_order():
    entry = load_log.LoadLogEntry(
        load_id="L1",
        dataset="transactions",
        status=load_log.SUCCESS,
        started_at=datetime(2026, 1, 1, 10, 0, 0),
        finished_at=datetime(2026, 1, 1, 10, 0, 2, 500000),
        rows_read=5,
        rows_written=5,
    )
    row = dict(zip(load_log.COLUMNS, entry.as_row(), strict=True))
    assert row["duration_seconds"] == 2.5
    assert row["rows_written"] == 5
    assert len(row) == len(load_log.COLUMNS) == 13


def test_load_log_truncates_long_errors_and_rejects_bad_status():
    t = datetime(2026, 1, 1)
    entry = load_log.LoadLogEntry("L", "d", load_log.FAILED, t, t, error_message="x" * 5000)
    assert len(dict(zip(load_log.COLUMNS, entry.as_row(), strict=True))["error_message"]) == 2000
    with pytest.raises(ValueError):
        load_log.LoadLogEntry("L", "d", "OK", t, t)


def test_file_status():
    assert load_log.file_status(10, 10) == load_log.SUCCESS
    assert load_log.file_status(10, 9) == load_log.MISMATCH
