"""Data quality checks against small tables built to break them. Runs with or without Delta."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from dubai_property.bronze import load_log
from dubai_property.quality import checks as dq
from dubai_property.quality.runner import DataQualityError, evaluate, run_quality

pytestmark = pytest.mark.spark

T0 = datetime(2026, 11, 1, 9, 0)
SILVER_TRANSACTIONS = (
    "transaction_id string, is_area_mapped boolean, instance_date date, "
    "size_sqft decimal(18,2), actual_worth decimal(18,2)"
)


def put(spark, quality_settings, placeholder, rows, schema):
    """Create the table behind ``placeholder`` with ``rows``."""
    name = quality_settings.tables()[placeholder]
    catalog, schema_name, _ = name.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
    (
        spark.createDataFrame(rows, schema)
        .write.format(quality_settings.table_format)
        .mode("overwrite")
        .saveAsTable(name)
    )


def run(spark, quality_settings, name):
    check = next(c for c in dq.CHECKS if c.name == name)
    return evaluate(spark, check, quality_settings.tables())


def sale(key, mapped=True, day=date(2024, 1, 5), size="100", worth="1000000"):
    return (key, mapped, day, Decimal(size), Decimal(worth))


# --- silver ---------------------------------------------------------------------------------


def test_broken_keys_fail(spark, quality_settings):
    put(
        spark,
        quality_settings,
        "silver_transactions",
        [sale("T1"), sale("T1"), sale(None)],
        SILVER_TRANSACTIONS,
    )
    not_null = run(spark, quality_settings, "silver_transactions_key_not_null")
    assert (not_null.rows_checked, not_null.rows_failed, not_null.status) == (3, 1, dq.FAIL)

    unique = run(spark, quality_settings, "silver_transactions_key_unique")
    assert (unique.rows_checked, unique.rows_failed, unique.status) == (3, 1, dq.FAIL)


def test_a_few_outliers_warn_and_many_fail(spark, quality_settings):
    few = [sale(f"T{i}") for i in range(9)] + [sale("ZERO", size="0", mapped=False)]
    put(spark, quality_settings, "silver_transactions", few, SILVER_TRANSACTIONS)
    assert run(spark, quality_settings, "silver_transactions_positive_size").status == dq.WARN
    assert run(spark, quality_settings, "silver_transactions_area_mapped").status == dq.WARN
    assert run(spark, quality_settings, "silver_transactions_key_unique").status == dq.PASS

    many = [sale("A"), sale("B", size="0"), sale("C", size="0")]
    put(spark, quality_settings, "silver_transactions", many, SILVER_TRANSACTIONS)
    result = run(spark, quality_settings, "silver_transactions_positive_size")
    assert (result.rows_failed, result.status) == (2, dq.FAIL)


def test_dates_outside_the_calendar(spark, quality_settings):
    rows = [sale(f"T{i}") for i in range(9)] + [sale("OLD", day=date(1999, 12, 31))]
    put(spark, quality_settings, "silver_transactions", rows, SILVER_TRANSACTIONS)
    assert run(spark, quality_settings, "silver_transactions_date_in_calendar").status == dq.FAIL

    future = [sale("SOON", day=date.today() + timedelta(days=400))]
    put(spark, quality_settings, "silver_transactions", future, SILVER_TRANSACTIONS)
    assert run(spark, quality_settings, "silver_transactions_date_not_future").status == dq.WARN


def test_quarantine_rate_reads_only_the_latest_silver_run(spark, silver_settings, quality_settings):
    from dubai_property.silver.pipeline import ensure_processed_loads_table

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {silver_settings.catalog}.{silver_settings.silver_schema}"
    )
    ensure_processed_loads_table(spark, silver_settings)
    table = silver_settings.processed_loads_table
    rows = [
        ("transactions", "L1", 100, 50, T0),  # a bad old run, already dealt with
        ("transactions", "L2", 100, 5, T0 + timedelta(days=1)),
        ("rents", "L3", 10, 9, T0 + timedelta(days=2)),
    ]
    (
        spark.createDataFrame(rows, spark.table(table).schema)
        .write.format(silver_settings.table_format)
        .mode("append")
        .saveAsTable(table)
    )
    result = run(spark, quality_settings, "silver_transactions_quarantine_rate")
    assert (result.rows_checked, result.rows_failed, result.status) == (100, 5, dq.WARN)
    assert run(spark, quality_settings, "silver_rents_quarantine_rate").status == dq.FAIL


def test_checks_on_tables_not_built_yet_are_skipped(spark, quality_settings):
    result = run(spark, quality_settings, "silver_rents_key_not_null")
    assert (result.status, result.rows_checked) == (dq.SKIPPED, 0)


# --- bronze ---------------------------------------------------------------------------------


def _log(spark, settings, load_id, rows, finished, status=load_log.SUCCESS):
    entry = load_log.LoadLogEntry(
        load_id=load_id,
        dataset="transactions",
        status=status,
        started_at=finished - timedelta(minutes=5),
        finished_at=finished,
        source_file=None if status == load_log.FAILED else f"{load_id}.csv",
        rows_read=rows,
        rows_written=rows,
    )
    load_log.write_entries(
        spark, settings.load_log_table, [entry], table_format=settings.table_format
    )


def test_row_count_change_and_last_load(spark, settings, quality_settings):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.bronze_schema}")
    load_log.ensure_load_log_table(spark, settings.load_log_table, settings.table_format)

    _log(spark, settings, "A", 100, T0)
    first = run(spark, quality_settings, "bronze_transactions_row_count_change")
    assert (first.rows_checked, first.status) == (0, dq.PASS)  # nothing to compare yet

    _log(spark, settings, "B", 60, T0 + timedelta(days=1))  # 40% fewer rows
    assert run(spark, quality_settings, "bronze_transactions_row_count_change").status == dq.WARN

    _log(spark, settings, "C", 70, T0 + timedelta(days=2))  # within 30% of B
    assert run(spark, quality_settings, "bronze_transactions_row_count_change").status == dq.PASS
    assert run(spark, quality_settings, "bronze_transactions_last_load_ok").status == dq.PASS

    _log(spark, settings, "D", None, T0 + timedelta(days=3), status=load_log.FAILED)
    assert run(spark, quality_settings, "bronze_transactions_last_load_ok").status == dq.FAIL


# --- gold ---------------------------------------------------------------------------------------


def test_orphan_keys_and_double_current_versions_fail(spark, quality_settings):
    put(
        spark,
        quality_settings,
        "gold_dim_area",
        [(1, "a", True), (2, "a", True), (3, "b", True), (-1, None, True)],
        "area_sk bigint, area_key string, is_current boolean",
    )
    put(
        spark, quality_settings, "gold_dim_property_type", [(10,), (-1,)], "property_type_sk bigint"
    )
    put(spark, quality_settings, "gold_dim_project", [(20,), (-1,)], "project_sk bigint")
    put(
        spark,
        quality_settings,
        "gold_fact_sales",
        [("F1", 3, 10, 20), ("F2", 999, 10, 20), ("F3", -1, -1, -1)],
        "transaction_id string, area_sk bigint, property_type_sk bigint, project_sk bigint",
    )

    current = run(spark, quality_settings, "gold_dim_area_one_current_version")
    assert (current.rows_checked, current.rows_failed, current.status) == (2, 1, dq.FAIL)

    orphans = run(spark, quality_settings, "gold_fact_sales_keys_resolve")
    assert (orphans.rows_checked, orphans.rows_failed, orphans.status) == (3, 1, dq.FAIL)

    unknown = run(spark, quality_settings, "gold_fact_sales_known_area")
    assert unknown.status == dq.WARN  # 1 of 3 sales has no area


def test_only_published_yields_are_checked(spark, quality_settings):
    put(
        spark,
        quality_settings,
        "gold_agg_area_monthly",
        [(5.0,), (25.0,), (None,)],
        "gross_yield_pct double",
    )
    result = run(spark, quality_settings, "gold_agg_yield_plausible")
    assert (result.rows_checked, result.rows_failed, result.status) == (2, 1, dq.WARN)


# --- the runner -----------------------------------------------------------------------------------


def test_results_are_recorded_then_the_run_stops(spark, quality_settings):
    put(
        spark,
        quality_settings,
        "silver_transactions",
        [sale("T1"), sale("T1")],
        SILVER_TRANSACTIONS,
    )

    report = run_quality(spark, quality_settings, "silver", load_id="adf-1", raise_on_fail=False)
    assert report.status == dq.FAIL
    assert {r.rule for r in report.by_status(dq.FAIL)} == {"silver_transactions_key_unique"}

    recorded = spark.table(quality_settings.dq_results_table).collect()
    assert len(recorded) == len(dq.checks_for("silver"))
    assert {r["load_id"] for r in recorded} == {"adf-1"}
    assert {r["run_id"] for r in recorded} == {report.run_id}

    with pytest.raises(DataQualityError, match="silver_transactions_key_unique") as raised:
        run_quality(spark, quality_settings, "silver")
    assert raised.value.report.stage == "silver"
    # the failed run was recorded before it stopped
    assert spark.table(quality_settings.dq_results_table).count() == 2 * len(recorded)
