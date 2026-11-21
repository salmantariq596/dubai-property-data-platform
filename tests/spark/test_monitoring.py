"""Monitoring views over the pipeline's own logs. Runs with or without Delta."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from dubai_property import health, jobs, monitoring
from dubai_property.bronze import load_log
from dubai_property.orchestrate import record_step
from dubai_property.quality.runner import run_quality
from dubai_property.silver.pipeline import ensure_processed_loads_table

pytestmark = pytest.mark.spark

NOW = datetime.now().replace(microsecond=0)


def step(spark, pipeline_settings, name, status, hours_ago, error=None):
    finished = NOW - timedelta(hours=hours_ago)
    record = jobs.StepRecord(
        name, status, finished - timedelta(minutes=3), finished, "run-1", "ok", error
    )
    record_step(spark, pipeline_settings.quality, record)


def test_nothing_recorded_yet(spark, pipeline_settings):
    assert monitoring.last_runs(spark, pipeline_settings) is None
    assert monitoring.rows_per_load(spark, pipeline_settings) is None
    assert monitoring.pipeline_health(spark, pipeline_settings, NOW).status == health.STALE


def test_last_runs_and_health(spark, pipeline_settings):
    for name in jobs.STEPS:
        step(spark, pipeline_settings, name, jobs.SUCCESS, hours_ago=30)
    step(spark, pipeline_settings, "silver", jobs.FAILED, hours_ago=2, error="Boom: disk full")

    latest = {
        r["step"]: r["status"] for r in monitoring.last_runs(spark, pipeline_settings).collect()
    }
    assert latest["silver"] == jobs.FAILED
    assert latest["gold"] == jobs.SUCCESS
    assert len(latest) == len(jobs.STEPS)

    result = monitoring.pipeline_health(spark, pipeline_settings, NOW)
    assert result.status == health.FAILED
    assert any("Boom: disk full" in r for r in result.reasons)

    trend = {
        (r["step"], r["failures"])
        for r in monitoring.failure_trend(spark, pipeline_settings).collect()
    }
    assert ("silver", 1) in trend


def test_rows_per_load_from_the_load_log(spark, settings, pipeline_settings):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.bronze_schema}")
    load_log.ensure_load_log_table(spark, settings.load_log_table, settings.table_format)
    entries = [
        load_log.LoadLogEntry(
            "L1",
            "transactions",
            load_log.SUCCESS,
            NOW - timedelta(hours=1),
            NOW,
            source_file=f"f{i}.csv",
            rows_read=100,
            rows_written=100,
            rows_rescued=i,
        )
        for i in range(2)
    ] + [
        load_log.LoadLogEntry("L2", "rents", load_log.NO_NEW_FILES, NOW, NOW),
    ]
    load_log.write_entries(
        spark, settings.load_log_table, entries, table_format=settings.table_format
    )

    [row] = monitoring.rows_per_load(spark, pipeline_settings).collect()  # NO_NEW_FILES left out
    assert (row["load_id"], row["files"], row["rows_read"], row["rows_rescued"]) == (
        "L1",
        2,
        200,
        1,
    )
    assert row["problem"] is None


def test_data_quality_trend_and_warnings_in_health(spark, pipeline_settings):
    for name in jobs.STEPS:
        step(spark, pipeline_settings, name, jobs.SUCCESS, hours_ago=1)
    run_quality(spark, pipeline_settings.quality, "gold", raise_on_fail=False)  # all SKIPPED

    [row] = monitoring.dq_trend(spark, pipeline_settings).collect()
    assert row["stage"] == "gold"
    assert row["SKIPPED"] > 0 and row["FAIL"] == 0
    assert monitoring.pipeline_health(spark, pipeline_settings, NOW).status == health.OK


def test_quarantine_trend(spark, silver_settings, pipeline_settings):
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {silver_settings.catalog}.{silver_settings.silver_schema}"
    )
    ensure_processed_loads_table(spark, silver_settings)
    table = silver_settings.processed_loads_table
    spark.createDataFrame(
        [("transactions", "L1", 200, 5, NOW)], spark.table(table).schema
    ).write.format(silver_settings.table_format).mode("append").saveAsTable(table)

    [row] = monitoring.quarantine_trend(spark, pipeline_settings).collect()
    assert row["quarantined_pct"] == 2.5
