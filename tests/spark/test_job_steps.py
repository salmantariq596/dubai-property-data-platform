"""Each step of the scheduled job logs to quality.pipeline_runs, and failures still fail."""

from __future__ import annotations

import pytest

from dubai_property import jobs, orchestrate
from dubai_property.quality.runner import DataQualityError

pytestmark = pytest.mark.spark


def logged(spark, pipeline_settings):
    table = pipeline_settings.quality.pipeline_runs_table
    return spark.table(table).orderBy("started_at").collect()


def test_a_successful_step_is_logged_with_its_summary(spark, pipeline_settings):
    summary = orchestrate.run_step(spark, pipeline_settings, "gold", load_id="adf-1")

    [row] = logged(spark, pipeline_settings)
    assert (row["step"], row["status"], row["load_id"]) == ("gold", jobs.SUCCESS, "adf-1")
    assert "NO_SILVER" in row["summary"] == summary  # nothing to build yet is not a failure
    assert row["error_message"] is None
    assert row["duration_seconds"] >= 0


def test_a_failing_step_is_logged_then_raised(spark, pipeline_settings, monkeypatch):
    def broken(*_args):
        raise RuntimeError("storage unavailable")

    monkeypatch.setitem(orchestrate.STEP_RUNNERS, "silver", broken)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        orchestrate.run_step(spark, pipeline_settings, "silver", load_id="adf-2")

    [row] = logged(spark, pipeline_settings)
    assert (row["step"], row["status"]) == ("silver", jobs.FAILED)
    assert row["error_message"] == "RuntimeError: storage unavailable"


def test_a_failed_quality_check_stops_the_job(spark, pipeline_settings, monkeypatch):
    class Report:
        stage = "silver"

        def by_status(self, status):
            return []

    def failing_checks(*_args, **_kwargs):
        raise DataQualityError(Report())

    monkeypatch.setattr(orchestrate, "run_quality", failing_checks)
    with pytest.raises(DataQualityError):
        orchestrate.run_step(spark, pipeline_settings, "quality_silver", load_id="adf-3")

    [row] = logged(spark, pipeline_settings)
    assert (row["step"], row["status"]) == ("quality_silver", jobs.FAILED)
    assert row["error_message"].startswith("DataQualityError")


def test_quality_steps_run_their_own_stage(spark, pipeline_settings):
    """Nothing built yet: every check is skipped, and skipping is not a failure."""
    summary = orchestrate.run_step(spark, pipeline_settings, "quality_gold")
    assert "gold data quality" in summary
    [row] = logged(spark, pipeline_settings)
    assert (row["step"], row["status"]) == ("quality_gold", jobs.SUCCESS)


def test_every_job_step_has_a_runner():
    assert set(orchestrate.STEP_RUNNERS) == set(jobs.STEPS)
    with pytest.raises(ValueError, match="Unknown step"):
        orchestrate.run_step(None, None, "platinum")
