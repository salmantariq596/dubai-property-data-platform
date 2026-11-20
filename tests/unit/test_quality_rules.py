"""Data quality checks as plain data: no Spark needed."""

from __future__ import annotations

import pytest

from dubai_property.config import PipelineSettings, QualitySettings
from dubai_property.quality import checks as dq

TABLES = QualitySettings("c").tables()

# --- the catalogue -----------------------------------------------------------------------


def test_check_names_are_unique():
    names = [c.name for c in dq.CHECKS]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("stage", dq.STAGES)
def test_every_stage_has_checks(stage):
    assert dq.checks_for(stage)


@pytest.mark.parametrize("check", dq.CHECKS, ids=lambda c: c.name)
def test_every_check_renders_against_real_tables(check):
    assert check.placeholders <= set(TABLES)
    assert check.table in TABLES
    sql = check.render(TABLES)
    assert "{" not in sql and "}" not in sql
    assert "rows_checked" in sql and "rows_failed" in sql


def test_keys_and_reconciliation_always_fail_the_run():
    """The decisions ADR 0005 documents: broken keys or lost rows stop the pipeline."""
    must_fail = [
        c
        for c in dq.CHECKS
        if c.name.endswith(("_key_not_null", "_key_unique", "_reconciles", "_keys_resolve"))
        or c.name in {"gold_dim_area_one_current_version"}
        or c.name.endswith("_last_load_ok")
    ]
    assert len(must_fail) >= 10
    assert all(c.fail_above == 0 for c in must_fail)


def test_outlier_checks_tolerate_flagged_rows():
    """Zero sizes and values are kept in silver by design: warn first, fail only if common."""
    check = next(c for c in dq.CHECKS if c.name == "silver_transactions_positive_size")
    assert (check.warn_above, check.fail_above) == (0, 20)


def test_row_count_change_matches_the_plan():
    max_change = dq.MAX_ROW_COUNT_CHANGE
    assert max_change == pytest.approx(0.30)
    check = next(c for c in dq.CHECKS if c.name == "bronze_transactions_row_count_change")
    assert (check.warn_above, check.fail_above) == (0, None)
    assert "0.3 * previous.row_total" in check.render(TABLES)


def test_checks_for_unknown_stage():
    with pytest.raises(ValueError, match="bronze"):
        dq.checks_for("platinum")


# --- building checks ------------------------------------------------------------------------


def test_row_rule_sql():
    check = dq.row_rule(
        "r", "silver", "silver_rents", "x > 0", "d", where="y IS NOT NULL", warn_above=0
    )
    assert check.render(TABLES) == (
        "SELECT count(*) AS rows_checked, count_if(NOT coalesce(x > 0, false)) AS rows_failed "
        "FROM c.silver.rents WHERE y IS NOT NULL"
    )


def test_unique_sql_counts_surplus_rows():
    sql = dq.unique("u", "gold", "gold_fact_sales", ["transaction_id"], "d").render(TABLES)
    assert "coalesce(sum(n - 1), 0) AS rows_failed" in sql
    assert "GROUP BY transaction_id" in sql


def test_a_check_needs_a_threshold_and_a_known_stage():
    with pytest.raises(ValueError, match="threshold"):
        dq.Check("x", "silver", "silver_rents", "SELECT 1", "d")
    with pytest.raises(ValueError, match="stage"):
        dq.Check("x", "platinum", "silver_rents", "SELECT 1", "d", fail_above=0)


# --- classification ---------------------------------------------------------------------------

GRADED = dq.Check("g", "silver", "silver_rents", "SELECT 1", "d", warn_above=1, fail_above=20)
STRICT = dq.Check("s", "silver", "silver_rents", "SELECT 1", "d", fail_above=0)
WARN_ONLY = dq.Check("w", "silver", "silver_rents", "SELECT 1", "d", warn_above=0)


@pytest.mark.parametrize(
    ("check", "checked", "failed", "status"),
    [
        (GRADED, 1000, 0, dq.PASS),
        (GRADED, 1000, 5, dq.PASS),  # 0.5% is under the 1% warning line
        (GRADED, 1000, 50, dq.WARN),
        (GRADED, 1000, 200, dq.WARN),  # exactly 20% is not above it
        (GRADED, 1000, 201, dq.FAIL),
        (STRICT, 1_000_000, 1, dq.FAIL),  # one broken key is enough
        (STRICT, 10, 0, dq.PASS),
        (WARN_ONLY, 10, 10, dq.WARN),  # never fails, however bad
        (GRADED, 0, 0, dq.PASS),  # nothing to check yet
    ],
)
def test_classify(check, checked, failed, status):
    assert dq.classify(check, checked, failed) == status


def test_failed_pct():
    assert dq.failed_pct(200, 50) == 25.0
    assert dq.failed_pct(0, 0) == 0.0


def test_worst_status_wins():
    assert dq.worst([dq.PASS, dq.WARN, dq.SKIPPED]) == dq.WARN
    assert dq.worst([dq.WARN, dq.FAIL, dq.PASS]) == dq.FAIL
    assert dq.worst([dq.SKIPPED]) == dq.SKIPPED
    assert dq.worst([]) == dq.PASS


# --- settings -----------------------------------------------------------------------------------


def test_quality_settings():
    s = QualitySettings.for_databricks("dubai")
    assert s.dq_results_table == "dubai.quality.dq_results"
    assert s.tables()["bronze_load_log"] == "dubai.bronze.load_log"
    assert s.tables()["gold_agg_area_monthly"] == "dubai.gold.agg_area_monthly"


def test_pipeline_settings_share_one_catalog():
    s = PipelineSettings.for_databricks("workspace")
    assert {s.bronze.catalog, s.silver.catalog, s.gold.catalog, s.quality.catalog} == {"workspace"}
    assert s.gold.cluster_facts is True
