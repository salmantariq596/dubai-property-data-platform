"""The whole pipeline on the synthetic fixtures: bronze -> checks -> silver -> checks ->
gold -> checks. Needs Delta (runs in CI)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dubai_property.config import get_dataset
from dubai_property.orchestrate import run_pipeline
from dubai_property.quality import checks as dq
from dubai_property.quality.runner import DataQualityError

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).parents[1] / "fixtures"
TRANSACTIONS_CSV = FIXTURES / "sample_transactions.csv"
RENTS_CSV = FIXTURES / "sample_rents.csv"

# Expected results for the fixtures (see tests/fixtures/README.md)
SALES_IN_GOLD = 7  # 9 sales rows, 2 quarantined
CONTRACTS_IN_GOLD = 8  # 11 rows, 2 quarantined, 1 duplicate


@pytest.fixture(autouse=True)
def _delta_only(requires_delta):
    """Silver MERGEs and gold needs Delta."""


def land(settings, dataset: str, load_date: str, source: Path | None = None, text: str = ""):
    folder = Path(settings.landing_path(get_dataset(dataset))) / f"load_date={load_date}"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{dataset}.csv"
    if source is not None:
        shutil.copy(source, target)
    else:
        target.write_text(text, encoding="utf-8")


def gold_count(spark, pipeline_settings, table: str) -> int:
    return spark.table(pipeline_settings.gold.table(table)).count()


def dq_rows(spark, pipeline_settings):
    return spark.table(pipeline_settings.quality.dq_results_table)


@pytest.fixture
def first_delivery(settings):
    land(settings, "transactions", "2026-11-01", TRANSACTIONS_CSV)
    land(settings, "rents", "2026-11-01", RENTS_CSV)


def test_fixtures_flow_from_bronze_to_gold(spark, pipeline_settings, first_delivery):
    result = run_pipeline(spark, pipeline_settings, load_id="adf-run-1")

    assert [(r.dataset, r.rows_read) for r in result.bronze] == [
        ("transactions", 12),
        ("rents", 11),
    ]
    assert [(r.dataset, r.rows_merged, r.rows_quarantined) for r in result.silver] == [
        ("transactions", 9, 2),
        ("rents", 8, 2),
    ]
    assert gold_count(spark, pipeline_settings, "fact_sales") == SALES_IN_GOLD
    assert gold_count(spark, pipeline_settings, "fact_rents") == CONTRACTS_IN_GOLD

    # Every check ran and was recorded; quarantined rows warn but do not stop the run
    statuses = {q.stage: q.status for q in result.quality}
    assert statuses == {"bronze": dq.PASS, "silver": dq.WARN, "gold": dq.PASS}
    recorded = dq_rows(spark, pipeline_settings)
    assert recorded.count() == len(dq.CHECKS)
    assert recorded.where(F.col("status") == dq.SKIPPED).count() == 0
    assert {r["load_id"] for r in recorded.select("load_id").distinct().collect()} == {"adf-run-1"}

    # Sales and rents meet in the monthly aggregate for the same area
    bay = (
        spark.table(pipeline_settings.gold.table("agg_area_monthly"))
        .where(
            (F.col("area_key") == "business bay")
            & (F.col("month_key") == 202401)
            & (F.col("property_type_sk") == 0)
        )
        .first()
    )
    assert bay["sales_count"] == 3
    assert bay["rent_count"] == 4  # SYN-R-0001, -0004, -0007, -0010


def test_a_bad_load_is_stopped_before_gold(spark, settings, pipeline_settings, first_delivery):
    run_pipeline(spark, pipeline_settings)

    header = TRANSACTIONS_CSV.read_text(encoding="utf-8").splitlines()[0]
    garbled = "\n".join(
        [header]
        + [
            f"SYN-09{i},45-45-2024,Sales,Sell,Unit,Flat,Existing Properties,"
            "Business Bay,الخليج التجارى,80.0,1000000,11614.00"
            for i in range(4)
        ]
        + [
            "SYN-0950,20-01-2024,Sales,Sell,Unit,Flat,Existing Properties,"
            "Business Bay,الخليج التجارى,80.0,1000000,11614.00"
        ]
    )
    land(settings, "transactions", "2026-11-02", text=garbled + "\n")

    with pytest.raises(DataQualityError) as stopped:
        run_pipeline(spark, pipeline_settings, load_id="adf-run-2")

    report = stopped.value.report
    assert report.stage == "silver"
    assert [r.rule for r in report.by_status(dq.FAIL)] == ["silver_transactions_quarantine_rate"]

    # Gold still shows the last good data: the bad load never reached it
    assert gold_count(spark, pipeline_settings, "fact_sales") == SALES_IN_GOLD

    # The evidence is recorded: bronze warned about the size change, silver failed
    second = dq_rows(spark, pipeline_settings).where(F.col("load_id") == "adf-run-2")
    by_rule = {r["rule"]: r["status"] for r in second.collect()}
    assert by_rule["bronze_transactions_row_count_change"] == dq.WARN  # 12 rows, then 5
    assert by_rule["silver_transactions_quarantine_rate"] == dq.FAIL  # 4 of 5 rejected
    assert not any(rule.startswith("gold_") for rule in by_rule)


def test_running_again_changes_nothing(spark, pipeline_settings, first_delivery):
    run_pipeline(spark, pipeline_settings)
    again = run_pipeline(spark, pipeline_settings)

    assert {r.status for r in again.bronze} == {"NO_NEW_FILES"}
    assert {r.status for r in again.silver} == {"NO_NEW_LOADS"}
    assert gold_count(spark, pipeline_settings, "fact_sales") == SALES_IN_GOLD
    assert gold_count(spark, pipeline_settings, "fact_rents") == CONTRACTS_IN_GOLD
    assert dq.FAIL not in {q.status for q in again.quality}
