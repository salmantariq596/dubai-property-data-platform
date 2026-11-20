"""Run the data quality checks of one stage and record them in ``dq_results``.

Results are written before anything is raised, so a failed run always leaves
its evidence behind: which rule, which table, how many rows, which load.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from dubai_property.config import QualitySettings
from dubai_property.quality import checks as dq

DQ_RESULTS_DDL = """
    run_id STRING NOT NULL,
    load_id STRING,
    stage STRING NOT NULL,
    rule STRING NOT NULL,
    table_name STRING NOT NULL,
    description STRING,
    rows_checked BIGINT,
    rows_failed BIGINT,
    failed_pct DOUBLE,
    warn_above DOUBLE,
    fail_above DOUBLE,
    status STRING NOT NULL,
    checked_at TIMESTAMP NOT NULL
"""
COLUMNS = [line.split()[0] for line in DQ_RESULTS_DDL.strip().splitlines()]


@dataclass(frozen=True)
class CheckResult:
    rule: str
    stage: str
    table_name: str
    description: str
    rows_checked: int
    rows_failed: int
    status: str
    warn_above: float | None
    fail_above: float | None

    @property
    def failed_pct(self) -> float:
        return dq.failed_pct(self.rows_checked, self.rows_failed)

    def __str__(self) -> str:
        return (
            f"{self.status:<7} {self.rule}: {self.rows_failed:,} of {self.rows_checked:,} "
            f"rows failed ({self.failed_pct:.2f}%)"
        )


@dataclass(frozen=True)
class QualityReport:
    run_id: str
    stage: str
    results: tuple[CheckResult, ...]

    @property
    def status(self) -> str:
        return dq.worst([r.status for r in self.results])

    def by_status(self, status: str) -> list[CheckResult]:
        return [r for r in self.results if r.status == status]

    def __str__(self) -> str:
        lines = [f"[{self.status}] {self.stage} data quality, run {self.run_id}"]
        lines += [f"  {r}" for r in self.results if r.status != dq.SKIPPED]
        skipped = len(self.by_status(dq.SKIPPED))
        if skipped:
            lines.append(f"  ({skipped} check(s) skipped: table not built yet)")
        return "\n".join(lines)


class DataQualityError(RuntimeError):
    """A FAIL check: the pipeline stops before the next layer."""

    def __init__(self, report: QualityReport):
        self.report = report
        failed = ", ".join(r.rule for r in report.by_status(dq.FAIL))
        super().__init__(f"Data quality failed at {report.stage}: {failed}\n{report}")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def evaluate(spark: SparkSession, check: dq.Check, tables: dict[str, str]) -> CheckResult:
    """Run one check. Missing tables (a dataset not loaded yet) give SKIPPED."""
    table_name = tables[check.table]
    if not all(spark.catalog.tableExists(tables[p]) for p in check.placeholders):
        checked = failed = 0
        status = dq.SKIPPED
    else:
        row = spark.sql(check.render(tables)).first()
        checked, failed = int(row["rows_checked"] or 0), int(row["rows_failed"] or 0)
        status = dq.classify(check, checked, failed)
    return CheckResult(
        rule=check.name,
        stage=check.stage,
        table_name=table_name,
        description=check.description,
        rows_checked=checked,
        rows_failed=failed,
        status=status,
        warn_above=check.warn_above,
        fail_above=check.fail_above,
    )


def ensure_dq_results_table(spark: SparkSession, settings: QualitySettings) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.quality_schema}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {settings.dq_results_table} ({DQ_RESULTS_DDL}) "
        f"USING {settings.table_format}"
    )


def record(
    spark: SparkSession, settings: QualitySettings, report: QualityReport, load_id: str | None
) -> None:
    ensure_dq_results_table(spark, settings)
    checked_at = _now()
    rows = [
        (
            report.run_id,
            load_id,
            r.stage,
            r.rule,
            r.table_name,
            r.description,
            r.rows_checked,
            r.rows_failed,
            r.failed_pct,
            None if r.warn_above is None else float(r.warn_above),
            None if r.fail_above is None else float(r.fail_above),
            r.status,
            checked_at,
        )
        for r in report.results
    ]
    table = settings.dq_results_table
    df = spark.createDataFrame(rows, schema=spark.table(table).schema)
    df.write.format(settings.table_format).mode("append").saveAsTable(table)


def run_quality(
    spark: SparkSession,
    settings: QualitySettings,
    stage: str,
    load_id: str | None = None,
    raise_on_fail: bool = True,
    checks: list[dq.Check] | None = None,
) -> QualityReport:
    """Run every check of ``stage``, record the results, and stop on FAIL.

    ``load_id`` ties the results to the pipeline run that produced the data
    (Data Factory passes its run id).
    """
    tables = settings.tables()
    selected = dq.checks_for(stage) if checks is None else checks
    report = QualityReport(
        run_id=f"dq-{stage}-{_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}",
        stage=stage,
        results=tuple(evaluate(spark, check, tables) for check in selected),
    )
    record(spark, settings, report, load_id)
    if raise_on_fail and report.status == dq.FAIL:
        raise DataQualityError(report)
    return report
