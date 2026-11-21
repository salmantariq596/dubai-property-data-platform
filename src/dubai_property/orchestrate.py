"""The whole pipeline in one call: bronze -> checks -> silver -> checks -> gold -> checks.

Data quality sits between the layers. A FAIL check raises
:class:`~dubai_property.quality.runner.DataQualityError` and stops the run
before the next layer. If silver fails its checks, gold is not rebuilt, and
the dashboard keeps showing the last good data. Every step is safe to re-run,
so after fixing the cause the whole pipeline is simply run again.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from dubai_property import jobs
from dubai_property.bronze.ingest import IngestionResult, run_ingestion
from dubai_property.config import PipelineSettings, QualitySettings
from dubai_property.gold.pipeline import GoldResult, run_gold
from dubai_property.quality.runner import QualityReport, run_quality
from dubai_property.silver.pipeline import SilverResult, run_silver

DATASETS = ("transactions", "rents")


@dataclass
class PipelineResult:
    bronze: list[IngestionResult] = field(default_factory=list)
    silver: list[SilverResult] = field(default_factory=list)
    gold: GoldResult | None = None
    quality: list[QualityReport] = field(default_factory=list)

    def __str__(self) -> str:
        parts = [str(r) for r in self.bronze + self.silver]
        parts += [str(self.gold)] if self.gold else []
        parts += [str(q) for q in self.quality]
        return "\n".join(parts)


def run_pipeline(
    spark: SparkSession,
    settings: PipelineSettings,
    datasets: tuple[str, ...] = DATASETS,
    load_id: str | None = None,
) -> PipelineResult:
    """Run every layer for ``datasets``. ``load_id`` tags the run (e.g. a Data Factory run id)."""
    result = PipelineResult()

    for name in datasets:
        bronze_id = f"{load_id}-{name}" if load_id else None
        result.bronze.append(run_ingestion(spark, settings.bronze, name, load_id=bronze_id))
    result.quality.append(run_quality(spark, settings.quality, "bronze", load_id=load_id))

    for name in datasets:
        result.silver.append(run_silver(spark, settings.silver, name))
    result.quality.append(run_quality(spark, settings.quality, "silver", load_id=load_id))

    result.gold = run_gold(spark, settings.gold)
    result.quality.append(run_quality(spark, settings.quality, "gold", load_id=load_id))
    return result


# --- one step at a time: the scheduled Databricks job ------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record_step(spark: SparkSession, settings: QualitySettings, record: jobs.StepRecord) -> None:
    """Append one row to ``quality.pipeline_runs``."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.quality_schema}")
    table = settings.pipeline_runs_table
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({jobs.PIPELINE_RUNS_DDL}) "
        f"USING {settings.table_format}"
    )
    df = spark.createDataFrame([record.as_row()], schema=spark.table(table).schema)
    df.write.format(settings.table_format).mode("append").saveAsTable(table)


def _bronze(spark, settings, datasets, load_id) -> str:
    results = [
        run_ingestion(
            spark, settings.bronze, name, load_id=f"{load_id}-{name}" if load_id else None
        )
        for name in datasets
    ]
    return "; ".join(map(str, results))


def _silver(spark, settings, datasets, load_id) -> str:
    return "; ".join(str(run_silver(spark, settings.silver, name)) for name in datasets)


def _gold(spark, settings, datasets, load_id) -> str:
    return str(run_gold(spark, settings.gold))


def _quality(stage: str) -> Callable[..., str]:
    def run(spark, settings, datasets, load_id) -> str:
        return str(run_quality(spark, settings.quality, stage, load_id=load_id))

    return run


STEP_RUNNERS: dict[str, Callable[..., str]] = {
    "bronze": _bronze,
    "silver": _silver,
    "gold": _gold,
    **{step: _quality(jobs.quality_stage(step)) for step in jobs.STEPS if jobs.quality_stage(step)},
}


def run_step(
    spark: SparkSession,
    settings: PipelineSettings,
    step: str,
    datasets: tuple[str, ...] = DATASETS,
    load_id: str | None = None,
) -> str:
    """Run one step of the scheduled job and record it in ``quality.pipeline_runs``.

    A failure is recorded with its error and then raised again, so the job
    task fails, later tasks do not run, and the failure alert fires.
    """
    if step not in STEP_RUNNERS:
        raise ValueError(f"Unknown step {step!r}. Steps: {jobs.STEPS}")
    started = _now()
    try:
        summary = STEP_RUNNERS[step](spark, settings, datasets, load_id)
    except Exception as exc:
        failed = jobs.StepRecord(
            step,
            jobs.FAILED,
            started,
            _now(),
            load_id,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        record_step(spark, settings.quality, failed)
        raise
    record_step(
        spark,
        settings.quality,
        jobs.StepRecord(step, jobs.SUCCESS, started, _now(), load_id, summary),
    )
    return summary
