"""Monitoring views over the pipeline's own logs.

| View | From | Answers |
|---|---|---|
| ``last_runs`` | ``quality.pipeline_runs`` | Did each step's last run succeed? When, how long? |
| ``rows_per_load`` | ``bronze.load_log`` | How many files and rows did each load bring? |
| ``failure_trend`` | ``quality.pipeline_runs`` | Runs and failures per step per day |
| ``dq_trend`` | ``quality.dq_results`` | PASS / WARN / FAIL checks per day |
| ``quarantine_trend`` | ``silver.processed_loads`` | Share of rows quarantined per load |
| ``pipeline_health`` | the above | One status: OK, WARN, STALE or FAILED |

Every view returns None when its table does not exist yet, so the
monitoring notebook works from the first day.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dubai_property import health
from dubai_property.config import PipelineSettings


def _table(spark: SparkSession, name: str) -> DataFrame | None:
    return spark.table(name) if spark.catalog.tableExists(name) else None


def _since(column: str, days: int):
    return F.col(column) >= F.date_sub(F.current_date(), days)


def last_runs(spark: SparkSession, settings: PipelineSettings) -> DataFrame | None:
    """The most recent row of each job step."""
    runs = _table(spark, settings.quality.pipeline_runs_table)
    if runs is None:
        return None
    latest = Window.partitionBy("step").orderBy(F.col("finished_at").desc())
    return (
        runs.withColumn("__rank", F.row_number().over(latest))
        .where("__rank = 1")
        .select(
            "step",
            "status",
            "load_id",
            "started_at",
            "finished_at",
            "duration_seconds",
            "error_message",
        )
        .orderBy("started_at")
    )


def rows_per_load(
    spark: SparkSession, settings: PipelineSettings, days: int = 30
) -> DataFrame | None:
    """Files and rows per bronze load (runs that found nothing new are left out)."""
    log = _table(spark, settings.bronze.load_log_table)
    if log is None:
        return None
    return (
        log.where(_since("finished_at", days) & F.col("source_file").isNotNull())
        .groupBy("load_id", "dataset")
        .agg(
            F.min("load_date").alias("load_date"),
            F.count(F.lit(1)).alias("files"),
            F.sum("rows_read").alias("rows_read"),
            F.sum("rows_written").alias("rows_written"),
            F.sum("rows_rescued").alias("rows_rescued"),
            F.max(F.when(F.col("status") != "SUCCESS", F.col("status"))).alias("problem"),
            F.max("finished_at").alias("finished_at"),
        )
        .orderBy(F.col("finished_at").desc())
    )


def failure_trend(
    spark: SparkSession, settings: PipelineSettings, days: int = 30
) -> DataFrame | None:
    runs = _table(spark, settings.quality.pipeline_runs_table)
    if runs is None:
        return None
    return (
        runs.where(_since("started_at", days))
        .groupBy(F.to_date("started_at").alias("day"), "step")
        .agg(
            F.count(F.lit(1)).alias("runs"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failures"),
            F.round(F.avg("duration_seconds"), 1).alias("avg_seconds"),
        )
        .orderBy("day", "step")
    )


def dq_trend(spark: SparkSession, settings: PipelineSettings, days: int = 30) -> DataFrame | None:
    results = _table(spark, settings.quality.dq_results_table)
    if results is None:
        return None
    return (
        results.where(_since("checked_at", days))
        .groupBy(F.to_date("checked_at").alias("day"), "stage")
        .pivot("status", ["PASS", "WARN", "FAIL", "SKIPPED"])
        .count()
        .na.fill(0)
        .orderBy("day", "stage")
    )


def quarantine_trend(spark: SparkSession, settings: PipelineSettings) -> DataFrame | None:
    loads = _table(spark, settings.silver.processed_loads_table)
    if loads is None:
        return None
    return loads.select(
        "dataset",
        "load_id",
        "processed_at",
        "rows_read",
        "rows_quarantined",
        F.round(100 * F.col("rows_quarantined") / F.col("rows_read"), 2).alias("quarantined_pct"),
    ).orderBy(F.col("processed_at").desc())


def pipeline_health(
    spark: SparkSession, settings: PipelineSettings, now: datetime | None = None
) -> health.Health:
    """OK, WARN, STALE or FAILED, with the reasons."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    latest = {}
    runs = last_runs(spark, settings)
    if runs is not None:
        for row in runs.collect():
            latest[row["step"]] = health.StepState(
                row["status"], row["finished_at"], row["error_message"]
            )

    dq_statuses = []
    results = _table(spark, settings.quality.dq_results_table)
    if results is not None:
        newest = Window.partitionBy("stage").orderBy(F.col("checked_at").desc())
        last_run_ids = results.withColumn("__rank", F.dense_rank().over(newest)).where("__rank = 1")
        dq_statuses = [r["status"] for r in last_run_ids.select("status").collect()]
    return health.assess(latest, dq_statuses, now)
