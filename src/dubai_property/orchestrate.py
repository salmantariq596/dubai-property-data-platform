"""The whole pipeline in one call: bronze -> checks -> silver -> checks -> gold -> checks.

Data quality sits between the layers. A FAIL check raises
:class:`~dubai_property.quality.runner.DataQualityError` and stops the run
before the next layer. If silver fails its checks, gold is not rebuilt, and
the dashboard keeps showing the last good data. Every step is safe to re-run,
so after fixing the cause the whole pipeline is simply run again.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from dubai_property.bronze.ingest import IngestionResult, run_ingestion
from dubai_property.config import PipelineSettings
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
