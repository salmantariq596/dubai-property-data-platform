"""Build the gold star schema from silver.

How a run works

1. ``dim_date`` is generated (2000-2030).
2. ``dim_area`` is brought up to date with today's ``silver.areas`` as SCD
   Type 2: changed areas get a new version, history is kept.
3. ``dim_property_type`` and ``dim_project`` are rebuilt from silver.
4. ``fact_sales`` and ``fact_rents`` are rebuilt from silver, with surrogate
   keys looked up from the dimensions (liquid-clustered by date on Databricks).
5. ``agg_area_monthly`` is rebuilt from the facts.

Gold is rebuilt in full from silver on every run, so a run is always safe to
repeat. The only table that carries state is ``dim_area``, whose SCD Type 2
logic is itself idempotent: the same snapshot applied twice changes nothing.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from dubai_property.config import DATASETS, GoldSettings
from dubai_property.gold import dimensions, facts, rules
from dubai_property.silver import rules as silver_rules
from dubai_property.silver.transform import transform

SUCCESS = "SUCCESS"
NO_SILVER = "NO_SILVER"

FACT_TABLES = ("fact_sales", "fact_rents")


@dataclass(frozen=True)
class GoldResult:
    status: str
    as_of: date | None = None
    rows: dict[str, int] = field(default_factory=dict)

    def __str__(self) -> str:
        counts = ", ".join(f"{t} {n:,}" for t, n in self.rows.items())
        return f"[{self.status}] gold as of {self.as_of}: {counts}"


def _today() -> date:
    return datetime.now(timezone.utc).date()


# --- reading silver ----------------------------------------------------------------------


def empty_silver(spark: SparkSession, dataset: str) -> DataFrame:
    """A silver table with no rows but the exact silver schema.

    Gold can then run before a dataset (rents, say) has been loaded at all.
    """
    columns = [f.name for f in silver_rules.fields_for(dataset)]
    schema = StructType(
        [StructField(c, StringType()) for c in columns]
        + [
            StructField("_rescued_data", StringType()),
            StructField("_ingested_at", TimestampType()),
            StructField("_source_file", StringType()),
            StructField("_load_id", StringType()),
            StructField("_load_date", DateType()),
        ]
    )
    return transform(spark.createDataFrame([], schema), dataset).valid


def read_silver(spark: SparkSession, settings: GoldSettings, dataset: str) -> DataFrame:
    table = settings.silver_table(DATASETS[dataset].silver_table)
    if spark.catalog.tableExists(table):
        return spark.table(table)
    return empty_silver(spark, dataset)


# --- writing --------------------------------------------------------------------------------


def write_table(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    settings: GoldSettings,
    cluster_by: list[str] | None = None,
) -> None:
    """Replace ``table`` with ``df``; liquid-cluster it when settings ask for it."""
    if cluster_by and settings.cluster_facts:
        view = f"gold_source_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(view)
        try:
            spark.sql(rules.create_clustered_table_sql(table, view, cluster_by))
        finally:
            spark.catalog.dropTempView(view)
        return
    (
        df.write.format(settings.table_format)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def _materialise(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Small dimension built from its own table: collect it before overwriting that table."""
    return spark.createDataFrame(df.collect(), df.schema)


# --- running ---------------------------------------------------------------------------------


def run_gold(spark: SparkSession, settings: GoldSettings, as_of: date | None = None) -> GoldResult:
    """Rebuild gold from silver. ``as_of`` dates any area changes (default: today, UTC)."""
    as_of = as_of or _today()
    areas_table = settings.silver_table("areas")
    if not spark.catalog.tableExists(areas_table):
        return GoldResult(NO_SILVER, as_of)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.gold_schema}")
    transactions = read_silver(spark, settings, "transactions")
    rents = read_silver(spark, settings, "rents")

    write_table(spark, dimensions.build_dim_date(spark), settings.table("dim_date"), settings)

    dim_area_table = settings.table("dim_area")
    current = spark.table(dim_area_table) if spark.catalog.tableExists(dim_area_table) else None
    dim_area = dimensions.scd2_area(current, spark.table(areas_table), as_of)
    write_table(spark, _materialise(spark, dim_area), dim_area_table, settings)

    write_table(
        spark,
        dimensions.build_dim_property_type(transactions, rents),
        settings.table("dim_property_type"),
        settings,
    )
    write_table(
        spark,
        dimensions.build_dim_project(transactions, rents),
        settings.table("dim_project"),
        settings,
    )

    dims = (
        spark.table(dim_area_table),
        spark.table(settings.table("dim_property_type")),
        spark.table(settings.table("dim_project")),
    )
    write_table(
        spark,
        facts.build_fact_sales(transactions, *dims),
        settings.table("fact_sales"),
        settings,
        cluster_by=["date_key"],
    )
    write_table(
        spark,
        facts.build_fact_rents(rents, *dims),
        settings.table("fact_rents"),
        settings,
        cluster_by=["date_key"],
    )
    write_table(
        spark,
        facts.build_agg_area_monthly(
            spark.table(settings.table("fact_sales")),
            spark.table(settings.table("fact_rents")),
            spark.table(dim_area_table),
        ),
        settings.table("agg_area_monthly"),
        settings,
    )

    rows = {
        name: spark.table(settings.table(name)).count()
        for name in ("dim_area", "dim_property_type", "dim_project", *FACT_TABLES)
    }
    rows["agg_area_monthly"] = spark.table(settings.table("agg_area_monthly")).count()
    return GoldResult(SUCCESS, as_of, rows)


# --- performance -------------------------------------------------------------------------------


def optimize_facts(spark: SparkSession, settings: GoldSettings) -> list[str]:
    """Compact the fact tables' small files (and cluster them, where clustering is on)."""
    optimized = []
    for name in FACT_TABLES:
        table = settings.table(name)
        if spark.catalog.tableExists(table):
            spark.sql(f"OPTIMIZE {table}")
            optimized.append(table)
    return optimized


def time_query(spark: SparkSession, sql: str, repeats: int = 3) -> float:
    """Best wall-clock time in seconds over ``repeats`` runs of ``sql``."""
    best = float("inf")
    for _ in range(repeats):
        started = time.perf_counter()
        spark.sql(sql).collect()
        best = min(best, time.perf_counter() - started)
    return best
