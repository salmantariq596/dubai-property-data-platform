"""Export gold tables to Parquet, for Power BI when no SQL warehouse is available.

The project plan allows this fallback when compute is limited: Power BI reads
the files from a local folder instead of Databricks (``GoldSource`` parameter
set to ``Parquet``). Each table becomes one folder of Parquet files.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from dubai_property.config import GOLD_TABLES, GoldSettings


def export_gold(
    spark: SparkSession,
    settings: GoldSettings,
    target_dir: str,
    tables: tuple[str, ...] = GOLD_TABLES,
) -> dict[str, int]:
    """Write each gold table to ``<target_dir>/<table>/``; return rows per table.

    One file per table (``coalesce(1)``) keeps the download simple; gold tables
    are small enough for that.
    """
    exported = {}
    for name in tables:
        table = settings.table(name)
        if not spark.catalog.tableExists(table):
            continue
        df = spark.table(table)
        df.coalesce(1).write.mode("overwrite").parquet(f"{target_dir.rstrip('/')}/{name}")
        exported[name] = df.count()
    return exported
