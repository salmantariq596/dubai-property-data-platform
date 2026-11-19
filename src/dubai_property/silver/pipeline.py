"""Load new bronze loads into silver with Delta MERGE.

How a run works

1. Find bronze ``_load_id`` values not yet in ``silver.processed_loads``.
2. Clean those rows (:func:`dubai_property.silver.transform.transform`).
3. MERGE good rows into the silver table on the business key. A newer
   ``_ingested_at`` updates the row; an older one never overwrites it.
4. MERGE rejected rows into ``silver.quarantine`` on a hash of the raw row,
   its load and file, so a rejected row is stored once however often it is seen.
5. Rebuild ``silver.areas`` from the silver tables.
6. Record each load in ``silver.processed_loads``.

Every step is safe to repeat. If a run dies after step 3, the next run picks
the same loads up again, and MERGE turns the repeat into a no-op.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dubai_property.config import DATASETS, SilverSettings, get_dataset
from dubai_property.silver import areas, transform

SUCCESS = "SUCCESS"
NO_NEW_LOADS = "NO_NEW_LOADS"

PROCESSED_LOADS_DDL = """
    dataset STRING NOT NULL,
    load_id STRING NOT NULL,
    rows_read BIGINT,
    rows_quarantined BIGINT,
    processed_at TIMESTAMP NOT NULL
"""


@dataclass(frozen=True)
class SilverResult:
    dataset: str
    status: str
    load_ids: tuple[str, ...] = ()
    rows_read: int = 0
    rows_quarantined: int = 0
    rows_duplicate: int = 0  # good rows dropped because a later version of the key exists
    rows_merged: int = 0  # unique keys sent to MERGE

    def __str__(self) -> str:
        return (
            f"[{self.status}] {self.dataset}: {len(self.load_ids)} load(s), "
            f"{self.rows_read:,} rows read, {self.rows_merged:,} merged, "
            f"{self.rows_quarantined:,} quarantined, {self.rows_duplicate:,} duplicates"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- writing ---------------------------------------------------------------------------


def merge_into(
    spark: SparkSession,
    source: DataFrame,
    target: str,
    keys: list[str],
    table_format: str = "delta",
    update_condition: str | None = None,
) -> None:
    """Upsert ``source`` into ``target`` on ``keys``; create the table on first use.

    With ``update_condition`` matched rows are updated when it holds (``s`` is
    the source row, ``t`` the target row); without it, matched rows are left alone.
    """
    if table_format != "delta":
        raise ValueError(f"Silver loads with Delta MERGE; table_format is {table_format!r}")
    if not spark.catalog.tableExists(target):
        source.limit(0).write.format(table_format).saveAsTable(target)

    view = f"silver_merge_source_{uuid.uuid4().hex[:8]}"
    source.createOrReplaceTempView(view)
    try:
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        matched = (
            f"WHEN MATCHED AND ({update_condition}) THEN UPDATE SET * " if update_condition else ""
        )
        spark.sql(
            f"MERGE INTO {target} AS t USING {view} AS s ON {on} "
            f"{matched}WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def ensure_processed_loads_table(spark: SparkSession, settings: SilverSettings) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {settings.processed_loads_table} "
        f"({PROCESSED_LOADS_DDL}) USING {settings.table_format}"
    )


def record_processed_loads(
    spark: SparkSession,
    settings: SilverSettings,
    dataset: str,
    read_by_load: dict[str, int],
    quarantined_by_load: dict[str, int],
) -> None:
    table = settings.processed_loads_table
    processed_at = _now()
    rows = [
        (dataset, load_id, read, quarantined_by_load.get(load_id, 0), processed_at)
        for load_id, read in sorted(read_by_load.items())
    ]
    df = spark.createDataFrame(rows, schema=spark.table(table).schema)
    df.write.format(settings.table_format).mode("append").saveAsTable(table)


def refresh_areas(spark: SparkSession, settings: SilverSettings) -> None:
    """Rebuild ``silver.areas`` from every silver dataset table that exists."""
    frames = {
        name: spark.table(settings.silver_table(ds))
        for name, ds in DATASETS.items()
        if ds.silver_table and spark.catalog.tableExists(settings.silver_table(ds))
    }
    if not frames:
        return
    (
        areas.build_areas(frames)
        .write.format(settings.table_format)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(settings.areas_table)
    )


# --- running ---------------------------------------------------------------------------


def _counts_by_load(df: DataFrame) -> dict[str, int]:
    return {row["_load_id"]: row["count"] for row in df.groupBy("_load_id").count().collect()}


def new_load_ids(spark: SparkSession, settings: SilverSettings, dataset: str) -> list[str]:
    """Bronze loads of ``dataset`` that silver has not processed yet, oldest first."""
    bronze = spark.table(settings.bronze_table(get_dataset(dataset)))
    done = (
        spark.table(settings.processed_loads_table)
        .where(F.col("dataset") == dataset)
        .select(F.col("load_id").alias("_load_id"))
    )
    pending = bronze.select("_load_id").distinct().join(done, "_load_id", "left_anti")
    return sorted(row["_load_id"] for row in pending.collect())


def run_silver(spark: SparkSession, settings: SilverSettings, dataset_name: str) -> SilverResult:
    """Clean every bronze load of one dataset that silver has not seen yet."""
    dataset = get_dataset(dataset_name)
    target = settings.silver_table(dataset)

    if not spark.catalog.tableExists(settings.bronze_table(dataset)):
        return SilverResult(dataset.name, NO_NEW_LOADS)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.silver_schema}")
    ensure_processed_loads_table(spark, settings)

    load_ids = new_load_ids(spark, settings, dataset.name)
    if not load_ids:
        return SilverResult(dataset.name, NO_NEW_LOADS)

    batch = spark.table(settings.bronze_table(dataset)).where(F.col("_load_id").isin(load_ids))
    known_areas = (
        spark.table(settings.areas_table)
        if spark.catalog.tableExists(settings.areas_table)
        else None
    )
    frames = transform.transform(batch, dataset.name, known_areas=known_areas)

    merge_into(
        spark,
        frames.valid,
        target,
        [dataset.business_key],
        settings.table_format,
        update_condition="s._ingested_at >= t._ingested_at",
    )
    merge_into(
        spark,
        frames.quarantine,
        settings.quarantine_table,
        ["dataset", "record_hash"],
        settings.table_format,
    )
    refresh_areas(spark, settings)

    read_by_load = _counts_by_load(batch)
    quarantined_by_load = _counts_by_load(frames.quarantine)
    record_processed_loads(spark, settings, dataset.name, read_by_load, quarantined_by_load)

    accepted, merged = frames.accepted.count(), frames.valid.count()
    return SilverResult(
        dataset=dataset.name,
        status=SUCCESS,
        load_ids=tuple(load_ids),
        rows_read=sum(read_by_load.values()),
        rows_quarantined=sum(quarantined_by_load.values()),
        rows_duplicate=accepted - merged,
        rows_merged=merged,
    )
