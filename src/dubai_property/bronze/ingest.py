"""Bronze ingestion: land raw DLD CSV files in append-only tables, exactly once.

How a run works

1. A streaming reader picks up only files it has not seen before. On Databricks
   this is Auto Loader (``cloudFiles``); locally it is Spark's file source. Both
   remember processed files in a checkpoint, which is what makes re-runs safe.
2. Every column is read as a string, headers are renamed to snake_case, and
   four metadata columns are added (``_ingested_at``, ``_source_file``,
   ``_load_id``, ``_load_date``).
3. ``trigger(availableNow=True)`` processes everything new, then stops. It is a
   batch job built on streaming machinery, so it suits a daily schedule.
4. Each micro-batch is appended with ``txnAppId``/``txnVersion``, so if a batch
   is retried after a crash, Delta skips the write instead of duplicating it.
5. Rows are read back per file and compared with rows read; the result goes
   to the load log.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dubai_property.bronze import load_log
from dubai_property.config import BronzeSettings, Dataset, get_dataset
from dubai_property.utils.naming import sanitize_column_names

RESCUED_COLUMN = "_rescued_data"
METADATA_COLUMNS = ["_ingested_at", "_source_file", "_load_id", "_load_date"]

# RFC 4180 CSV. Spark's default escape character is a backslash, which breaks
# on the standard doubled quote ("") inside quoted values, so set it to '"'.
# multiLine handles quoted values containing line breaks (seen in free text).
CSV_OPTIONS = {
    "header": "true",
    "multiLine": "true",
    "escape": '"',
    "encoding": "UTF-8",
    "pathGlobFilter": "*.csv",
}

_LOAD_DATE_PATTERN = r"load_date=(\d{4}-\d{2}-\d{2})"


@dataclass(frozen=True)
class IngestionResult:
    load_id: str
    dataset: str
    status: str
    files: int
    rows_read: int
    rows_rescued: int

    def __str__(self) -> str:
        return (
            f"[{self.status}] {self.dataset} load {self.load_id}: "
            f"{self.files} file(s), {self.rows_read:,} rows, {self.rows_rescued:,} rescued"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_load_id(dataset: str, now: datetime | None = None) -> str:
    """Readable and unique: ``transactions-20261108T091500Z-3fa2c1``."""
    now = now or datetime.now(timezone.utc)
    return f"{dataset}-{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


# --- reading -------------------------------------------------------------------


def _string_schema_from_headers(spark: SparkSession, path: str):
    """Local mode only: Spark's file stream needs a schema up front.

    Without ``inferSchema`` every column is a string, which is what we want.
    """
    return spark.read.options(**CSV_OPTIONS).option("recursiveFileLookup", "true").csv(path).schema


def build_reader(spark: SparkSession, settings: BronzeSettings, dataset: Dataset) -> DataFrame:
    path = settings.landing_path(dataset)

    if settings.source == "autoloader":
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{settings.checkpoint_path(dataset)}/_schema")
            .option("cloudFiles.inferColumnTypes", "false")  # all strings
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.partitionColumns", "")  # we derive _load_date ourselves
            .options(**CSV_OPTIONS)
            .load(path)
        )

    return (
        spark.readStream.format("csv")
        .schema(_string_schema_from_headers(spark, path))
        .option("recursiveFileLookup", "true")
        .options(**CSV_OPTIONS)
        .load(path)
    )


def add_metadata_columns(df: DataFrame, load_id: str) -> DataFrame:
    """Rename source columns to snake_case and append lineage columns.

    ``_metadata.file_path`` is a hidden column on file sources, so it must be
    selected here, before any projection drops it.
    """
    source_columns = [c for c in df.columns if c != RESCUED_COLUMN]
    clean_names = sanitize_column_names(source_columns)

    rescued = (
        F.col(RESCUED_COLUMN)
        if RESCUED_COLUMN in df.columns
        else F.lit(None).cast("string")  # Auto Loader adds this column; local Spark does not
    )

    projected = df.select(
        *[
            F.col("`" + raw.replace("`", "``") + "`").alias(clean)
            for raw, clean in zip(source_columns, clean_names, strict=True)
        ],
        rescued.alias(RESCUED_COLUMN),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.lit(load_id).alias("_load_id"),
    )
    load_date = F.regexp_extract("_source_file", _LOAD_DATE_PATTERN, 1)
    return projected.withColumn("_load_date", F.nullif(load_date, F.lit(""))).withColumn(
        "_load_date", F.expr("try_cast(_load_date AS DATE)")
    )


# --- writing -------------------------------------------------------------------


def make_batch_writer(settings: BronzeSettings, dataset: Dataset, load_id: str):
    """Return the ``foreachBatch`` function for one dataset and one load.

    Only plain values are captured, so the function can be pickled and run on
    serverless compute, where foreachBatch executes away from the notebook.
    """
    target = settings.table(dataset.bronze_table)
    log_table = settings.load_log_table
    table_format = settings.table_format
    app_id = settings.write_app_id(dataset)
    dataset_name = dataset.name

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        started = _now()

        stats = (
            batch_df.groupBy("_source_file", "_load_date")
            .agg(
                F.count(F.lit(1)).alias("rows_read"),
                F.count(RESCUED_COLUMN).alias("rows_rescued"),
            )
            .collect()
        )
        if not stats:
            return

        (
            batch_df.write.format(table_format)
            .mode("append")
            .option("mergeSchema", "true")  # Auto Loader may add columns
            .option("txnAppId", app_id)
            .option("txnVersion", batch_id)
            .saveAsTable(target)
        )

        files = [row["_source_file"] for row in stats]
        written_by_file = {
            row["_source_file"]: row["count"]
            for row in spark.table(target)
            .where((F.col("_load_id") == load_id) & F.col("_source_file").isin(files))
            .groupBy("_source_file")
            .count()
            .collect()
        }

        if (
            not written_by_file
            and spark.table(target).where(F.col("_source_file").isin(files)).limit(1).count()
        ):
            # A retried batch whose write was already committed by an earlier
            # run: Delta skipped it (same txnAppId + txnVersion). Its log rows
            # were written then too, so there is nothing to do.
            return

        finished = _now()
        entries = [
            load_log.LoadLogEntry(
                load_id=load_id,
                dataset=dataset_name,
                source_file=row["_source_file"],
                load_date=row["_load_date"],
                batch_id=batch_id,
                rows_read=row["rows_read"],
                rows_written=written_by_file.get(row["_source_file"], 0),
                rows_rescued=row["rows_rescued"],
                status=load_log.file_status(
                    row["rows_read"], written_by_file.get(row["_source_file"], 0)
                ),
                started_at=started,
                finished_at=finished,
            )
            for row in stats
        ]
        load_log.write_entries(spark, log_table, entries, txnAppId=app_id, txnVersion=batch_id)

        bad = [e.source_file for e in entries if e.status == load_log.MISMATCH]
        if bad:
            raise RuntimeError(f"Row count mismatch after writing {target}: {bad}")

    return write_batch


# --- running -------------------------------------------------------------------


def _has_csv_files(path: str) -> bool:
    """Works locally and on Unity Catalog volumes (mounted under /Volumes)."""
    return any(name.lower().endswith(".csv") for _, _, names in os.walk(path) for name in names)


def _allow_remote_pickling() -> None:
    """Let serverless compute run our foreachBatch function.

    On serverless (Spark Connect), the function is pickled and executed in a
    separate Python process that may not be able to import this package.
    Pickling the package by value ships the code along with the function.
    """
    try:
        from pyspark import cloudpickle

        import dubai_property

        cloudpickle.register_pickle_by_value(dubai_property)
    except Exception:  # pragma: no cover - best effort, harmless if unavailable
        pass


def _ensure_append_only(spark: SparkSession, table: str) -> None:
    """Bronze never deletes or updates: make Delta enforce it."""
    current = {
        row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    }
    if current.get("delta.appendOnly") != "true":
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")


def _log_run(spark, settings, dataset, load_id, status, started, error=None) -> None:
    entry = load_log.LoadLogEntry(
        load_id=load_id,
        dataset=dataset.name,
        status=status,
        started_at=started,
        finished_at=_now(),
        error_message=error,
    )
    load_log.write_entries(spark, settings.load_log_table, [entry])


def run_ingestion(
    spark: SparkSession,
    settings: BronzeSettings,
    dataset_name: str,
    load_id: str | None = None,
) -> IngestionResult:
    """Ingest every new file for one dataset, then stop."""
    dataset = get_dataset(dataset_name)
    load_id = load_id or new_load_id(dataset.name)
    started = _now()
    target = settings.table(dataset.bronze_table)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.bronze_schema}")
    load_log.ensure_load_log_table(spark, settings.load_log_table, settings.table_format)
    _allow_remote_pickling()

    if not _has_csv_files(settings.landing_path(dataset)):
        _log_run(spark, settings, dataset, load_id, load_log.NO_NEW_FILES, started)
        return IngestionResult(load_id, dataset.name, load_log.NO_NEW_FILES, 0, 0, 0)

    try:
        stream = add_metadata_columns(build_reader(spark, settings, dataset), load_id)
        query = (
            stream.writeStream.foreachBatch(make_batch_writer(settings, dataset, load_id))
            .option("checkpointLocation", settings.checkpoint_path(dataset))
            .trigger(availableNow=True)
            .queryName(f"bronze_{dataset.name}_{load_id[-6:]}")
            .start()
        )
        query.awaitTermination()
    except Exception as exc:
        _log_run(spark, settings, dataset, load_id, load_log.FAILED, started, error=str(exc))
        raise

    for table in (target, settings.load_log_table):
        if spark.catalog.tableExists(table):
            # Micro-batches write through a cloned session. Delta re-reads its
            # log on every query, but plain file tables (used in local tests)
            # keep a cached file listing here unless refreshed.
            spark.catalog.refreshTable(table)
            if settings.table_format == "delta":
                _ensure_append_only(spark, table)

    rows = (
        spark.table(settings.load_log_table)
        .where(F.col("load_id") == load_id)
        .agg(
            F.count(F.lit(1)).alias("files"),
            F.coalesce(F.sum("rows_read"), F.lit(0)).alias("rows_read"),
            F.coalesce(F.sum("rows_rescued"), F.lit(0)).alias("rows_rescued"),
        )
        .first()
    )
    if rows["files"] == 0:
        _log_run(spark, settings, dataset, load_id, load_log.NO_NEW_FILES, started)
        return IngestionResult(load_id, dataset.name, load_log.NO_NEW_FILES, 0, 0, 0)

    return IngestionResult(
        load_id,
        dataset.name,
        load_log.SUCCESS,
        rows["files"],
        rows["rows_read"],
        rows["rows_rescued"],
    )
