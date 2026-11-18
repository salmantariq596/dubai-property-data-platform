"""The load log: one row per file per load, plus one row for empty or failed runs.

It answers the operational questions every pipeline gets asked:
*Did last night's load run? Which files did it pick up? How many rows? How long?*
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

SUCCESS = "SUCCESS"  # file written and read back with the same row count
MISMATCH = "MISMATCH"  # rows read back differ from rows read: the run is failed
NO_NEW_FILES = "NO_NEW_FILES"  # run finished, nothing new to load (expected on re-runs)
FAILED = "FAILED"  # run raised an error before finishing

STATUSES = (SUCCESS, MISMATCH, NO_NEW_FILES, FAILED)

LOAD_LOG_DDL = """
    load_id STRING NOT NULL,
    dataset STRING NOT NULL,
    source_file STRING,
    load_date DATE,
    batch_id BIGINT,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_rescued BIGINT,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    duration_seconds DOUBLE,
    error_message STRING
"""

# Column order must match LOAD_LOG_DDL
COLUMNS = [line.split()[0] for line in LOAD_LOG_DDL.strip().splitlines()]

_MAX_ERROR_CHARS = 2000


@dataclass(frozen=True)
class LoadLogEntry:
    load_id: str
    dataset: str
    status: str
    started_at: datetime
    finished_at: datetime
    source_file: str | None = None
    load_date: object | None = None  # datetime.date; object keeps Spark Row values simple
    batch_id: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_rescued: int | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"Unknown status {self.status!r}")

    @property
    def duration_seconds(self) -> float:
        return round((self.finished_at - self.started_at).total_seconds(), 3)

    def as_row(self) -> tuple:
        values = asdict(self)
        values["duration_seconds"] = self.duration_seconds
        if values["error_message"]:
            values["error_message"] = values["error_message"][:_MAX_ERROR_CHARS]
        return tuple(values[c] for c in COLUMNS)


def file_status(rows_read: int, rows_written: int) -> str:
    return SUCCESS if rows_read == rows_written else MISMATCH


def ensure_load_log_table(spark, table: str, table_format: str = "delta") -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({LOAD_LOG_DDL}) USING {table_format}")


def write_entries(spark, table: str, entries: list[LoadLogEntry], **write_options) -> None:
    """Append entries. ``write_options`` carries txnAppId/txnVersion for idempotency."""
    if not entries:
        return
    schema = spark.table(table).schema
    df = spark.createDataFrame([e.as_row() for e in entries], schema=schema)
    writer = df.write.mode("append")
    for key, value in write_options.items():
        writer = writer.option(key, value)
    writer.saveAsTable(table)
