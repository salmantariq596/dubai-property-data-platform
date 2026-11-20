"""Shared builders for Spark tests: rows shaped like bronze tables, and silver frames."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from dubai_property.bronze import ingest
from dubai_property.silver.transform import transform

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample_transactions.csv"

METADATA = {
    "_rescued_data": None,
    "_ingested_at": datetime(2026, 11, 1, 9, 0),
    "_source_file": "landing/transactions/load_date=2026-11-01/sales.csv",
    "_load_id": "load-1",
    "_load_date": date(2026, 11, 1),
}
METADATA_TYPES = [StringType(), TimestampType(), StringType(), StringType(), DateType()]


def bronze_frame(spark, rows: list[dict]):
    """Rows shaped like a bronze table: string columns plus pipeline metadata."""
    data_columns = sorted({k for row in rows for k in row if not k.startswith("_")})
    schema = StructType(
        [StructField(c, StringType()) for c in data_columns]
        + [StructField(m, t) for m, t in zip(METADATA, METADATA_TYPES, strict=True)]
    )
    data = [
        tuple(row.get(c) for c in data_columns)
        + tuple(row.get(m, default) for m, default in METADATA.items())
        for row in rows
    ]
    return spark.createDataFrame(data, schema)


def fixture_batch(spark):
    """The synthetic fixture exactly as bronze stores it."""
    raw = spark.read.options(**ingest.CSV_OPTIONS).csv(str(FIXTURE))
    return ingest.add_metadata_columns(raw, "load-1")


def sale(transaction_id, **overrides):
    row = {
        "transaction_id": transaction_id,
        "instance_date": "05-01-2024",
        "trans_group_en": "Sales",
        "procedure_name_en": "Sell",
        "reg_type_en": "Existing Properties",
        "area_name_en": "Business Bay",
        "area_name_ar": "الخليج التجارى",
        "procedure_area": "100",
        "actual_worth": "1500000",
    }
    row.update(overrides)
    return row


def rent(contract_id, **overrides):
    row = {
        "contract_id": contract_id,
        "contract_start_date": "01-01-2024",
        "contract_end_date": "31-12-2024",
        "annual_amount": "120000",
        "contract_amount": "120000",
        "area_name_en": "Dubai Marina",
        "area_name_ar": "مرسى دبي",
        "actual_area": "100",
        "no_of_prop": "1",
    }
    row.update(overrides)
    return row


def by_key(df, key="transaction_id"):
    return {row[key]: row for row in df.collect()}


def silver_frame(spark, rows: list[dict], dataset: str):
    """Silver rows for ``dataset``, cleaned exactly as the pipeline would."""
    return transform(bronze_frame(spark, rows), dataset).valid
