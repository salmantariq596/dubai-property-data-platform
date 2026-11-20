"""Gold dimensions. DataFrames in, DataFrames out; no tables touched.

Surrogate keys are ``xxhash64`` of the natural key (and, for ``dim_area``, of
the version's start date). The same input always gives the same key, so a
rebuild never re-numbers rows and facts never need re-keying. Two reserved
keys sit beside them: ``-1`` (Unknown) and ``0`` (All types, used only by the
monthly aggregate).
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import chain

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dubai_property.gold import rules
from dubai_property.silver.areas import clean_text

# --- shared helpers ------------------------------------------------------------------


def date_key_column(day: Column) -> Column:
    """Same as :func:`rules.date_key`: ``2024-01-05`` -> ``20240105``."""
    return (F.year(day) * 10_000 + F.month(day) * 100 + F.dayofmonth(day)).cast("int")


def _aliased(column: Column, aliases: dict[str, str]) -> Column:
    mapping = rules.alias_map(aliases)
    if not mapping:
        return column
    literal = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in mapping.items()))
    return F.coalesce(literal[column], column)


def property_type_key_column(
    property_type: Column,
    sub_type: Column,
    type_aliases: dict[str, str] = rules.PROPERTY_TYPE_ALIASES,
    sub_type_aliases: dict[str, str] = rules.PROPERTY_SUB_TYPE_ALIASES,
) -> Column:
    """Same as :func:`rules.property_type_key`: ``"unit|flat"``, or null."""
    kind = _aliased(F.lower(clean_text(property_type)), type_aliases)
    sub = _aliased(F.coalesce(F.lower(clean_text(sub_type)), F.lit("")), sub_type_aliases)
    return F.when(kind.isNotNull(), F.concat_ws("|", kind, sub))


def project_key_column(name: Column) -> Column:
    """Same as :func:`rules.project_key`."""
    return F.lower(clean_text(name))


def _static_rows(spark: SparkSession, rows: list[tuple], schema: StructType) -> DataFrame:
    return spark.createDataFrame(rows, schema)


def _prefer_sales(column: str) -> Column:
    """Inside a group: the spelling sales use, where sales have one; else the most common.

    Sales and rents can spell the same name differently ("Marina Gate", "MARINA GATE").
    Preferring sales makes the choice deterministic instead of a tie between counts.
    """
    return F.coalesce(F.mode(F.when(F.col("__sales"), F.col(column))), F.mode(column))


# --- dim_date ----------------------------------------------------------------------------


def build_dim_date(
    spark: SparkSession, start: date = rules.DATE_START, end: date = rules.DATE_END
) -> DataFrame:
    """One row per day, with the UAE weekend as it was on that day."""
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{start.isoformat()}', DATE'{end.isoformat()}', "
        "INTERVAL 1 DAY)) AS date"
    )
    day = F.col("date")
    iso_weekday = ((F.dayofweek(day) + 5) % 7) + 1  # Spark: 1 = Sunday; ISO: 1 = Monday
    weekend = F.when(
        day < F.lit(rules.WEEKEND_CHANGE), iso_weekday.isin(*rules.WEEKEND_BEFORE)
    ).otherwise(iso_weekday.isin(*rules.WEEKEND_FROM))
    return days.select(
        date_key_column(day).alias("date_key"),
        day.alias("date"),
        F.year(day).alias("year"),
        F.quarter(day).alias("quarter"),
        F.month(day).alias("month"),
        F.date_format(day, "MMMM").alias("month_name"),
        (F.year(day) * 100 + F.month(day)).cast("int").alias("month_key"),
        F.trunc(day, "month").alias("month_start"),
        F.dayofmonth(day).alias("day_of_month"),
        iso_weekday.cast("int").alias("iso_weekday"),
        F.date_format(day, "EEEE").alias("day_name"),
        weekend.alias("is_weekend"),
    )


# --- dim_property_type -------------------------------------------------------------------

PROPERTY_TYPE_SCHEMA = StructType(
    [
        StructField("property_type_sk", LongType()),
        StructField("property_type_key", StringType()),
        StructField("property_type", StringType()),
        StructField("property_sub_type", StringType()),
        StructField("in_sales", BooleanType()),
        StructField("in_rents", BooleanType()),
    ]
)


def build_dim_property_type(
    transactions: DataFrame,
    rents: DataFrame,
    type_aliases: dict[str, str] = rules.PROPERTY_TYPE_ALIASES,
    sub_type_aliases: dict[str, str] = rules.PROPERTY_SUB_TYPE_ALIASES,
) -> DataFrame:
    """One row per property type and sub-type, shared by sales and rents."""

    def side(df: DataFrame, type_col: str, sub_col: str, is_sales: bool) -> DataFrame:
        return df.select(
            property_type_key_column(
                F.col(type_col), F.col(sub_col), type_aliases, sub_type_aliases
            ).alias("property_type_key"),
            clean_text(F.col(type_col)).alias("__type"),
            clean_text(F.col(sub_col)).alias("__sub"),
            F.lit(is_sales).alias("__sales"),
        )

    rows = side(transactions, "property_type_en", "property_sub_type_en", True).unionByName(
        side(rents, "ejari_property_type_en", "ejari_property_sub_type_en", False)
    )

    found = (
        rows.where(F.col("property_type_key").isNotNull())
        .groupBy("property_type_key")
        .agg(
            _prefer_sales("__type").alias("property_type"),
            _prefer_sales("__sub").alias("property_sub_type"),
            F.max("__sales").alias("in_sales"),
            F.max(~F.col("__sales")).alias("in_rents"),
        )
        .select(
            F.xxhash64("property_type_key").alias("property_type_sk"),
            "property_type_key",
            "property_type",
            "property_sub_type",
            "in_sales",
            "in_rents",
        )
    )
    reserved = _static_rows(
        transactions.sparkSession,
        [
            (rules.UNKNOWN_SK, None, rules.UNKNOWN_LABEL, None, False, False),
            (rules.ALL_TYPES_SK, None, rules.ALL_TYPES_LABEL, None, False, False),
        ],
        PROPERTY_TYPE_SCHEMA,
    )
    return found.unionByName(reserved)


# --- dim_project ----------------------------------------------------------------------------

PROJECT_SCHEMA = StructType(
    [
        StructField("project_sk", LongType()),
        StructField("project_key", StringType()),
        StructField("project_name_en", StringType()),
        StructField("project_number", StringType()),
        StructField("master_project_en", StringType()),
        StructField("developer_name", StringType()),
        StructField("project_status", StringType()),
        StructField("in_sales", BooleanType()),
        StructField("in_rents", BooleanType()),
    ]
)


def build_dim_project(transactions: DataFrame, rents: DataFrame) -> DataFrame:
    """One row per project, matched across datasets by normalised English name.

    ``developer_name`` and ``project_status`` stay empty until the DLD projects
    dataset is cleaned; the columns exist now so Power BI's model does not change then.
    """
    sales = transactions.select(
        project_key_column(F.col("project_name_en")).alias("project_key"),
        clean_text(F.col("project_name_en")).alias("__name"),
        clean_text(F.col("project_number")).alias("__number"),
        clean_text(F.col("master_project_en")).alias("__master"),
        F.lit(True).alias("__sales"),
    )
    rented = rents.select(
        project_key_column(F.col("project_name_en")).alias("project_key"),
        clean_text(F.col("project_name_en")).alias("__name"),
        F.lit(None).cast("string").alias("__number"),
        clean_text(F.col("master_project_en")).alias("__master"),
        F.lit(False).alias("__sales"),
    )
    found = (
        sales.unionByName(rented)
        .where(F.col("project_key").isNotNull())
        .groupBy("project_key")
        .agg(
            _prefer_sales("__name").alias("project_name_en"),
            _prefer_sales("__number").alias("project_number"),
            _prefer_sales("__master").alias("master_project_en"),
            F.max("__sales").alias("in_sales"),
            F.max(~F.col("__sales")).alias("in_rents"),
        )
        .select(
            F.xxhash64("project_key").alias("project_sk"),
            "project_key",
            "project_name_en",
            "project_number",
            "master_project_en",
            F.lit(None).cast("string").alias("developer_name"),
            F.lit(None).cast("string").alias("project_status"),
            "in_sales",
            "in_rents",
        )
    )
    unknown = _static_rows(
        transactions.sparkSession,
        [(rules.UNKNOWN_SK, None, rules.UNKNOWN_LABEL, None, None, None, None, False, False)],
        PROJECT_SCHEMA,
    )
    return found.unionByName(unknown)


# --- dim_area: SCD Type 2 -------------------------------------------------------------------

AREA_TRACKED = ["area_name_en", "area_name_ar"]
AREA_SCHEMA = StructType(
    [
        StructField("area_sk", LongType()),
        StructField("area_key", StringType()),
        StructField("area_name_en", StringType()),
        StructField("area_name_ar", StringType()),
        StructField("attr_hash", StringType()),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType()),
    ]
)
AREA_COLUMNS = [f.name for f in AREA_SCHEMA.fields]


def _attr_hash() -> Column:
    values = [F.coalesce(F.col(c), F.lit("")) for c in AREA_TRACKED]
    return F.sha2(F.concat_ws("||", *values), 256)


def _area_version(df: DataFrame, valid_from: date, valid_to: date, is_current: bool) -> DataFrame:
    """Rows of ``df`` (area_key, tracked attributes, attr_hash) as one dimension version."""
    start = F.lit(valid_from)
    return df.select(
        F.xxhash64("area_key", start).alias("area_sk"),
        "area_key",
        *AREA_TRACKED,
        "attr_hash",
        start.alias("valid_from"),
        F.lit(valid_to).alias("valid_to"),
        F.lit(is_current).alias("is_current"),
    )


def _unknown_area(spark: SparkSession) -> DataFrame:
    row = (
        rules.UNKNOWN_SK,
        None,
        rules.UNKNOWN_LABEL,
        None,
        None,
        rules.VALID_FROM_FIRST,
        rules.VALID_TO_OPEN,
        True,
    )
    return _static_rows(spark, [row], AREA_SCHEMA)


def scd2_area(current: DataFrame | None, snapshot: DataFrame, as_of: date) -> DataFrame:
    """The whole ``dim_area`` after applying today's ``silver.areas`` snapshot.

    * New area: one version, valid from the start of time.
    * Name changed: the current version closes the day before ``as_of`` and a
      new version starts on ``as_of``. A second change on the same day updates
      that day's version instead of adding a one-day row.
    * Unchanged or no longer delivered: kept as it is. Areas are never deleted,
      because old facts still point at them.

    Running it twice with the same snapshot changes nothing.
    """
    spark = snapshot.sparkSession
    source = (
        snapshot.where(F.col("area_key").isNotNull())
        .select("area_key", *AREA_TRACKED)
        .dropDuplicates(["area_key"])
        .withColumn("attr_hash", _attr_hash())
    )
    if current is None:
        first = _area_version(source, rules.VALID_FROM_FIRST, rules.VALID_TO_OPEN, True)
        return first.unionByName(_unknown_area(spark))

    known = current.where(F.col("area_key").isNotNull()).select(*AREA_COLUMNS)
    history = known.where(~F.col("is_current"))
    live = known.where(F.col("is_current"))

    incoming = source.select(
        "area_key",
        *[F.col(c).alias(f"__new_{c}") for c in AREA_TRACKED],
        F.col("attr_hash").alias("__new_hash"),
    )
    joined = live.join(incoming, "area_key", "full")

    is_new = F.col("area_sk").isNull()
    changed = (
        ~is_new & F.col("__new_hash").isNotNull() & (F.col("__new_hash") != F.col("attr_hash"))
    )
    same_day = changed & (F.col("valid_from") == F.lit(as_of))

    def incoming_rows(condition: Column) -> DataFrame:
        return joined.where(condition).select(
            "area_key",
            *[F.col(f"__new_{c}").alias(c) for c in AREA_TRACKED],
            F.col("__new_hash").alias("attr_hash"),
        )

    kept = joined.where(~is_new & ~changed).select(*AREA_COLUMNS)
    added = _area_version(incoming_rows(is_new), rules.VALID_FROM_FIRST, rules.VALID_TO_OPEN, True)
    closed = (
        joined.where(changed & ~same_day)
        .select(*AREA_COLUMNS)
        .withColumns({"valid_to": F.lit(as_of - timedelta(days=1)), "is_current": F.lit(False)})
    )
    reopened = _area_version(incoming_rows(changed & ~same_day), as_of, rules.VALID_TO_OPEN, True)
    # same-day correction: same start date, so the same surrogate key
    corrected = _area_version(incoming_rows(same_day), as_of, rules.VALID_TO_OPEN, True)

    result = history
    for part in (kept, added, closed, reopened, corrected, _unknown_area(spark)):
        result = result.unionByName(part)
    return result
