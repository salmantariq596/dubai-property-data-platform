"""Bronze rows -> silver rows. DataFrames in, DataFrames out; no tables touched.

How a batch is cleaned

1. Every field in :mod:`rules` is read from bronze (missing columns become
   null), trimmed, and cast with an explicit format. Casts never fail the job:
   ``try_cast`` turns a bad value into null, and the rule checks catch it.
2. Each row is checked. A row breaking any rule goes to quarantine with the
   names of the rules it broke and its raw values. Nothing is silently dropped.
3. Good rows are deduplicated on the business key, keeping the latest
   ingested version.
4. Measures are derived (size in sqft, price per sqft, USD value, off-plan,
   annual rent) and outliers are flagged, not removed.
5. Area names are standardised (:mod:`dubai_property.silver.areas`).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from dubai_property.config import get_dataset
from dubai_property.silver import rules
from dubai_property.silver.areas import AREA_COLUMNS, clean_text, resolve_areas

RESCUED_COLUMN = "_rescued_data"
LINEAGE_COLUMNS = ["_load_id", "_source_file", "_ingested_at"]

QUARANTINE_COLUMNS = [
    "dataset",
    "business_key",
    "failed_rules",
    "raw_record",
    "record_hash",
    *LINEAGE_COLUMNS,
    "quarantined_at",
]

TRANSACTION_DERIVED = [
    "size_sqft",
    "price_per_sqft",
    "value_usd",
    "is_offplan",
    "bedrooms",
    "is_outlier",
    "outlier_reason",
]
RENT_DERIVED = [
    "contract_days",
    "annual_rent_aed",
    "annual_rent_source",
    "size_sqft",
    "is_multi_unit",
]
DERIVED = {"transactions": TRANSACTION_DERIVED, "rents": RENT_DERIVED}


@dataclass(frozen=True)
class SilverFrames:
    accepted: DataFrame  # rows that passed every rule, before deduplication
    valid: DataFrame  # one row per business key: what MERGE writes to silver
    quarantine: DataFrame  # one row per rejected source row


# --- casting -----------------------------------------------------------------------


def _source(df: DataFrame, name: str) -> Column:
    """The raw column for a silver field: its DLD name, else a known alias, else
    the documented default (normally null) when the file has neither."""
    for column in (name, *rules.SOURCE_ALIASES.get(name, ())):
        if column in df.columns:
            return F.col(f"`{column}`")
    return F.lit(rules.MISSING_COLUMN_DEFAULTS.get(name)).cast("string")


def _ymd(year: Column, month: Column, day: Column) -> Column:
    return F.concat_ws("-", year, F.lpad(month, 2, "0"), F.lpad(day, 2, "0"))


def iso_date_string(raw: Column) -> Column:
    """``"05-01-2024"`` / ``"2024-1-5"`` -> ``"2024-01-05"``; anything else -> null.

    The string is cast to DATE afterwards, which rejects impossible dates such
    as 31 February. No parser guesses at ambiguous layouts.
    """
    iso, dmy = rules.ISO_DATE, rules.DAY_FIRST_DATE

    def part(pattern: str, group: int) -> Column:
        return F.regexp_extract(raw, pattern, group)

    return F.when(raw.rlike(iso), _ymd(part(iso, 1), part(iso, 2), part(iso, 3))).when(
        raw.rlike(dmy), _ymd(part(dmy, 3), part(dmy, 2), part(dmy, 1))
    )


def _helper(field: rules.Field) -> str:
    return f"__parse_{field.name}"


def _helper_column(field: rules.Field) -> Column | None:
    raw = F.col(f"__raw_{field.name}")
    if field.kind in ("decimal", "int"):
        return F.nullif(F.regexp_replace(raw, rules.NUMBER_NOISE, ""), F.lit(""))
    if field.kind == "date":
        return iso_date_string(raw)
    return None


def _typed_column(field: rules.Field) -> Column:
    raw = F.col(f"__raw_{field.name}")
    helper = f"`{_helper(field)}`"
    if field.kind == "string":
        return raw
    if field.kind == "decimal":
        return F.expr(f"try_cast({helper} AS {rules.MONEY_TYPE})")
    if field.kind == "int":
        # "3" and "3.0" are fine, "3.5" is not an integer and fails the cast
        number = f"try_cast({helper} AS DECIMAL(38,6))"
        return F.expr(f"CASE WHEN {number} = floor({number}) THEN try_cast({number} AS INT) END")
    if field.kind == "date":
        return F.expr(f"try_cast({helper} AS DATE)")
    lowered = F.lower(raw)  # bool
    return F.when(lowered.isin(*rules.TRUE_VALUES), F.lit(True)).when(
        lowered.isin(*rules.FALSE_VALUES), F.lit(False)
    )


def cast_fields(bronze: DataFrame, fields: tuple[rules.Field, ...]) -> DataFrame:
    """Typed columns plus the cleaned raw text (``__raw_*``) the checks need."""
    data_columns = [c for c in bronze.columns if not c.startswith("_")]
    rescued = _source(bronze, RESCUED_COLUMN)
    raw_record = F.to_json(
        F.struct(*[F.col(f"`{c}`") for c in data_columns], rescued.alias(RESCUED_COLUMN))
    )

    staged = bronze.select(
        *[clean_text(_source(bronze, f.name)).alias(f"__raw_{f.name}") for f in fields],
        raw_record.alias("raw_record"),
        rescued.alias(RESCUED_COLUMN),
        *[F.col(c) for c in LINEAGE_COLUMNS],
    )
    helpers = {_helper(f): c for f in fields if (c := _helper_column(f)) is not None}
    staged = staged.withColumns(helpers)
    typed = staged.withColumns({f.name: _typed_column(f) for f in fields})
    return typed.drop(*helpers)


# --- rules -------------------------------------------------------------------------


def _field_checks(fields: tuple[rules.Field, ...]) -> list[Column]:
    checks = []
    for field in fields:
        raw = F.col(f"__raw_{field.name}")
        if field.required:
            checks.append(F.when(raw.isNull(), F.lit(f"missing_{field.name}")))
        if field.kind != "string":
            failed_cast = raw.isNotNull() & F.col(field.name).isNull()
            checks.append(F.when(failed_cast, F.lit(f"invalid_{field.name}")))
    checks.append(F.when(F.col(RESCUED_COLUMN).isNotNull(), F.lit("rescued_data")))
    return checks


def _rent_checks() -> list[Column]:
    start, end = F.col("contract_start_date"), F.col("contract_end_date")
    no_amount = F.col("__raw_annual_amount").isNull() & F.col("__raw_contract_amount").isNull()
    return [
        F.when(end < start, F.lit("end_before_start")),
        F.when(no_amount, F.lit("missing_rent_amount")),
    ]


def _transaction_checks() -> list[Column]:
    return []  # zero sizes and prices are outliers to flag, not broken records


DATASET_CHECKS = {"transactions": _transaction_checks, "rents": _rent_checks}


def with_failed_rules(df: DataFrame, dataset: str) -> DataFrame:
    checks = _field_checks(rules.fields_for(dataset)) + DATASET_CHECKS[dataset]()
    return df.withColumn("failed_rules", F.filter(F.array(*checks), lambda r: r.isNotNull()))


# --- derived measures ----------------------------------------------------------------


def _money(column: Column) -> Column:
    return F.round(column, 2).cast(rules.MONEY_TYPE)


def derive_transactions(df: DataFrame) -> DataFrame:
    sqm = F.col("procedure_area").cast("double")
    worth = F.col("actual_worth").cast("double")
    price_per_sqft = F.when((sqm > 0) & (worth > 0), worth / (sqm * rules.SQFT_PER_SQM))

    outlier_reason = (
        F.when(sqm.isNull() | (sqm <= 0), F.lit("missing_size"))
        .when(worth <= 0, F.lit("non_positive_value"))
        .when(price_per_sqft < rules.PRICE_PER_SQFT_MIN, F.lit("price_per_sqft_below_min"))
        .when(price_per_sqft > rules.PRICE_PER_SQFT_MAX, F.lit("price_per_sqft_above_max"))
    )

    reg_type, procedure = F.col("reg_type_en"), F.col("procedure_name_en")
    is_offplan = F.when(
        reg_type.isNotNull() | procedure.isNotNull(),
        F.coalesce(reg_type.rlike(rules.OFF_PLAN), F.lit(False))
        | F.coalesce(procedure.rlike(rules.PRE_REGISTRATION), F.lit(False)),
    )

    rooms = F.col("rooms_en")
    bedrooms = (
        F.when(rooms.rlike(rules.STUDIO), F.lit(0))
        .when(rooms.rlike(rules.BEDROOMS), F.regexp_extract(rooms, rules.BEDROOMS, 1).cast("int"))
        .when(rooms.rlike(rules.BEDROOM_COUNT), rooms.cast("int"))
    )

    return df.withColumns(
        {
            "size_sqft": _money(sqm * rules.SQFT_PER_SQM),
            "price_per_sqft": _money(price_per_sqft),
            "value_usd": _money(worth / rules.AED_PER_USD),
            "is_offplan": is_offplan,
            "bedrooms": bedrooms,
            "outlier_reason": outlier_reason,
            "is_outlier": outlier_reason.isNotNull(),
        }
    )


def derive_rents(df: DataFrame) -> DataFrame:
    days = F.datediff(F.col("contract_end_date"), F.col("contract_start_date")) + 1
    annual = F.col("annual_amount").cast("double")
    total = F.col("contract_amount").cast("double")
    from_total = (total > 0) & (days > 0)

    return df.withColumns(
        {
            "contract_days": days,
            "annual_rent_aed": _money(
                F.when(annual > 0, annual).when(from_total, total * 365.0 / days)
            ),
            "annual_rent_source": F.when(annual > 0, F.lit("annual_amount")).when(
                from_total, F.lit("contract_amount")
            ),
            "size_sqft": _money(F.col("actual_area").cast("double") * rules.SQFT_PER_SQM),
            "is_multi_unit": F.col("no_of_prop") > 1,
        }
    )


DERIVE = {"transactions": derive_transactions, "rents": derive_rents}


# --- the whole step -------------------------------------------------------------------


def deduplicate(df: DataFrame, key: str) -> DataFrame:
    """One row per key: latest ``_ingested_at`` wins; ties broken deterministically."""
    latest_first = Window.partitionBy(key).orderBy(
        F.col("_ingested_at").desc(),
        F.col("_load_id").desc(),
        F.col("_source_file").desc(),
        F.col("raw_record").desc(),
    )
    return df.withColumn("__rank", F.row_number().over(latest_first)).where("__rank = 1")


def output_columns(dataset: str) -> list[str]:
    """Column order of the silver table."""
    names = [f.name for f in rules.fields_for(dataset)]
    for column in AREA_COLUMNS:
        if column not in names:
            names.append(column)
    return names + DERIVED[dataset] + LINEAGE_COLUMNS + ["_processed_at"]


def transform(
    bronze: DataFrame,
    dataset: str,
    known_areas: DataFrame | None = None,
    aliases: dict[str, str] = rules.AREA_ALIASES,
) -> SilverFrames:
    """Clean one batch of bronze rows for ``dataset``."""
    key = get_dataset(dataset).business_key
    checked = with_failed_rules(cast_fields(bronze, rules.fields_for(dataset)), dataset)

    rejected = checked.where(F.size("failed_rules") > 0)
    quarantine = rejected.select(
        F.lit(dataset).alias("dataset"),
        F.col(f"__raw_{key}").alias("business_key"),
        "failed_rules",
        "raw_record",
        F.sha2(
            F.concat_ws("|", F.col("raw_record"), F.col("_load_id"), F.col("_source_file")), 256
        ).alias("record_hash"),
        *LINEAGE_COLUMNS,
        F.current_timestamp().alias("quarantined_at"),
    ).dropDuplicates(["record_hash"])  # identical rejected rows in one file are kept once

    accepted = checked.where(F.size("failed_rules") == 0)
    latest = deduplicate(accepted, key)
    derived = DERIVE[dataset](latest)
    resolved = resolve_areas(derived, known_areas, aliases)
    valid = resolved.withColumn("_processed_at", F.current_timestamp()).select(
        *output_columns(dataset)
    )
    return SilverFrames(accepted=accepted, valid=valid, quarantine=quarantine)
