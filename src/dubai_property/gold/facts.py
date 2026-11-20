"""Gold facts and the monthly aggregate. DataFrames in, DataFrames out.

Grain
- ``fact_sales``: one registered sale (``trans_group_en`` = Sales). Mortgages
  and gifts are not sales and are left out.
- ``fact_rents``: one Ejari rental contract, dated by its start date.
- ``agg_area_monthly``: one area x property type x month, plus an
  "All types" row (``property_type_sk = 0``) per area and month.

Every fact row finds its dimension rows: missing or unmapped values point at
the Unknown row (``-1``). ``dim_area`` is SCD Type 2, so a fact joins the
version that was valid on the fact's own date.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dubai_property.gold import rules
from dubai_property.gold.dimensions import (
    date_key_column,
    project_key_column,
    property_type_key_column,
)
from dubai_property.silver.areas import clean_text

FACT_SALES_COLUMNS = [
    "transaction_id",
    "date_key",
    "area_sk",
    "property_type_sk",
    "project_sk",
    "area_key",
    "size_sqft",
    "value_aed",
    "value_usd",
    "price_per_sqft",
    "is_offplan",
    "is_outlier",
    "bedrooms",
    "procedure_name_en",
    "property_usage_en",
    "_load_id",
]
FACT_RENTS_COLUMNS = [
    "contract_id",
    "date_key",
    "end_date_key",
    "area_sk",
    "property_type_sk",
    "project_sk",
    "area_key",
    "annual_rent_aed",
    "size_sqft",
    "contract_days",
    "is_multi_unit",
    "contract_reg_type_en",
    "property_usage_en",
    "_load_id",
]
AGG_COLUMNS = [
    "month_key",
    "month_start",
    "area_key",
    "area_sk",
    "area_name_en",
    "property_type_sk",
    "sales_count",
    "sales_value_aed",
    "offplan_sales_count",
    "priced_sales_count",
    "median_price_per_sqft",
    "median_price_per_sqft_yoy_pct",
    "median_sale_price_aed",
    "rent_count",
    "priced_rent_count",
    "median_annual_rent_aed",
    "gross_yield_pct",
]

_UNMAPPED = ""  # stands in for a null area_key inside joins, so unmapped rows still pair up


# --- dimension lookups ------------------------------------------------------------------


def _with_area_sk(df: DataFrame, dim_area: DataFrame, event_date: str) -> DataFrame:
    """Point-in-time lookup: the area version valid on the fact's own date."""
    versions = dim_area.where(F.col("area_key").isNotNull()).select(
        F.col("area_key").alias("__area_key"),
        F.col("area_sk").alias("__area_sk"),
        F.col("valid_from").alias("__from"),
        F.col("valid_to").alias("__to"),
    )
    on = (F.col("area_key") == F.col("__area_key")) & F.col(event_date).between(
        F.col("__from"), F.col("__to")
    )
    return (
        df.join(F.broadcast(versions), on, "left")
        .withColumn("area_sk", F.coalesce(F.col("__area_sk"), F.lit(rules.UNKNOWN_SK)))
        .drop("__area_key", "__area_sk", "__from", "__to")
    )


def _with_sk(df: DataFrame, dim: DataFrame, key: Column, dim_key: str, sk: str) -> DataFrame:
    lookup = dim.where(F.col(dim_key).isNotNull()).select(
        F.col(dim_key).alias("__key"), F.col(sk).alias("__sk")
    )
    return (
        df.withColumn("__fact_key", key)
        .join(F.broadcast(lookup), F.col("__fact_key") == F.col("__key"), "left")
        .withColumn(sk, F.coalesce(F.col("__sk"), F.lit(rules.UNKNOWN_SK)))
        .drop("__fact_key", "__key", "__sk")
    )


def _with_dimension_keys(
    df: DataFrame,
    event_date: str,
    type_columns: tuple[str, str],
    dim_area: DataFrame,
    dim_property_type: DataFrame,
    dim_project: DataFrame,
    type_aliases: dict[str, str],
    sub_type_aliases: dict[str, str],
) -> DataFrame:
    type_key = property_type_key_column(
        F.col(type_columns[0]), F.col(type_columns[1]), type_aliases, sub_type_aliases
    )
    keyed = _with_area_sk(df, dim_area, event_date)
    keyed = _with_sk(keyed, dim_property_type, type_key, "property_type_key", "property_type_sk")
    keyed = _with_sk(
        keyed,
        dim_project,
        project_key_column(F.col("project_name_en")),
        "project_key",
        "project_sk",
    )
    return keyed.withColumn("date_key", date_key_column(F.col(event_date)))


# --- facts ----------------------------------------------------------------------------------


def build_fact_sales(
    transactions: DataFrame,
    dim_area: DataFrame,
    dim_property_type: DataFrame,
    dim_project: DataFrame,
    type_aliases: dict[str, str] = rules.PROPERTY_TYPE_ALIASES,
    sub_type_aliases: dict[str, str] = rules.PROPERTY_SUB_TYPE_ALIASES,
) -> DataFrame:
    sales = transactions.where(F.lower(clean_text(F.col("trans_group_en"))) == rules.SALES_GROUP)
    keyed = _with_dimension_keys(
        sales,
        "instance_date",
        ("property_type_en", "property_sub_type_en"),
        dim_area,
        dim_property_type,
        dim_project,
        type_aliases,
        sub_type_aliases,
    )
    return keyed.withColumn("value_aed", F.col("actual_worth")).select(*FACT_SALES_COLUMNS)


def build_fact_rents(
    rents: DataFrame,
    dim_area: DataFrame,
    dim_property_type: DataFrame,
    dim_project: DataFrame,
    type_aliases: dict[str, str] = rules.PROPERTY_TYPE_ALIASES,
    sub_type_aliases: dict[str, str] = rules.PROPERTY_SUB_TYPE_ALIASES,
) -> DataFrame:
    keyed = _with_dimension_keys(
        rents,
        "contract_start_date",
        ("ejari_property_type_en", "ejari_property_sub_type_en"),
        dim_area,
        dim_property_type,
        dim_project,
        type_aliases,
        sub_type_aliases,
    )
    return keyed.withColumn("end_date_key", date_key_column(F.col("contract_end_date"))).select(
        *FACT_RENTS_COLUMNS
    )


# --- agg_area_monthly ---------------------------------------------------------------------------


def _by_month(df: DataFrame) -> DataFrame:
    return df.withColumns(
        {
            "month_key": F.floor(F.col("date_key") / 100).cast("int"),
            "area_key": F.coalesce(F.col("area_key"), F.lit(_UNMAPPED)),
        }
    )


def _with_all_types(df: DataFrame, aggregations: list[Column]) -> DataFrame:
    """Aggregate per property type, plus an "All types" row per area and month."""
    by_type = df.groupBy("month_key", "area_key", "property_type_sk").agg(*aggregations)
    all_types = (
        df.groupBy("month_key", "area_key")
        .agg(*aggregations)
        .withColumn("property_type_sk", F.lit(rules.ALL_TYPES_SK).cast("long"))
    )
    return by_type.unionByName(all_types)


def build_agg_area_monthly(
    fact_sales: DataFrame,
    fact_rents: DataFrame,
    dim_area: DataFrame,
    min_records: int = rules.MIN_RECORDS_FOR_YIELD,
) -> DataFrame:
    """Medians per area, property type and month, and gross rental yield.

    Medians leave out flagged outliers (sales) and multi-unit contracts (rents),
    which would distort them. Counts include every row.
    """
    priced = ~F.col("is_outlier") & F.col("price_per_sqft").isNotNull()
    sales = _with_all_types(
        _by_month(fact_sales),
        [
            F.count(F.lit(1)).alias("sales_count"),
            F.sum("value_aed").alias("sales_value_aed"),
            F.sum(F.when(F.col("is_offplan"), 1).otherwise(0)).alias("offplan_sales_count"),
            F.sum(F.when(priced, 1).otherwise(0)).alias("priced_sales_count"),
            F.median(F.when(priced, F.col("price_per_sqft"))).alias("median_price_per_sqft"),
            F.median(F.when(priced, F.col("value_aed"))).alias("median_sale_price_aed"),
        ],
    )

    rented = (
        ~F.coalesce(F.col("is_multi_unit"), F.lit(False)) & F.col("annual_rent_aed").isNotNull()
    )
    rents = _with_all_types(
        _by_month(fact_rents),
        [
            F.count(F.lit(1)).alias("rent_count"),
            F.sum(F.when(rented, 1).otherwise(0)).alias("priced_rent_count"),
            F.median(F.when(rented, F.col("annual_rent_aed"))).alias("median_annual_rent_aed"),
        ],
    )

    keys = ["month_key", "area_key", "property_type_sk"]
    joined = sales.join(rents, keys, "full")
    counts = ["sales_count", "offplan_sales_count", "priced_sales_count"]
    counts += ["rent_count", "priced_rent_count"]
    joined = joined.withColumns({c: F.coalesce(F.col(c), F.lit(0)).cast("long") for c in counts})

    enough = (F.col("priced_sales_count") >= min_records) & (
        F.col("priced_rent_count") >= min_records
    )
    joined = joined.withColumn(
        "gross_yield_pct",
        F.when(
            enough & (F.col("median_sale_price_aed") > 0),
            F.round(F.col("median_annual_rent_aed") / F.col("median_sale_price_aed") * 100, 2),
        ),
    )

    year_before = joined.select(
        (F.col("month_key") + 100).alias("month_key"),
        "area_key",
        "property_type_sk",
        F.col("median_price_per_sqft").alias("__last_year"),
    )
    joined = joined.join(year_before, keys, "left").withColumn(
        "median_price_per_sqft_yoy_pct",
        F.when(
            F.col("__last_year") > 0,
            F.round((F.col("median_price_per_sqft") / F.col("__last_year") - 1) * 100, 2),
        ),
    )

    current_areas = dim_area.where(F.col("is_current") & F.col("area_key").isNotNull()).select(
        "area_key", F.col("area_sk").alias("__current_sk"), F.col("area_name_en")
    )
    result = joined.join(F.broadcast(current_areas), "area_key", "left").withColumns(
        {
            "area_sk": F.coalesce(F.col("__current_sk"), F.lit(rules.UNKNOWN_SK)),
            "area_name_en": F.coalesce(F.col("area_name_en"), F.lit(rules.UNKNOWN_LABEL)),
            "area_key": F.nullif(F.col("area_key"), F.lit(_UNMAPPED)),
            "month_start": F.make_date(
                F.floor(F.col("month_key") / 100).cast("int"),
                (F.col("month_key") % 100).cast("int"),
                F.lit(1),
            ),
            "median_price_per_sqft": F.round("median_price_per_sqft", 2),
            "median_sale_price_aed": F.round("median_sale_price_aed", 2),
            "median_annual_rent_aed": F.round("median_annual_rent_aed", 2),
        }
    )
    return result.select(*AGG_COLUMNS)
