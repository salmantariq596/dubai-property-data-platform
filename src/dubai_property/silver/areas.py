"""One area, one key: map English, Arabic and misspelled area names together.

Every row gets an ``area_key``, the normalised English name (``"business bay"``),
found in this order:

1. its English name, after the aliases in :data:`rules.AREA_ALIASES`;
2. otherwise its Arabic name, matched against Arabic names already paired with
   an English name, in ``silver.areas`` or elsewhere in the same batch.

Each key gets one display name in English and in Arabic: an alias first, then
the name already in ``silver.areas``, then the most common spelling in the
batch. The names as delivered are kept in ``area_name_*_source``.
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from dubai_property.silver import rules

AREA_COLUMNS = [
    "area_key",
    "area_name_en",
    "area_name_ar",
    "area_name_en_source",
    "area_name_ar_source",
    "is_area_mapped",
]

_KNOWN_WEIGHT = 1_000_000_000  # names already in silver.areas beat any batch majority


def clean_text(column: Column) -> Column:
    """Collapse whitespace and trim; empty becomes null (same as rules.clean_text)."""
    return F.nullif(F.trim(F.regexp_replace(column, rules.WHITESPACE, " ")), F.lit(""))


def normalize_en(column: Column) -> Column:
    return F.lower(clean_text(column))


def normalize_ar(column: Column) -> Column:
    stripped = F.regexp_replace(clean_text(column), rules.ARABIC_STRIP, "")
    return F.nullif(F.translate(stripped, rules.ARABIC_FROM, rules.ARABIC_TO), F.lit(""))


def _literal_map(mapping: dict[str, str]) -> Column | None:
    if not mapping:
        return None
    return F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in mapping.items()))


def _most_common(df: DataFrame, key: str, value: str, alias: str) -> DataFrame:
    """For each key, the value seen most often (ties broken alphabetically)."""
    counts = df.where(F.col(key).isNotNull() & F.col(value).isNotNull()).groupBy(key, value).count()
    window = Window.partitionBy(key).orderBy(F.col("count").desc(), F.col(value))
    return (
        counts.withColumn("__rank", F.row_number().over(window))
        .where("__rank = 1")
        .select(key, F.col(value).alias(alias))
    )


def resolve_areas(
    df: DataFrame,
    known_areas: DataFrame | None = None,
    aliases: dict[str, str] = rules.AREA_ALIASES,
) -> DataFrame:
    """Replace ``area_name_en`` / ``area_name_ar`` with standard names; add the key.

    ``known_areas`` is the current ``silver.areas`` table, or None on a first run.
    """
    key_map = _literal_map(rules.alias_keys(aliases))
    display_map = _literal_map(rules.alias_display(aliases))

    en_norm = normalize_en(F.col("area_name_en"))
    key_from_en = F.coalesce(key_map.getItem(en_norm), en_norm) if key_map is not None else en_norm

    staged = df.withColumns(
        {
            "__en": clean_text(F.col("area_name_en")),
            "__ar": clean_text(F.col("area_name_ar")),
            "__ar_norm": normalize_ar(F.col("area_name_ar")),
            "__key_en": key_from_en,
        }
    )

    # Arabic name -> key, learned from rows that carry both names
    pairs = (
        staged.where(F.col("__key_en").isNotNull() & F.col("__ar_norm").isNotNull())
        .groupBy("__ar_norm", "__key_en")
        .agg(F.count(F.lit(1)).alias("__n"))
    )
    if known_areas is not None:
        known_pairs = known_areas.select(
            "area_key", F.explode("area_name_ar_variants").alias("__variant")
        ).select(
            normalize_ar(F.col("__variant")).alias("__ar_norm"),
            F.col("area_key").alias("__key_en"),
            F.lit(_KNOWN_WEIGHT).cast("long").alias("__n"),
        )
        pairs = pairs.unionByName(known_pairs)

    by_votes = Window.partitionBy("__ar_norm").orderBy(F.col("__n").desc(), F.col("__key_en"))
    ar_lookup = (
        pairs.where(F.col("__ar_norm").isNotNull())
        .withColumn("__rank", F.row_number().over(by_votes))
        .where("__rank = 1")
        .select("__ar_norm", F.col("__key_en").alias("__key_ar"))
    )

    keyed = staged.join(F.broadcast(ar_lookup), on="__ar_norm", how="left").withColumn(
        "area_key", F.coalesce(F.col("__key_en"), F.col("__key_ar"))
    )

    # One display name per key
    keyed = keyed.join(
        F.broadcast(_most_common(keyed, "area_key", "__en", "__batch_en")), "area_key", "left"
    ).join(F.broadcast(_most_common(keyed, "area_key", "__ar", "__batch_ar")), "area_key", "left")
    if known_areas is not None:
        keyed = keyed.join(
            F.broadcast(
                known_areas.select(
                    "area_key",
                    F.col("area_name_en").alias("__known_en"),
                    F.col("area_name_ar").alias("__known_ar"),
                )
            ),
            "area_key",
            "left",
        )
    else:
        keyed = keyed.withColumns(
            {"__known_en": F.lit(None).cast("string"), "__known_ar": F.lit(None).cast("string")}
        )

    alias_name = (
        display_map.getItem(F.col("area_key"))
        if display_map is not None
        else F.lit(None).cast("string")
    )
    resolved = keyed.withColumns(
        {
            "area_name_en": F.coalesce(alias_name, F.col("__known_en"), F.col("__batch_en")),
            "area_name_ar": F.coalesce(F.col("__known_ar"), F.col("__batch_ar")),
            "area_name_en_source": F.col("__en"),
            "area_name_ar_source": F.col("__ar"),
            "is_area_mapped": F.col("area_key").isNotNull(),
        }
    )
    return resolved.drop(*[c for c in resolved.columns if c.startswith("__")])


def build_areas(frames: dict[str, DataFrame]) -> DataFrame:
    """``silver.areas``: one row per area key, from the silver dataset tables.

    ``frames`` maps dataset name to its silver table (rows already resolved).
    """
    parts = [
        df.select(
            "area_key",
            "area_name_en",
            "area_name_ar",
            "area_name_en_source",
            "area_name_ar_source",
            F.lit(name).alias("__dataset"),
        )
        for name, df in frames.items()
    ]
    rows = parts[0]
    for part in parts[1:]:
        rows = rows.unionByName(part)

    rows = rows.where(F.col("area_key").isNotNull())
    return rows.groupBy("area_key").agg(
        F.mode("area_name_en").alias("area_name_en"),
        F.mode("area_name_ar").alias("area_name_ar"),
        F.array_sort(F.collect_set("area_name_en_source")).alias("area_name_en_variants"),
        F.array_sort(F.collect_set("area_name_ar_source")).alias("area_name_ar_variants"),
        F.sum(F.when(F.col("__dataset") == "transactions", 1).otherwise(0)).alias("transactions"),
        F.sum(F.when(F.col("__dataset") == "rents", 1).otherwise(0)).alias("rents"),
        F.current_timestamp().alias("updated_at"),
    )
