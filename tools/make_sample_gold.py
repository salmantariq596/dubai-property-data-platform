"""Synthetic gold tables, so the Power BI report can be built before real data is loaded.

    python tools/make_sample_gold.py            # writes data/sample_gold/<table>/part-0.parquet

EVERY NUMBER IS INVENTED. Areas are real place names, but prices, volumes,
rents and projects are made up. Never publish screenshots or insights made
from this data; switch Power BI to real gold first.

The tables have exactly the columns and types the Power BI model expects, and
follow gold's rules (outliers flagged and kept, multi-unit rents, yield only
with 10 records a side, an SCD Type 2 rename), so every visual can be built
and checked against realistic shapes.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dubai_property.gold import rules
from dubai_property.silver import rules as silver_rules

START, END = date(2021, 1, 1), date(2026, 10, 31)
OUTPUT = Path(__file__).parents[1] / "data" / "sample_gold"

# key, English, Arabic, AED/sqft in 2021, yearly growth, gross yield, off-plan share, villas?
AREAS = [
    ("business bay", "Business Bay", "الخليج التجاري", 1300, 0.12, 0.065, 0.45, False),
    ("dubai marina", "Dubai Marina", "مرسى دبي", 1450, 0.10, 0.062, 0.15, False),
    (
        "jumeirah village circle",
        "Jumeirah Village Circle",
        "قرية جميرا الدائرية",
        750,
        0.14,
        0.078,
        0.55,
        False,
    ),
    ("downtown dubai", "Downtown Dubai", "وسط مدينة دبي", 2000, 0.11, 0.055, 0.25, False),
    ("palm jumeirah", "Palm Jumeirah", "نخلة جميرا", 2600, 0.16, 0.048, 0.20, True),
    ("arabian ranches", "Arabian Ranches", "المرابع العربية", 1000, 0.13, 0.050, 0.10, True),
    (
        "jumeirah lakes towers",
        "Jumeirah Lakes Towers",
        "أبراج بحيرات جميرا",
        950,
        0.09,
        0.075,
        0.10,
        False,
    ),
    ("dubai hills estate", "Dubai Hills Estate", "دبي هيلز استيت", 1600, 0.15, 0.055, 0.50, True),
    ("al barsha", "Al Barsha", "البرشاء", 900, 0.07, 0.070, 0.05, False),
    ("international city", "International City", "المدينة العالمية", 450, 0.08, 0.090, 0.05, False),
    (
        "dubai silicon oasis",
        "Dubai Silicon Oasis",
        "واحة دبي للسيليكون",
        700,
        0.09,
        0.080,
        0.20,
        False,
    ),
    ("al furjan", "Al Furjan", "الفرجان", 950, 0.12, 0.065, 0.40, True),
]
# SCD Type 2 example: Dubai Marina was delivered as "Marsa Dubai" until mid-2024
RENAMED_KEY, OLD_NAME, RENAMED_ON = "dubai marina", "Marsa Dubai", date(2024, 7, 1)

TYPES = {  # key: (type, sub-type, size sqft median, price factor)
    "unit|flat": ("Unit", "Flat", 900, 1.00),
    "villa|villa": ("Villa", "Villa", 3500, 0.90),
    "unit|office": ("Unit", "Office", 1100, 0.85),
    "land|residential": ("Land", "Residential", 8000, 0.40),
}
PROJECT_WORDS = ["Residences", "Heights", "Views", "Gardens", "Towers", "Park"]


def sk(*parts: object) -> int:
    """Stable 63-bit surrogate key, like gold's xxhash64 keys."""
    digest = hashlib.blake2b("|".join(map(str, parts)).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") >> 1


def money(values) -> list[Decimal | None]:
    return [None if pd.isna(v) else Decimal(f"{v:.2f}") for v in values]


def write(df: pd.DataFrame, name: str, schema: pa.Schema, out: Path) -> None:
    folder = out / name
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("*.parquet"):
        old.unlink()
    arrays = []
    for field in schema:
        values = df[field.name].tolist()
        if pa.types.is_decimal(field.type):
            values = money(values)
        elif pa.types.is_integer(field.type):
            values = [None if pd.isna(v) else int(v) for v in values]
        elif pa.types.is_floating(field.type):
            values = [None if pd.isna(v) else float(v) for v in values]
        elif pa.types.is_string(field.type):
            values = [
                None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
                for v in values
            ]
        arrays.append(pa.array(values, type=field.type))
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), folder / "part-0.parquet")
    print(f"{name:<20} {len(df):>8,} rows")


def build(seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)

    # --- dimensions -------------------------------------------------------------------
    days = pd.date_range(rules.DATE_START, rules.DATE_END, freq="D").date
    dim_date = pd.DataFrame(
        {
            "date_key": [rules.date_key(d) for d in days],
            "date": days,
            "year": [d.year for d in days],
            "quarter": [(d.month - 1) // 3 + 1 for d in days],
            "month": [d.month for d in days],
            "month_name": [d.strftime("%B") for d in days],
            "month_key": [d.year * 100 + d.month for d in days],
            "month_start": [d.replace(day=1) for d in days],
            "day_of_month": [d.day for d in days],
            "iso_weekday": [d.isoweekday() for d in days],
            "day_name": [d.strftime("%A") for d in days],
            "is_weekend": [rules.is_weekend(d) for d in days],
        }
    )

    area_rows = []
    for key, en, ar, *_ in AREAS:
        if key == RENAMED_KEY:
            old_to = RENAMED_ON - timedelta(days=1)
            area_rows.append(
                (
                    sk(key, rules.VALID_FROM_FIRST),
                    key,
                    OLD_NAME,
                    ar,
                    rules.VALID_FROM_FIRST,
                    old_to,
                    False,
                )
            )
            area_rows.append(
                (sk(key, RENAMED_ON), key, en, ar, RENAMED_ON, rules.VALID_TO_OPEN, True)
            )
        else:
            area_rows.append(
                (
                    sk(key, rules.VALID_FROM_FIRST),
                    key,
                    en,
                    ar,
                    rules.VALID_FROM_FIRST,
                    rules.VALID_TO_OPEN,
                    True,
                )
            )
    area_rows.append(
        (
            rules.UNKNOWN_SK,
            None,
            rules.UNKNOWN_LABEL,
            None,
            rules.VALID_FROM_FIRST,
            rules.VALID_TO_OPEN,
            True,
        )
    )
    dim_area = pd.DataFrame(
        area_rows,
        columns=[
            "area_sk",
            "area_key",
            "area_name_en",
            "area_name_ar",
            "valid_from",
            "valid_to",
            "is_current",
        ],
    )
    dim_area["attr_hash"] = [
        sk(en, ar) and f"{sk(en, ar):016x}"
        for en, ar in zip(dim_area.area_name_en, dim_area.area_name_ar, strict=True)
    ]

    dim_property_type = pd.DataFrame(
        [(sk(k), k, t, s, True, k != "land|residential") for k, (t, s, _, _) in TYPES.items()]
        + [
            (rules.UNKNOWN_SK, None, rules.UNKNOWN_LABEL, None, False, False),
            (rules.ALL_TYPES_SK, None, rules.ALL_TYPES_LABEL, None, False, False),
        ],
        columns=[
            "property_type_sk",
            "property_type_key",
            "property_type",
            "property_sub_type",
            "in_sales",
            "in_rents",
        ],
    )

    projects = []
    for i, (key, en, *_rest) in enumerate(AREAS):
        for j in range(2):
            name = f"{en} {PROJECT_WORDS[(i + j) % len(PROJECT_WORDS)]}"  # unique per area
            projects.append((sk(name.lower()), name.lower(), name, f"P-{100 + 2 * i + j}", en, key))
    dim_project = pd.DataFrame(
        [(p[0], p[1], p[2], p[3], p[4], None, None, True, True) for p in projects]
        + [(rules.UNKNOWN_SK, None, rules.UNKNOWN_LABEL, None, None, None, None, False, False)],
        columns=[
            "project_sk",
            "project_key",
            "project_name_en",
            "project_number",
            "master_project_en",
            "developer_name",
            "project_status",
            "in_sales",
            "in_rents",
        ],
    )
    projects_by_area = {}
    for p in projects:
        projects_by_area.setdefault(p[5], []).append(p[0])

    type_sk = dict(
        zip(dim_property_type.property_type_key, dim_property_type.property_type_sk, strict=True)
    )
    months = pd.period_range(START, END, freq="M")

    def area_sk_on(key: str, day: date) -> int:
        if key == RENAMED_KEY and day < RENAMED_ON:
            return sk(key, rules.VALID_FROM_FIRST)
        return sk(key, RENAMED_ON if key == RENAMED_KEY else rules.VALID_FROM_FIRST)

    def type_mix(villas: bool) -> tuple[list[str], list[float]]:
        if villas:
            return ["unit|flat", "villa|villa", "land|residential"], [0.55, 0.42, 0.03]
        return ["unit|flat", "unit|office", "land|residential"], [0.85, 0.13, 0.02]

    # --- sales ------------------------------------------------------------------------
    sales, rents = [], []
    for t, month in enumerate(months):
        years = t / 12
        for key, _en, _ar, base, growth, yield_, offplan, villas in AREAS:
            kinds, weights = type_mix(villas)
            level = base * (1 + growth) ** years
            for _ in range(rng.poisson(14 + 0.25 * t)):
                kind = rng.choice(kinds, p=weights)
                _, _, size_median, factor = TYPES[kind]
                day = (
                    month.start_time + pd.Timedelta(days=int(rng.integers(0, month.days_in_month)))
                ).date()
                is_offplan = bool(rng.random() < min(0.9, offplan * (0.6 + 0.8 * t / len(months))))
                size = size_median * rng.lognormal(0, 0.35)
                ppsf = level * factor * rng.lognormal(0, 0.15) * (1.05 if is_offplan else 1.0)
                value = ppsf * size
                if rng.random() < 0.01:  # data-entry problems: kept, flagged as outliers
                    size = 0.0
                price_per_sqft = value / size if size > 0 else None
                is_outlier = (
                    size <= 0
                    or price_per_sqft < silver_rules.PRICE_PER_SQFT_MIN
                    or price_per_sqft > silver_rules.PRICE_PER_SQFT_MAX
                )
                sales.append(
                    {
                        "transaction_id": f"SAMPLE-S-{len(sales) + 1:07d}",
                        "date_key": rules.date_key(day),
                        "area_sk": area_sk_on(key, day),
                        "property_type_sk": type_sk[kind],
                        "project_sk": rng.choice(projects_by_area[key])
                        if rng.random() < 0.8
                        else rules.UNKNOWN_SK,
                        "area_key": key,
                        "size_sqft": size,
                        "value_aed": value,
                        "value_usd": value / silver_rules.AED_PER_USD,
                        "price_per_sqft": price_per_sqft,
                        "is_offplan": is_offplan,
                        "is_outlier": bool(is_outlier),
                        "bedrooms": int(rng.integers(0, 4)) if kind == "unit|flat" else None,
                        "procedure_name_en": "Sell - Pre registration" if is_offplan else "Sell",
                        "property_usage_en": "Commercial"
                        if kind == "unit|office"
                        else "Residential",
                        "_load_id": "sample",
                        "_day": day,
                    }
                )
            # --- rents for the same area and month ------------------------------------
            for _ in range(rng.poisson(18 + 0.2 * t)):
                kind = rng.choice(kinds[:2], p=np.array(weights[:2]) / sum(weights[:2]))
                _, _, size_median, factor = TYPES[kind]
                day = (
                    month.start_time + pd.Timedelta(days=int(rng.integers(0, month.days_in_month)))
                ).date()
                size = size_median * rng.lognormal(0, 0.3)
                multi = bool(rng.random() < 0.02)
                rent = level * factor * size * yield_ * rng.lognormal(0, 0.12) * (8 if multi else 1)
                end = day + timedelta(days=364)
                rents.append(
                    {
                        "contract_id": f"SAMPLE-R-{len(rents) + 1:07d}",
                        "date_key": rules.date_key(day),
                        "end_date_key": rules.date_key(end),
                        "area_sk": area_sk_on(key, day),
                        "property_type_sk": type_sk[kind],
                        "project_sk": rng.choice(projects_by_area[key])
                        if rng.random() < 0.6
                        else rules.UNKNOWN_SK,
                        "area_key": key,
                        "annual_rent_aed": rent,
                        "size_sqft": size * (8 if multi else 1),
                        "contract_days": 365,
                        "is_multi_unit": multi,
                        "contract_reg_type_en": "Renew" if rng.random() < 0.45 else "New",
                        "property_usage_en": "Commercial"
                        if kind == "unit|office"
                        else "Residential",
                        "_load_id": "sample",
                        "_day": day,
                    }
                )
    fact_sales, fact_rents = pd.DataFrame(sales), pd.DataFrame(rents)

    # --- agg_area_monthly, with gold's rules -------------------------------------------------
    s = fact_sales.assign(month_key=fact_sales.date_key // 100)
    s["priced"] = ~s.is_outlier & s.price_per_sqft.notna()
    r = fact_rents.assign(month_key=fact_rents.date_key // 100)
    r["priced"] = ~r.is_multi_unit & r.annual_rent_aed.notna()

    def sales_agg(g: pd.DataFrame) -> pd.Series:
        p = g[g.priced]
        return pd.Series(
            {
                "sales_count": len(g),
                "sales_value_aed": g.value_aed.sum(),
                "offplan_sales_count": int(g.is_offplan.sum()),
                "priced_sales_count": len(p),
                "median_price_per_sqft": p.price_per_sqft.median(),
                "median_sale_price_aed": p.value_aed.median(),
            }
        )

    def rents_agg(g: pd.DataFrame) -> pd.Series:
        p = g[g.priced]
        return pd.Series(
            {
                "rent_count": len(g),
                "priced_rent_count": len(p),
                "median_annual_rent_aed": p.annual_rent_aed.median(),
            }
        )

    keys = ["month_key", "area_key", "property_type_sk"]

    def with_all_types(df: pd.DataFrame, fn) -> pd.DataFrame:
        by_type = df.groupby(keys).apply(fn, include_groups=False).reset_index()
        all_types = df.groupby(keys[:2]).apply(fn, include_groups=False).reset_index()
        all_types["property_type_sk"] = rules.ALL_TYPES_SK
        return pd.concat([by_type, all_types], ignore_index=True)

    agg = with_all_types(s, sales_agg).merge(with_all_types(r, rents_agg), on=keys, how="outer")
    for c in [
        "sales_count",
        "offplan_sales_count",
        "priced_sales_count",
        "rent_count",
        "priced_rent_count",
    ]:
        agg[c] = agg[c].fillna(0).astype("int64")
    enough = (agg.priced_sales_count >= rules.MIN_RECORDS_FOR_YIELD) & (
        agg.priced_rent_count >= rules.MIN_RECORDS_FOR_YIELD
    )
    agg["gross_yield_pct"] = np.where(
        enough, (agg.median_annual_rent_aed / agg.median_sale_price_aed * 100).round(2), np.nan
    )
    last_year = agg[keys + ["median_price_per_sqft"]].rename(
        columns={"median_price_per_sqft": "ly"}
    )
    last_year["month_key"] += 100
    agg = agg.merge(last_year, on=keys, how="left")
    agg["median_price_per_sqft_yoy_pct"] = ((agg.median_price_per_sqft / agg.ly - 1) * 100).round(2)
    current = dim_area[dim_area.is_current & dim_area.area_key.notna()][
        ["area_key", "area_sk", "area_name_en"]
    ]
    agg = agg.merge(current, on="area_key", how="left")
    agg["month_start"] = [date(int(m) // 100, int(m) % 100, 1) for m in agg.month_key]
    for c in ["median_price_per_sqft", "median_sale_price_aed", "median_annual_rent_aed"]:
        agg[c] = agg[c].round(2)

    return {
        "dim_date": dim_date,
        "dim_area": dim_area,
        "dim_property_type": dim_property_type,
        "dim_project": dim_project,
        "fact_sales": fact_sales,
        "fact_rents": fact_rents,
        "agg_area_monthly": agg,
    }


TEXT, INT, FLOAT, BOOL, DATE = pa.string(), pa.int64(), pa.float64(), pa.bool_(), pa.date32()
MONEY = pa.decimal128(18, 2)
SCHEMAS = {
    "dim_date": [
        ("date_key", INT),
        ("date", DATE),
        ("year", INT),
        ("quarter", INT),
        ("month", INT),
        ("month_name", TEXT),
        ("month_key", INT),
        ("month_start", DATE),
        ("day_of_month", INT),
        ("iso_weekday", INT),
        ("day_name", TEXT),
        ("is_weekend", BOOL),
    ],
    "dim_area": [
        ("area_sk", INT),
        ("area_key", TEXT),
        ("area_name_en", TEXT),
        ("area_name_ar", TEXT),
        ("attr_hash", TEXT),
        ("valid_from", DATE),
        ("valid_to", DATE),
        ("is_current", BOOL),
    ],
    "dim_property_type": [
        ("property_type_sk", INT),
        ("property_type_key", TEXT),
        ("property_type", TEXT),
        ("property_sub_type", TEXT),
        ("in_sales", BOOL),
        ("in_rents", BOOL),
    ],
    "dim_project": [
        ("project_sk", INT),
        ("project_key", TEXT),
        ("project_name_en", TEXT),
        ("project_number", TEXT),
        ("master_project_en", TEXT),
        ("developer_name", TEXT),
        ("project_status", TEXT),
        ("in_sales", BOOL),
        ("in_rents", BOOL),
    ],
    "fact_sales": [
        ("transaction_id", TEXT),
        ("date_key", INT),
        ("area_sk", INT),
        ("property_type_sk", INT),
        ("project_sk", INT),
        ("area_key", TEXT),
        ("size_sqft", MONEY),
        ("value_aed", MONEY),
        ("value_usd", MONEY),
        ("price_per_sqft", MONEY),
        ("is_offplan", BOOL),
        ("is_outlier", BOOL),
        ("bedrooms", INT),
        ("procedure_name_en", TEXT),
        ("property_usage_en", TEXT),
        ("_load_id", TEXT),
    ],
    "fact_rents": [
        ("contract_id", TEXT),
        ("date_key", INT),
        ("end_date_key", INT),
        ("area_sk", INT),
        ("property_type_sk", INT),
        ("project_sk", INT),
        ("area_key", TEXT),
        ("annual_rent_aed", MONEY),
        ("size_sqft", MONEY),
        ("contract_days", INT),
        ("is_multi_unit", BOOL),
        ("contract_reg_type_en", TEXT),
        ("property_usage_en", TEXT),
        ("_load_id", TEXT),
    ],
    "agg_area_monthly": [
        ("month_key", INT),
        ("month_start", DATE),
        ("area_key", TEXT),
        ("area_sk", INT),
        ("area_name_en", TEXT),
        ("property_type_sk", INT),
        ("sales_count", INT),
        ("sales_value_aed", pa.decimal128(28, 2)),
        ("offplan_sales_count", INT),
        ("priced_sales_count", INT),
        ("median_price_per_sqft", FLOAT),
        ("median_price_per_sqft_yoy_pct", FLOAT),
        ("median_sale_price_aed", FLOAT),
        ("rent_count", INT),
        ("priced_rent_count", INT),
        ("median_annual_rent_aed", FLOAT),
        ("gross_yield_pct", FLOAT),
    ],
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    tables = build(args.seed)
    for name, fields in SCHEMAS.items():
        write(tables[name], name, pa.schema(fields), args.output)
    print(f"\nWritten to {args.output.resolve()}  (synthetic data: do not publish)")


if __name__ == "__main__":
    main()
