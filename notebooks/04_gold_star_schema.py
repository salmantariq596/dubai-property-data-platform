# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold star schema
# MAGIC
# MAGIC Rebuilds the star schema from silver. All logic lives in `src/dubai_property/gold/`.
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `fact_sales` | One registered sale |
# MAGIC | `fact_rents` | One Ejari rental contract |
# MAGIC | `dim_area` | One area version (SCD Type 2) |
# MAGIC | `dim_date`, `dim_property_type`, `dim_project` | One day / type and sub-type / project |
# MAGIC | `agg_area_monthly` | One area x property type x month (+ "All types") |
# MAGIC
# MAGIC Re-running is safe: facts and aggregates are rebuilt from silver, and `dim_area` only
# MAGIC changes when an area's names change.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import GoldSettings
from dubai_property.gold.pipeline import optimize_facts, run_gold, time_query

dbutils.widgets.text("catalog", "dubai")
catalog = dbutils.widgets.get("catalog").strip()

settings = GoldSettings.for_databricks(catalog)
print(settings)

# COMMAND ----------

result = run_gold(spark, settings)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

t = settings.table

# Every sale in silver is in fact_sales exactly once
silver_sales = spark.sql(f"""
    SELECT count(*) FROM {settings.silver_table("transactions")}
    WHERE lower(trim(trans_group_en)) = 'sales'
""").first()[0]
fact_sales = spark.table(t("fact_sales")).count()
print(f"silver sales {silver_sales:,} | fact_sales {fact_sales:,}")
assert silver_sales == fact_sales

# One current version per area
multi = spark.sql(f"""
    SELECT area_key FROM {t("dim_area")} WHERE is_current AND area_key IS NOT NULL
    GROUP BY area_key HAVING count(*) > 1
""").count()
assert multi == 0, "An area has more than one current version"

# Facts pointing at the Unknown row (-1): should be a small share
display(
    spark.sql(f"""
    SELECT 'sales' AS fact,
           avg(CASE WHEN area_sk = -1 THEN 1 ELSE 0 END) AS unknown_area,
           avg(CASE WHEN property_type_sk = -1 THEN 1 ELSE 0 END) AS unknown_type,
           avg(CASE WHEN project_sk = -1 THEN 1 ELSE 0 END) AS unknown_project
    FROM {t("fact_sales")}
    UNION ALL
    SELECT 'rents', avg(CASE WHEN area_sk = -1 THEN 1 ELSE 0 END),
           avg(CASE WHEN property_type_sk = -1 THEN 1 ELSE 0 END),
           avg(CASE WHEN project_sk = -1 THEN 1 ELSE 0 END)
    FROM {t("fact_rents")}
""")
)

# COMMAND ----------

# Area history (SCD Type 2): areas with more than one version
display(
    spark.sql(f"""
    SELECT * FROM {t("dim_area")}
    WHERE area_key IN (SELECT area_key FROM {t("dim_area")} GROUP BY area_key HAVING count(*) > 1)
    ORDER BY area_key, valid_from
""")
)

# COMMAND ----------

# Top areas by sales: median price per sqft, YoY change and gross yield (All types)
display(
    spark.sql(f"""
    SELECT month_start, area_name_en, sales_count, median_price_per_sqft,
           median_price_per_sqft_yoy_pct, median_annual_rent_aed, gross_yield_pct
    FROM {t("agg_area_monthly")}
    WHERE property_type_sk = 0
    ORDER BY month_start DESC, sales_count DESC
    LIMIT 50
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance: before and after OPTIMIZE
# MAGIC
# MAGIC Times a typical dashboard query, compacts and clusters the facts with `OPTIMIZE`,
# MAGIC then times it again. Copy the printed table into `docs/performance.md`.

# COMMAND ----------

BENCHMARK = f"""
    SELECT a.area_name_en, d.year, d.month,
           percentile(f.price_per_sqft, 0.5) AS median_price_per_sqft, count(*) AS sales
    FROM {t("fact_sales")} f
    JOIN {t("dim_area")} a ON f.area_sk = a.area_sk
    JOIN {t("dim_date")} d ON f.date_key = d.date_key
    WHERE d.year = (SELECT max(year) FROM {t("dim_date")} dd
                    JOIN {t("fact_sales")} ff ON ff.date_key = dd.date_key)
      AND NOT f.is_outlier
    GROUP BY ALL
"""

files_before = spark.sql(f"DESCRIBE DETAIL {t('fact_sales')}").first()["numFiles"]
before = time_query(spark, BENCHMARK)
optimize_facts(spark, settings)
files_after = spark.sql(f"DESCRIBE DETAIL {t('fact_sales')}").first()["numFiles"]
after = time_query(spark, BENCHMARK)

print("| Measure | Before OPTIMIZE | After OPTIMIZE |")
print("|---|---:|---:|")
print(f"| fact_sales files | {files_before} | {files_after} |")
print(f"| Benchmark query (best of 3) | {before:.2f} s | {after:.2f} s |")
print(f"\nRows: {fact_sales:,} sales. Run date: {result.as_of}.")
