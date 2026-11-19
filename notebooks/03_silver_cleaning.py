# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver cleaning
# MAGIC
# MAGIC Cleans every bronze load that silver has not processed yet, then MERGEs it into
# MAGIC `silver.transactions` / `silver.rents`. All logic lives in `src/dubai_property/silver/`.
# MAGIC
# MAGIC - Values are cast with explicit formats. Rows that break a rule go to
# MAGIC   `silver.quarantine` with the rule names and raw values; nothing is dropped silently.
# MAGIC - One row per business key: the latest ingested version wins.
# MAGIC - Outliers are flagged (`is_outlier`, `outlier_reason`), not deleted.
# MAGIC - Re-running is safe: processed loads are skipped, and MERGE makes a repeat a no-op.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import DATASETS, SilverSettings
from dubai_property.silver.pipeline import run_silver

silver_datasets = [name for name, ds in DATASETS.items() if ds.silver_table]

dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.multiselect("datasets", "transactions", silver_datasets)

catalog = dbutils.widgets.get("catalog").strip()
datasets = [d for d in dbutils.widgets.get("datasets").split(",") if d]

settings = SilverSettings.for_databricks(catalog)
print(settings)

# COMMAND ----------

for name in datasets:
    print(run_silver(spark, settings, name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# Exactly one row per business key. Expect zero rows.
for name in datasets:
    ds = DATASETS[name]
    table = settings.silver_table(ds)
    if spark.catalog.tableExists(table):
        dupes = spark.sql(
            f"SELECT {ds.business_key}, count(*) AS n FROM {table} GROUP BY ALL HAVING n > 1"
        )
        n = dupes.count()
        print(f"{table}: {n} duplicate key(s)")
        assert n == 0, f"Duplicate keys in {table}"

# COMMAND ----------

# Why rows were rejected
if spark.catalog.tableExists(settings.quarantine_table):
    display(
        spark.sql(f"""
        SELECT dataset, rule, count(*) AS rows
        FROM {settings.quarantine_table} LATERAL VIEW explode(failed_rules) r AS rule
        GROUP BY ALL ORDER BY rows DESC
    """)
    )

# COMMAND ----------

# Outliers: flagged, kept, and excluded from medians later in gold
transactions = settings.silver_table(DATASETS["transactions"])
if spark.catalog.tableExists(transactions):
    display(
        spark.sql(f"""
        SELECT coalesce(outlier_reason, 'ok') AS outlier_reason, count(*) AS rows
        FROM {transactions} GROUP BY ALL ORDER BY rows DESC
    """)
    )

# COMMAND ----------

# Areas, and rows whose area could not be mapped (fix with AREA_ALIASES in silver/rules.py)
if spark.catalog.tableExists(settings.areas_table):
    display(spark.table(settings.areas_table).orderBy("transactions", ascending=False))

for name in datasets:
    table = settings.silver_table(DATASETS[name])
    if spark.catalog.tableExists(table):
        unmapped = spark.table(table).where("NOT is_area_mapped").count()
        print(f"{table}: {unmapped} row(s) without an area")

# COMMAND ----------

# Loads processed so far. Run the notebook again: nothing new is processed.
display(spark.table(settings.processed_loads_table).orderBy("processed_at", ascending=False))
