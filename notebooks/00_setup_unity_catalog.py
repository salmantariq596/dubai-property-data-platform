# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Unity Catalog setup
# MAGIC
# MAGIC Creates everything bronze needs. Safe to run any number of times.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.raw.landing` volume | Raw DLD files, as delivered: `<dataset>/load_date=YYYY-MM-DD/*.csv` |
# MAGIC | `<catalog>.bronze.checkpoints` volume | Auto Loader checkpoints and inferred schemas |
# MAGIC | `bronze`, `silver`, `gold` schemas | One schema per medallion layer |
# MAGIC
# MAGIC **Databricks Free Edition:** if creating a catalog is not permitted, set the
# MAGIC `catalog` widget to `workspace` (the catalog every Free Edition workspace has)
# MAGIC and use the same value in every other notebook.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))  # repo code, when run from a Git folder

from dubai_property.config import DATASETS

dbutils.widgets.text("catalog", "dubai")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

if catalog != "workspace":
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    except Exception as exc:
        raise RuntimeError(
            f"Could not create catalog '{catalog}'. On Free Edition, set the catalog "
            "widget to 'workspace' and run again."
        ) from exc

for schema, comment in {
    "raw": "Landing zone for raw DLD files",
    "bronze": "Raw data as delivered: all strings, append-only",
    "silver": "Cleaned, typed, deduplicated",
    "gold": "Star schema and aggregates for Power BI",
}.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing COMMENT 'Raw DLD files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.checkpoints COMMENT 'Stream state'")

# COMMAND ----------

# One landing folder per dataset, so files have an obvious place to go
for name in DATASETS:
    os.makedirs(f"/Volumes/{catalog}/raw/landing/{name}", exist_ok=True)

display(spark.sql(f"SHOW VOLUMES IN {catalog}.raw"))
print("Landing folders:", sorted(os.listdir(f"/Volumes/{catalog}/raw/landing")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: land a file
# MAGIC
# MAGIC In **Catalog Explorer → `<catalog>` → `raw` → `landing`**, open the dataset folder,
# MAGIC create a folder named for today's date, for example `load_date=2026-11-08`,
# MAGIC and upload the CSV into it:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/raw/landing/transactions/load_date=2026-11-08/transactions.csv
# MAGIC ```
# MAGIC
# MAGIC Never edit or replace a file after it has been loaded. A correction is a new
# MAGIC file in a new `load_date` folder. Then run `02_bronze_ingestion`.
