# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Export gold for Power BI
# MAGIC
# MAGIC Writes every gold table to Parquet in the `gold.exports` volume. Use it when Power BI
# MAGIC cannot connect to a Databricks SQL warehouse (for example on Free Edition):
# MAGIC
# MAGIC 1. Run this notebook.
# MAGIC 2. In **Catalog Explorer → `<catalog>` → `gold` → `exports`**, download each table folder
# MAGIC    to one local folder, for example `C:\dubai-gold\fact_sales\...`.
# MAGIC 3. In Power BI, set the parameters `GoldSource = Parquet` and `GoldFolder = C:\dubai-gold`,
# MAGIC    then refresh. See `powerbi/README.md`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import GoldSettings
from dubai_property.gold.export import export_gold

dbutils.widgets.text("catalog", "dubai")
catalog = dbutils.widgets.get("catalog").strip()

settings = GoldSettings.for_databricks(catalog)
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog}.gold.exports COMMENT 'Parquet exports for Power BI'"
)

# COMMAND ----------

exported = export_gold(spark, settings, f"/Volumes/{catalog}/gold/exports")
for table, rows in exported.items():
    print(f"{table:<20} {rows:>12,} rows")
