# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze ingestion
# MAGIC
# MAGIC Loads every **new** file from the landing volume into `bronze.dld_*_raw`, then stops.
# MAGIC All logic lives in `src/dubai_property/bronze/`; this notebook only calls it.
# MAGIC
# MAGIC - Re-running is safe: Auto Loader skips files it has already loaded.
# MAGIC - Every run writes to `bronze.load_log`, including runs that found nothing new.
# MAGIC - To prove idempotency, run the notebook twice. The second run reports
# MAGIC   `NO_NEW_FILES` and the row counts in the checks below do not change.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.bronze.ingest import run_ingestion
from dubai_property.config import DATASETS, BronzeSettings

dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.multiselect("datasets", "transactions", list(DATASETS))
dbutils.widgets.text("load_id", "")  # optional: ADF passes its pipeline run id here later

catalog = dbutils.widgets.get("catalog").strip()
datasets = [d for d in dbutils.widgets.get("datasets").split(",") if d]
load_id = dbutils.widgets.get("load_id").strip() or None

settings = BronzeSettings.for_databricks(catalog)
print(settings)

# COMMAND ----------

results = []
for name in datasets:
    run_load_id = f"{load_id}-{name}" if load_id else None
    result = run_ingestion(spark, settings, name, load_id=run_load_id)
    results.append(result)
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# Latest load log entries
display(spark.table(settings.load_log_table).orderBy("finished_at", ascending=False).limit(20))

# COMMAND ----------

# Rows per load. Run the notebook twice: this table must not change.
for name in datasets:
    table = settings.table(DATASETS[name].bronze_table)
    if spark.catalog.tableExists(table):
        print(table)
        display(
            spark.sql(f"""
            SELECT _load_id, _load_date, count(*) AS rows, count(_rescued_data) AS rescued
            FROM {table} GROUP BY ALL ORDER BY _load_id
        """)
        )

# COMMAND ----------

# No file may be loaded more than once. Expect zero rows.
for name in datasets:
    table = settings.table(DATASETS[name].bronze_table)
    if spark.catalog.tableExists(table):
        dupes = spark.sql(f"""
            SELECT _source_file, count(DISTINCT _load_id) AS loads
            FROM {table} GROUP BY _source_file HAVING loads > 1
        """)
        n = dupes.count()
        print(f"{table}: {n} file(s) loaded more than once")
        assert n == 0, f"Duplicate loads in {table}"

# COMMAND ----------

# Bronze is append-only: Delta rejects DELETE and UPDATE on these tables
for name in datasets:
    table = settings.table(DATASETS[name].bronze_table)
    if spark.catalog.tableExists(table):
        display(spark.sql(f"SHOW TBLPROPERTIES {table} ('delta.appendOnly')"))
