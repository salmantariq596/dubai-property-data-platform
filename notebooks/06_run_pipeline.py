# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Run the whole pipeline
# MAGIC
# MAGIC bronze → data quality → silver → data quality → gold → data quality, in one run.
# MAGIC This is the notebook a scheduler (Data Factory, a Databricks job) calls.
# MAGIC
# MAGIC - A `FAIL` check stops the run before the next layer: if silver fails, gold is not
# MAGIC   rebuilt and the dashboard keeps the last good data. The notebook then fails, so the
# MAGIC   scheduler's failure path (alert) runs.
# MAGIC - Every step is safe to repeat. After fixing the cause, run the notebook again.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import PipelineSettings
from dubai_property.orchestrate import DATASETS, run_pipeline

dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.multiselect("datasets", ",".join(DATASETS), list(DATASETS))
dbutils.widgets.text("load_id", "")  # Data Factory passes its pipeline run id here

catalog = dbutils.widgets.get("catalog").strip()
datasets = tuple(d for d in dbutils.widgets.get("datasets").split(",") if d)
load_id = dbutils.widgets.get("load_id").strip() or None

settings = PipelineSettings.for_databricks(catalog)

# COMMAND ----------

result = run_pipeline(spark, settings, datasets=datasets, load_id=load_id)
print(result)

# COMMAND ----------

# Hand a short summary back to the caller (visible in Data Factory's activity output)
dbutils.notebook.exit(
    "; ".join(f"{q.stage}={q.status}" for q in result.quality)
    + f"; fact_sales={result.gold.rows.get('fact_sales', 0)}"
)
