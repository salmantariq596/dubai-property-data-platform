# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · One step of the scheduled job
# MAGIC
# MAGIC Each task of the `dpdp-daily-pipeline` job (`resources/jobs.yml`) runs this notebook
# MAGIC with a different `step`: bronze → quality_bronze → silver → quality_silver → gold →
# MAGIC quality_gold.
# MAGIC
# MAGIC Every step writes a row to `quality.pipeline_runs`: SUCCESS with a summary, or FAILED
# MAGIC with the error. A failure also fails the task, so the later steps do not run and the
# MAGIC job's failure alert fires. Re-running the job is always safe.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import PipelineSettings
from dubai_property.jobs import STEPS
from dubai_property.orchestrate import DATASETS, run_step

# Job parameters (catalog, datasets, load_id) arrive as widgets; step is set per task.
dbutils.widgets.dropdown("step", STEPS[0], list(STEPS))
dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.text("datasets", ",".join(DATASETS))
dbutils.widgets.text("load_id", "")

step = dbutils.widgets.get("step")
catalog = dbutils.widgets.get("catalog").strip()
datasets = tuple(d.strip() for d in dbutils.widgets.get("datasets").split(",") if d.strip())
load_id = dbutils.widgets.get("load_id").strip() or None

# COMMAND ----------

summary = run_step(spark, PipelineSettings.for_databricks(catalog), step, datasets, load_id)
print(summary)
dbutils.notebook.exit(summary[:1000])
