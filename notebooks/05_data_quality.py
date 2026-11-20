# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data quality
# MAGIC
# MAGIC Runs the checks of one or more stages and records every result in `quality.dq_results`.
# MAGIC All checks live in `src/dubai_property/quality/checks.py`.
# MAGIC
# MAGIC | Status | Meaning |
# MAGIC |---|---|
# MAGIC | `PASS` | Within limits |
# MAGIC | `WARN` | Recorded for review; the pipeline carries on |
# MAGIC | `FAIL` | Recorded, then this notebook fails, so the next layer does not run |
# MAGIC | `SKIPPED` | A table the check needs has not been built yet |

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property.config import QualitySettings
from dubai_property.quality.checks import STAGES
from dubai_property.quality.runner import run_quality

dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.multiselect("stages", "silver", list(STAGES))
dbutils.widgets.text("load_id", "")  # Data Factory passes its run id here

catalog = dbutils.widgets.get("catalog").strip()
stages = [s for s in dbutils.widgets.get("stages").split(",") if s]
load_id = dbutils.widgets.get("load_id").strip() or None

settings = QualitySettings.for_databricks(catalog)

# COMMAND ----------

for stage in [s for s in STAGES if s in stages]:  # always bronze -> silver -> gold
    print(run_quality(spark, settings, stage, load_id=load_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent results

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT checked_at, stage, rule, status, rows_checked, rows_failed, failed_pct, load_id
    FROM {settings.dq_results_table}
    WHERE status IN ('WARN', 'FAIL')
    ORDER BY checked_at DESC
    LIMIT 50
""")
)

# COMMAND ----------

# Trend per rule: is a warning getting worse?
display(
    spark.sql(f"""
    SELECT date(checked_at) AS day, rule, max(failed_pct) AS failed_pct, max(status) AS status
    FROM {settings.dq_results_table}
    GROUP BY ALL
    ORDER BY rule, day
""")
)
