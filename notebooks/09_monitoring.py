# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Monitoring
# MAGIC
# MAGIC Is the pipeline healthy, what did each load bring, and what does it cost?
# MAGIC All views read the pipeline's own logs (`src/dubai_property/monitoring.py`):
# MAGIC `bronze.load_log`, `silver.processed_loads`, `quality.dq_results`, `quality.pipeline_runs`.
# MAGIC
# MAGIC Run by hand to look around, or as the `dpdp-health-check` job (`resources/jobs.yml`),
# MAGIC which fails when the pipeline is FAILED or STALE, so its failure email is the alert
# MAGIC for "the pipeline did not run at all".

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from dubai_property import monitoring
from dubai_property.config import PipelineSettings

dbutils.widgets.text("catalog", "dubai")
dbutils.widgets.dropdown("fail_on_unhealthy", "false", ["false", "true"])
dbutils.widgets.text("job_name", "dpdp-daily-pipeline")

catalog = dbutils.widgets.get("catalog").strip()
settings = PipelineSettings.for_databricks(catalog)


def show(df, empty="No data yet."):
    display(df) if df is not None else print(empty)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Health

# COMMAND ----------

health = monitoring.pipeline_health(spark, settings)
print(health)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last run of each step

# COMMAND ----------

show(monitoring.last_runs(spark, settings))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rows per load (bronze)
# MAGIC Choose a bar chart in the cell output: `load_date` × `rows_read`, by `dataset`.

# COMMAND ----------

show(monitoring.rows_per_load(spark, settings))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures and data quality over time

# COMMAND ----------

show(monitoring.failure_trend(spark, settings))

# COMMAND ----------

show(monitoring.dq_trend(spark, settings))

# COMMAND ----------

show(monitoring.quarantine_trend(spark, settings))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost of the daily job
# MAGIC From Databricks system tables (`system.billing.usage`, `system.billing.list_prices`), at
# MAGIC list price. Needs access to the `system` catalog; Azure costs outside Databricks
# MAGIC (storage, Data Factory, Key Vault) are in Azure Cost Management. See `docs/cost.md`.

# COMMAND ----------

job_name = dbutils.widgets.get("job_name").strip()
try:
    display(
        spark.sql(f"""
        WITH job AS (
          SELECT DISTINCT job_id FROM system.lakeflow.jobs WHERE name = '{job_name}'
        ),
        usage AS (
          SELECT u.usage_metadata.job_run_id AS run_id,
                 min(u.usage_start_time) AS started,
                 sum(u.usage_quantity) AS dbus,
                 sum(u.usage_quantity * p.pricing.default) AS list_cost
          FROM system.billing.usage u
          JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name
           AND u.usage_start_time >= p.price_start_time
           AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
          WHERE u.usage_metadata.job_id IN (SELECT job_id FROM job)
            AND u.usage_date >= current_date() - INTERVAL 30 DAYS
          GROUP BY ALL
        )
        SELECT started, run_id, round(dbus, 2) AS dbus, round(list_cost, 2) AS list_cost
        FROM usage ORDER BY started DESC
    """)
    )
except Exception as exc:  # system tables not enabled, or no access
    print(f"Cost data not available here: {str(exc).splitlines()[0]}")

# COMMAND ----------

if dbutils.widgets.get("fail_on_unhealthy") == "true" and not health.healthy:
    raise RuntimeError(f"Pipeline is {health.status}:\n{health}")
