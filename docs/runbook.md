# Runbook

Start here when something is wrong. Each section says how you find out, where to look, and what to do. Detailed runbooks per layer are linked at the end of each section.

## First: how healthy is it?

Run `notebooks/09_monitoring` (or open the latest `dpdp-health-check` run). The first cell prints one status with its reasons:

| Status | Meaning | Urgency |
|---|---|---|
| `OK` | Last complete run within 26 hours, no warnings | None |
| `WARN` | Complete, but data quality checks warned | Review this week |
| `STALE` | No complete run for over 26 hours, or never | Today: the dashboard is going out of date |
| `FAILED` | A step's latest run failed, or a check failed | Today: fix and re-run |

```sql
-- The last 24 hours, step by step
SELECT step, status, finished_at, duration_seconds, error_message
FROM dubai.quality.pipeline_runs
WHERE started_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY started_at;
```

**The golden rule:** every step is safe to re-run. Fix the cause, then run the whole pipeline again (`pl_daily` in Data Factory, or *Run now* on `dpdp-daily-pipeline`). Never edit tables by hand.

## Ingestion fails

**You hear about it from:** a Data Factory alert ("Landing source files failed"), or a failed `bronze` task.

| Symptom | Cause | Action |
|---|---|---|
| `pl_ingest_dataset` fails, HTTP 404 or 403 | DLD moved or renamed the file | Find the new path on the portal; update `source_urls` in `infra/orchestration` and `docs/data_sources.md`; apply; re-run `pl_daily` for that date |
| `pl_ingest_dataset` fails, timeout | Portal slow or down | It retries twice; re-run later |
| `bronze` fails: new column | DLD added a column; Auto Loader updated the schema | Re-run; add the column to `docs/data_dictionary.md` |
| `bronze` fails: `MISMATCH` | Rows read back differ from rows read | Stop. Investigate before re-running (see bronze runbook) |
| `quality_bronze` warns: row count change | File much smaller or larger than the last | Check the file; legitimate when coverage changed |

Details: [runbook_bronze.md](runbook_bronze.md).

## Data quality fails

**You hear about it from:** a failed `quality_*` task (email), and a FAILED row in `quality.dq_results`. Gold was **not** rebuilt, so the dashboard still shows the last good data.

```sql
SELECT stage, rule, rows_checked, rows_failed, failed_pct
FROM dubai.quality.dq_results
WHERE status = 'FAIL' ORDER BY checked_at DESC LIMIT 20;
```

| Rule | Cause | Action |
|---|---|---|
| `*_quarantine_rate` | Most of a load was rejected: new layout or date format, or a corrupt file | Look at `silver.quarantine` for that load. Format change: update `silver/rules.py`, reprocess the load. Bad file: ask for a corrected one |
| `*_key_not_null`, `*_key_unique` | A bug: silver guarantees keys | Do not publish. Investigate the code |
| `gold_*_reconciles`, `gold_*_keys_resolve` | Gold lost, doubled or mis-keyed rows | Re-run; if it persists, investigate the code |
| `gold_dim_area_one_current_version` | `dim_area` edited by hand | `RESTORE TABLE dubai.gold.dim_area TO VERSION AS OF <n>` |

Details: [data_quality.md](data_quality.md), [runbook_silver.md](runbook_silver.md).

## Gold fails

**You hear about it from:** a failed `gold` task.

| Symptom | Cause | Action |
|---|---|---|
| `NO_SILVER` | Silver tables missing (first run, or wrong catalog) | Check the job's `catalog` parameter; run silver first |
| Out of memory, or very slow | Data grew | Larger node in the `azure` bundle target; check `OPTIMIZE` ran (`docs/performance.md`) |
| Area renamed unexpectedly | A real DLD rename, or a mapping change | Expected: `dim_area` keeps history (SCD Type 2). Check `AREA_ALIASES` if wrong |

Details: [gold_model.md](gold_model.md).

## The pipeline did not run at all

**You hear about it from:** the health check (`STALE`), or nobody, which is why the health check exists.

1. Data Factory → *Monitor → Trigger runs*: did `tr_daily` fire? Is it started (`enable_schedule = true`)?
2. *Pipeline runs*: did `pl_daily` start and fail early?
3. Databricks → *Workflows*: is `dpdp-daily-pipeline` there, and was it deployed recently (`deploy.yml` run)?
4. Is Data Factory's managed identity still allowed on the workspace (`adf_databricks` role assignment)?

Then run `pl_daily` by hand.

## Deployment fails

**You hear about it from:** a red `Deploy` workflow on `main`.

| Step | Cause | Action |
|---|---|---|
| `azure/login` | Federated credential or repository variables wrong | Compare `AZURE_*` variables with `terraform -chdir=infra/cicd output`; the `production` environment must exist |
| `bundle validate` | Invalid `databricks.yml` / `resources/jobs.yml` | The error names the field; it should also have failed in the pull request |
| `bundle deploy`: permission | Identity not workspace admin yet | Re-apply `infra/cicd`; wait a few minutes for the role to reach Databricks |

The previous job definition keeps running until a deploy succeeds. See [cicd.md](cicd.md).

## Costs jump

**You hear about it from:** the budget email (50%, 80%, forecast 100%).

1. Azure → *Cost Management → Cost analysis*, grouped by resource: Databricks compute is almost always the cause.
2. Databricks → `09_monitoring`, cost section: which job runs cost most?
3. Common causes: a cluster left running, a backfill run day by day instead of once, the schedule triggering more often than daily.

See [cost.md](cost.md).
