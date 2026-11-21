# Data Factory pipelines

The pipelines in `pipeline/`, as JSON in Data Factory's own export format. They are deployed by the `infra/orchestration` Terraform stack, together with the linked services, datasets and the daily trigger.

| Pipeline | Calls | Purpose |
|---|---|---|
| `pl_daily` | `pl_land_day`, `pl_run_job`, `pl_alert` | Daily load (trigger `tr_daily`, 02:00 Dubai) |
| `pl_backfill` | `pl_land_day`, `pl_run_job`, `pl_alert` | Land a date range, then run the job once |
| `pl_land_day` | `pl_ingest_dataset` | Land every dataset for one day |
| `pl_ingest_dataset` | — | Copy one file to `raw/landing/<dataset>/load_date=<date>/` |
| `pl_run_job` | `pl_alert` | Start and wait for the Databricks job |
| `pl_alert` | — | Webhook alert, then fail |

How it fits together, how to run it, and what to do when it fails: [docs/orchestration.md](../docs/orchestration.md).

**Editing:** change the JSON here (or in Data Factory Studio and copy the JSON back), run `make test-fast` (the tests check every pipeline, parameter, variable and activity reference), then `terraform apply` in `infra/orchestration`.
