# Cost

> **Status:** the cost model and controls below are built in. The measured numbers are filled in after the first runs on Azure (full history, then a week of daily runs). Until then the table shows `_`: do not quote estimates as measurements.

## What costs money

| Resource | Billed for | Share |
|---|---|---|
| Databricks compute | DBUs plus the VM, while a job cluster (or serverless) runs | Most of the bill |
| ADLS Gen2 | GB stored per month, plus operations | Small: a few GB of CSV and Delta |
| Data Factory | Activity runs, data movement hours | Small: about 20 activities per day |
| Key Vault | Operations | Negligible |
| Databricks workspace | Nothing by itself | — |
| GitHub Actions | Free for public repositories | — |

## Measured cost per run

Fill in from the sources below once the pipeline has run on Azure.

| Run | Duration | DBUs | Databricks cost | Azure total (day) |
|---|---:|---:|---:|---:|
| Full history (backfill, one job run) | _ | _ | _ | _ |
| Daily run | _ | _ | _ | _ |
| Monthly (30 daily runs + storage) | — | _ | _ | _ |

**Where the numbers come from:**

- **Databricks:** `notebooks/09_monitoring`, cost section. It reads `system.billing.usage` and `system.billing.list_prices`, giving DBUs and list price per job run.
- **Azure:** *Cost Management → Cost analysis* for resource group `rg-dpdp-dev`, by day and by resource. It includes the VMs behind job clusters, storage and Data Factory.
- **Duration:** `quality.pipeline_runs`, the sum of `duration_seconds` per run, and the job run page.

## What was done to keep it low

| Control | Where | Effect |
|---|---|---|
| Budget alert before anything else | `infra/bootstrap` | Email at 50%, 80% and forecast 100% of the monthly budget |
| One small single-node job cluster shared by all six tasks | `databricks.yml`, target `azure` | One cluster start per run instead of six; no idle cluster between runs |
| Job clusters, not all-purpose clusters | `resources/jobs.yml` | Lower DBU rate, and the cluster exists only while the job runs |
| Serverless option | `databricks.yml`, target `dev` | No cluster to size or forget; billed per second |
| Incremental loads | bronze (Auto Loader), silver (load ids + MERGE) | A daily run reads only the new day's files |
| Backfill runs the job once | `pl_backfill` | Three years of history is one job run, not about 1,000 |
| Schedule and health check deployed off | `enable_schedule`, `pause_status: PAUSED` | Nothing runs, and nothing bills, until switched on |
| One run at a time, 4-hour timeout | `resources/jobs.yml` | A stuck run cannot keep a cluster up for days |
| Liquid clustering and `OPTIMIZE` on facts | gold, `04_gold_star_schema` | Fewer files read per dashboard query (`docs/performance.md`) |
| Destroy when idle | `infra/README.md` | `terraform destroy` on platform stacks; rebuilt in minutes |
| Develop locally and on Free Edition first | ADR 0001 | Cloud credit spent on deployment, not on debugging |

## The one line for a CV

"A full daily refresh costs about _ USD" goes here once measured (plan, cost rules).
