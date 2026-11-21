# ADR 0009: Deploy with OIDC on merge to main; monitor from the pipeline's own logs

- **Status:** Accepted
- **Date:** 2026-11-21

## Context

The plan's last engineering module asks for:

- CI on every pull request, including `databricks bundle validate`;
- deployment of the bundle on merge to `main`;
- a monitoring view over the load and quality logs;
- a runbook and a cost note.

The platform so far uses no keys or passwords anywhere (ADRs 0007, 0008). Deployment should not be the exception.

## Decisions

### 1. GitHub signs in to Azure with OIDC, not a stored secret

`infra/cicd` creates an Entra application with two federated credentials for this repository: pull requests (to validate) and the `production` environment (to deploy). GitHub issues a short-lived token per run; Entra ID exchanges it for an Azure token. No client secret or Databricks token exists. The four values the workflows need are plain repository variables, and a test fails if any workflow reads `secrets.`.

The identity is a workspace admin (Contributor on the workspace) and has `ALL_PRIVILEGES` on the catalog, granted per principal with `databricks_grant` so other grants are untouched. The deployed jobs run as it, so it needs the data, not only the job definitions.

### 2. Deploy on merge to main, through a GitHub environment

`deploy.yml` runs on pushes to `main` (and by hand). It validates, then runs `databricks bundle deploy -t azure`. The `production` environment is what the federated credential trusts, and it is where a required reviewer can be added. Deployments queue rather than cancel each other: a half-applied bundle is worse than a delayed one.

### 3. Cloud steps wait for the cloud

`bundle-validate` and `deploy` are skipped until the repository variables exist. CI is green before Azure is set up, and the platform can be built module by module before any cloud spend (ADR 0001). `bundle-validate` runs on pull requests from this repository only: forks cannot get an OIDC token, and `main` is validated by the deploy itself.

### 4. Infrastructure is validated automatically, applied by a person

CI runs `terraform fmt` and `validate` on every stack, but `apply` stays manual. Infrastructure changes are rare and can destroy data, so a human reads the plan first. Code changes, which are frequent and safe to roll forward, deploy automatically.

### 5. Monitoring reads the pipeline's own logs

The views in `dubai_property/monitoring.py` read the tables the pipeline already writes:

- `bronze.load_log`: rows per load;
- `silver.processed_loads`: quarantine rate;
- `quality.dq_results`: check trend;
- `quality.pipeline_runs`: last run per step, failures.

`09_monitoring` shows them in Databricks, with no separate monitoring service to run or pay for. The views are tested with Spark like the rest of the pipeline.

### 6. A health check catches the run that never happened

Failure alerts only fire when something runs and fails. `health.assess` combines the latest state of every step with the latest quality results into OK, WARN, STALE or FAILED. STALE means no complete run in 26 hours: a disabled trigger, an expired permission or a paused job. The `dpdp-health-check` job runs it daily at 08:00 Dubai time and fails, and so emails, unless the status is OK or WARN. Like the daily trigger, it is deployed paused.

### 7. Cost: measured from billing data, controlled by design

`docs/cost.md` lists the cost controls already in the design and where each lives, from the budget alert to backfills that run the job once. Measured numbers come from `system.billing.usage` (in `09_monitoring`) and Azure Cost Management after the first real runs. No estimate is presented as a measurement.

## Consequences

- A merged pull request reaches the workspace in minutes, with nothing secret to steal from GitHub or rotate.
- Setting up deployment is a one-time step after the platform exists: apply `infra/cicd`, create the `production` environment, set four variables (`docs/cicd.md`).
- Monitoring and health checks cost only the few seconds of compute their queries take.
- The runbook (`docs/runbook.md`) is the single entry point when something fails; the layer runbooks hold the details.
