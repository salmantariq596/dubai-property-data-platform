# ADR 0008: Data Factory lands files and starts one Databricks job; the job chains the layers

- **Status:** Accepted
- **Date:** 2026-11-21

## Context

The pipeline must run daily without anyone pressing a button. The plan asks for:

- Data Factory pipelines with `dataset` and `load_date` parameters;
- Databricks work chained bronze → silver → data quality → gold;
- failure paths that log and alert;
- a daily schedule and a backfill;
- job definitions in the repo through Databricks Asset Bundles.

## Decisions

### 1. Data Factory moves files; Databricks runs the pipeline

Data Factory does what it is best at: copying files from a web source into the lake on a schedule, with retries. It then starts **one** Databricks job, whose tasks chain the layers. *Alternative considered:* one Data Factory activity per layer (Databricks notebook activities). Each activity would start its own cluster, adding minutes and cost per step. The chain would also be split between two tools, so the job's dependency graph would not be the whole truth.

### 2. The job lives in the repo as an Asset Bundle

`databricks.yml` and `resources/jobs.yml` define the job: six tasks, each `notebooks/08_job_step.py` with its step, each waiting for the previous one. It also sets a single concurrent run, a 4-hour timeout and an email on failure. Two targets:

- **`dev`:** serverless, which also works on Free Edition;
- **`azure`:** one small single-node job cluster shared by all tasks.

Unit tests check that the task order matches `jobs.STEPS` in the Python code and that every task's notebook exists.

### 3. Every step logs itself

`orchestrate.run_step` writes a row to `quality.pipeline_runs` for every step: SUCCESS with a summary, or FAILED with the error. It then re-raises, so the task still fails and later tasks are skipped. This is the plan's "on failure, write to the log", at the level where the failure is understood. Bronze's per-file `load_log` and the data quality `dq_results` stay as they are.

### 4. Data Factory calls Databricks with its managed identity

`pl_run_job` calls the Databricks Jobs API (`run-now`, then `runs/get` once a minute) with Data Factory's managed identity, which has Contributor on the workspace (platform stack). No token is created, stored or rotated. *Alternative considered:* the Data Factory Databricks linked service with a personal access token in Key Vault. It works, but the token is a long-lived secret tied to a person.

### 5. Alerts and failure in one place

Every failure path (landing, starting the job, the job itself) calls `pl_alert`. It posts to an optional Teams or Slack webhook, then ends in a **Fail** activity, so the pipeline run is marked failed in Data Factory's monitor. Databricks also emails on job failure, which covers runs started outside Data Factory.

### 6. Backfill lands every day, then runs the job once

Data Factory cannot nest loops, so landing one day is its own pipeline (`pl_land_day`). `pl_backfill` loops over the days in sequence, then starts the job once. Bronze and silver are incremental by file and by load (ADRs 0002, 0003), so one run processes the whole range. Three years of history is therefore one Databricks run, not about 1,000.

### 7. Pipelines as JSON, deployed by Terraform

The pipelines are JSON in Data Factory's own export format (`adf/pipeline/`), so they can be read, reviewed and diffed like code, and unit tests check every reference:

- the pipelines each pipeline calls exist;
- the parameters it uses are defined;
- the variables it uses are defined;
- the activities it refers to exist.

The `orchestration` Terraform stack deploys them with the linked services, datasets and trigger. It fills in what depends on the deployment: Databricks host and job id, source URLs, webhook. Pipelines are created in dependency order, because Data Factory rejects a call to a pipeline it does not know yet. The daily trigger is deployed **off** and switched on after a successful manual run.

## Consequences

- One schedule, one job, one place to look (`quality.pipeline_runs`) when something fails, and recovery is always "fix and re-run".
- Data Factory needs no secrets: its managed identity reaches both the lake and Databricks.
- The DLD download paths are not confirmed yet. They are parameters (`source_urls`) to set before the first run, and a wrong path fails at landing with an alert, before any data is touched.
- Durations and cost of the full-history and daily runs (plan, Phase 6 step 6) need the deployed platform; record them in `docs/performance.md`.
