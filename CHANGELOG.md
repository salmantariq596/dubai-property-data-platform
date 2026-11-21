# Changelog

Each module is merged with a pull request and tagged.

## [Unreleased] — Module 9: CI/CD, monitoring and cost (code ready, setup pending)

### Added
- `.github/workflows/deploy.yml`: on merge to `main`, validate and deploy the Databricks bundle through the `production` environment. GitHub signs in to Azure with OIDC; there are no secrets.
- CI job `bundle-validate` on pull requests. Deploy and bundle validation are skipped until the repository variables exist, so CI stays green before Azure.
- `infra/cicd` Terraform stack:
  - Entra app with federated credentials for pull requests and the `production` environment, and no client secret;
  - Contributor on the workspace;
  - `ALL_PRIVILEGES` on the catalog for the identity the jobs run as.

  CI validates it.
- `dubai_property.monitoring` views:
  - `last_runs`: last run per step;
  - `rows_per_load`;
  - `failure_trend`;
  - `dq_trend`;
  - `quarantine_trend`;
  - `pipeline_health`.

  `dubai_property.health.assess` turns them into OK / WARN / STALE / FAILED. STALE (no complete run in 26 hours) catches the run that never happened.
- `notebooks/09_monitoring.py`: health, views and job cost from `system.billing`. New bundle job `dpdp-health-check`: daily at 08:00 Dubai, paused by default, fails and emails when the pipeline is FAILED or STALE.
- `docs/runbook.md` (single entry point when something fails), `docs/cost.md` (cost model, controls in place, how to measure), `docs/cicd.md` (how a code change reaches production; one-time setup), ADR 0009.
- 15 unit tests:
  - health rules;
  - deploy only from `main` through `production`;
  - no `secrets.` in any workflow;
  - cloud jobs off until configured;
  - federated credential subjects match the workflows;
  - health check paused and alerting.

  Plus 5 Spark tests for the monitoring views.

### To do before v0.9.0
- Apply `infra/cicd`, create the `production` environment, set the four repository variables, see the first green deploy; fill `docs/cost.md` with measured numbers; unpause the health check.

## [Unreleased] — Module 8: Orchestration with Data Factory (code ready, deployment pending)

### Added
- Databricks Asset Bundle (`databricks.yml`, `resources/jobs.yml`): job `dpdp-daily-pipeline` with six chained tasks (bronze → quality_bronze → silver → quality_silver → gold → quality_gold), one concurrent run, 4-hour timeout, email on failure. Targets `dev` (serverless, works on Free Edition) and `azure` (one shared single-node job cluster).
- `notebooks/08_job_step.py` and `orchestrate.run_step`: every step writes SUCCESS (with a summary) or FAILED (with the error) to `quality.pipeline_runs`, then re-raises so the job stops.
- Data Factory pipelines (`adf/pipeline/`):
  - `pl_ingest_dataset` (copy one file to `raw/landing`) and `pl_land_day`;
  - `pl_run_job`: start the job with the factory's managed identity, poll, alert on failure;
  - `pl_alert`: webhook, then Fail;
  - `pl_daily`, and `pl_backfill`, which lands a date range then runs the job once.
- `infra/orchestration` Terraform stack: linked services (lake via managed identity, DLD source over HTTP), binary datasets, the pipelines in dependency order, and the daily 02:00 Dubai trigger (deployed off).
- 39 unit tests:
  - every pipeline, parameter, variable, activity and dataset reference resolves;
  - pipelines are created after the pipelines they call;
  - Databricks is called with the managed identity;
  - every failure path alerts and fails;
  - the job's tasks match the Python step order.
- 5 Spark tests for step logging. CI validates the new Terraform stack.
- ADR 0008, `docs/orchestration.md` (deploy, run, backfill, failure handling, re-running after a failure), `adf/README.md`.

### To do before v0.8.0
- Confirm the DLD download paths; deploy the bundle and the stack; run `pl_daily` by hand, then a backfill; switch on the schedule; record durations and cost; screenshot the run history.

## [Unreleased] — Module 7: Azure infrastructure with Terraform (code ready, apply pending)

### Added
- `infra/bootstrap`: monthly subscription budget with email alerts (50% and 80% actual, 100% forecast); versioned, soft-deleted storage for Terraform state.
- `infra/platform`, in UAE North:
  - resource group;
  - ADLS Gen2 lake with `raw`, `bronze`, `silver` and `gold` containers;
  - Databricks workspace (Premium) and access connector;
  - Key Vault (RBAC);
  - Data Factory.
  Storage account keys are disabled, and every connection is a managed identity with one role.
- `infra/unity-catalog`: storage credential on the access connector's managed identity, an external location per container, catalog `dubai` with `raw`/`bronze`/`silver`/`gold`/`quality` schemas (each layer's managed tables in its own container), and the `landing`, `checkpoints` and `exports` volumes.
- CI job: `terraform fmt -check` and `terraform validate` on all three stacks.
- 14 unit tests guarding the design: no account keys, no public blobs, TLS 1.2, ADLS Gen2, Premium workspace, RBAC Key Vault, managed identity for Unity Catalog, remote state, no real ids or emails in example files.
- ADR 0007 and `infra/README.md` (build, destroy-and-rebuild proof, costs).

### To do before v0.7.0
- Activate the Azure subscription, apply the three stacks, prove destroy and rebuild, screenshot the resource group.

## [Unreleased] — Module 6: Power BI dashboard (model ready, pages to build)

### Added
- Power BI semantic model as TMDL (`powerbi/model/definition/`):
  - 7 gold tables and 11 relationships;
  - 17 DAX measures: medians with gold's outlier rules, YoY change, off-plan share and premium, and gross yield with the 10-record minimum;
  - current English and Arabic area names for slicers and tooltips.
- One M source function, `GoldTable`, switching between a Databricks SQL warehouse and Parquet files with a single parameter.
- `dubai_property.gold.export.export_gold` and `notebooks/07_export_gold.py`: gold to Parquet when no SQL warehouse is available.
- Report theme (`powerbi/theme/dubai-market.json`) and a page-by-page design for the four pages, slicers and Arabic tooltip (`docs/dashboard.md`).
- Tests:
  - 52 unit tests that the model is consistent: DAX references resolve, relationships join existing columns, keys are hidden, TMDL indentation is valid;
  - 8 Spark tests that every model column matches the name and type gold produces, and that the Parquet export round-trips.
- ADR 0006 and `powerbi/README.md` (create the project, connect, build, export).

### Added (report)
- The Power BI project `powerbi/DubaiPropertyMarket.pbip`:
  - four pages built to `docs/dashboard.md`: Market overview, Area deep-dive, Off-plan vs ready, Rental yield;
  - Year, Area and Property type slicers synced across pages;
  - the Arabic area name in the tooltip of every area visual.
- Measures `Price Growth % (50+ sales)`, `Off-plan Premium % (20+ off-plan)` and `Area (Arabic)`, and the column `fact_sales[Registration]`.
- `tools/make_sample_gold.py`: synthetic gold tables (git-ignored) to build the report before real data exists.

### Changed
- The project's own model is the single source of truth: `powerbi/model/` removed, and the tests read `DubaiPropertyMarket.SemanticModel`.
- The Databricks connector is looked up at run time, so the Parquet route works on Power BI versions that have not loaded the connector.
- `Median Price per Sqft YoY %` rewritten without variables.
- Lint covers `tools/`.
- CI split into parallel jobs: lint and unit tests (no Spark), three Spark test groups on separate runners, and one combined coverage report. A newer push cancels the run in progress; the Delta jar cache no longer resets on every `pyproject.toml` change; Delta log replay uses 2 tasks instead of 50 in tests. A unit test fails if a Spark test file is missing from the CI groups.

### To do before v0.6.0
- Load real data, export the PDF and screenshots, and write three insights.

## [v0.5.0] — Module 5: Data quality and integration tests — 2026-11-20

### Added
- `dubai_property.quality.checks`: 25 checks between the layers, each one SQL query (rows checked, rows failed) with WARN and FAIL thresholds on the failed share:
  - **bronze:** row-count change within 30% of the previous load; last load succeeded;
  - **silver:** keys not null and unique, dates inside the calendar and not in the future, positive sizes, values and rents, areas mapped, quarantine rate;
  - **gold:** facts reconcile with silver, keys unique and resolving to dimensions, one current version per area, known areas, plausible yields.
- `dubai_property.quality.runner`: `run_quality` evaluates a stage and records every result in `quality.dq_results`, then raises `DataQualityError` on FAIL. Checks on tables not built yet are SKIPPED.
- `dubai_property.orchestrate.run_pipeline`: bronze → checks → silver → checks → gold → checks. A FAIL stops the run before the next layer, so gold keeps the last good data.
- `QualitySettings` and `PipelineSettings` in `config.py`; `quality` schema in `00_setup_unity_catalog`.
- Notebooks `05_data_quality.py` (run checks, review warnings and trends) and `06_run_pipeline.py` (the single entry point for a scheduler).
- `tests/fixtures/sample_rents.csv`: synthetic Ejari contracts with planted problems.
- Integration tests (`tests/integration/`) running bronze to gold on both fixtures: exact row counts per layer and statuses per stage, a mostly broken load stopped at silver with gold untouched, and a re-run that changes nothing.
- 61 new tests: 49 unit tests for the check catalogue and thresholds, 9 Spark tests that break each kind of check on purpose, and 3 integration tests.
- ADR 0005 and `docs/data_quality.md` (rule catalogue generated from code, `dq_results`, what to do when a check fails).

### Changed
- Spark fixtures moved to `tests/conftest.py` and shared by `tests/spark` and `tests/integration`; each Spark folder skips itself via `tests/spark_guard.py` when local Spark is unavailable.

## [v0.4.0] — Module 4: Gold star schema — 2026-11-20

### Added
- `dubai_property.gold.dimensions`:
  - `dim_date` (2000–2030, with the UAE weekend change of 2022);
  - `dim_property_type`, shared by sales and rents through a type mapping;
  - `dim_project`;
  - `dim_area` as SCD Type 2: renames close the old version and open a new one, same-day changes update in place, and re-runs change nothing.
- `dubai_property.gold.facts`:
  - `fact_sales` (sales only) and `fact_rents`, with point-in-time area lookups and `-1` Unknown rows so no fact is lost in a join;
  - `agg_area_monthly`: median price per sqft (outliers excluded), median sale price, median annual rent (multi-unit excluded), counts, YoY change, and gross yield with a minimum-sample rule. Includes an "All types" roll-up.
- Surrogate keys are `xxhash64` of natural keys: stable across rebuilds.
- `dubai_property.gold.pipeline`: `run_gold` rebuilds gold from silver (facts liquid-clustered by date on Databricks), `optimize_facts`, and `time_query` for benchmarks.
- `GoldSettings` in `config.py`.
- `notebooks/04_gold_star_schema.py` with model checks and a before/after `OPTIMIZE` benchmark.
- 52 new tests: 30 unit tests for gold rules, 17 Spark tests for dimensions, SCD Type 2, facts and the aggregate, and 5 Delta tests running bronze → silver → gold end to end.
- ADR 0004, `docs/gold_model.md` (model diagram, grains, measures) and `docs/performance.md` (to be filled from a Databricks run).

### Changed
- Spark test row builders moved to `tests/spark/builders.py`, shared by the silver and gold tests.
- Business questions: gross yield needs at least 10 records on each side.

## [v0.3.0] — Module 3: Silver cleaning — 2026-11-19

### Added
- `dubai_property.silver.rules`: every silver column, type and required flag, plus date and number patterns, unit constants, outlier thresholds and area aliases, as plain data with no Spark dependency.
- `dubai_property.silver.transform`: explicit casts with `try_cast` (ISO and day-first dates only, thousands separators removed). Rule checks send bad rows to quarantine with the rules they broke. Rows are deduplicated on the business key (latest ingested wins). Derived columns: `size_sqft`, `price_per_sqft`, `value_usd`, `is_offplan`, `bedrooms` and annualised rent. Outliers are flagged, not dropped.
- `dubai_property.silver.areas`: one `area_key` per area across English, Arabic and spelling variants. Arabic spellings are folded for matching, English names are filled from Arabic, and abbreviations are mapped via `AREA_ALIASES`. Builds `silver.areas`.
- `dubai_property.silver.pipeline.run_silver`: processes bronze loads not yet in `silver.processed_loads`, MERGEs into `silver.transactions` / `silver.rents` on the business key (older data never overwrites newer) and into `silver.quarantine` on a row hash. Every step is safe to repeat.
- `SilverSettings` and `Dataset.silver_table` in `config.py`.
- `notebooks/03_silver_cleaning.py` with checks for duplicate keys, quarantine by rule, outliers and unmapped areas.
- 65 new tests: 41 unit tests for the rules, 18 Spark tests for the transformations, and 6 Delta tests for MERGE, re-runs, replays and reprocessing.
- ADR 0003, `docs/runbook_silver.md`, and silver tables in the data dictionary.

## [v0.2.0] — Module 2: Bronze ingestion — 2026-11-08

### Added
- `dubai_property.bronze.ingest.run_ingestion`: Auto Loader (`cloudFiles`) with `availableNow`, all-string columns, snake_case headers, and `_ingested_at`, `_source_file`, `_load_id`, `_load_date` metadata columns.
- Idempotent `foreachBatch` appends using `txnAppId` + `txnVersion`, so a batch replayed after a crash is not written twice.
- Per-file read-back reconciliation and `bronze.load_log` (statuses `SUCCESS`, `MISMATCH`, `NO_NEW_FILES`, `FAILED`).
- `delta.appendOnly` enforced on bronze tables and the load log.
- `config.py`: one source of truth for datasets, volume paths and table names.
- Databricks notebooks in source format: `00_setup_unity_catalog.py` (catalog, schemas, volumes) and `02_bronze_ingestion.py` (run plus idempotency checks).
- 26 new tests: unit tests for naming, config and load log, and Spark tests that run the ingestion end to end. They cover re-runs, new files, quoted multi-line CSV, Arabic text and failed runs. Delta-only tests cover replayed batches and blocked deletes.
- CI installs Java 17, PySpark and Delta Lake, and requires Delta (`REQUIRE_DELTA=1`).
- ADR 0002 and `docs/runbook_bronze.md`.

### Changed
- Makefile: `setup-spark` and `test-fast` targets.

### Fixed
- Makefile works on Windows (`.venv/Scripts` instead of `.venv/bin`).

## [v0.1.0] — Module 1: Repo skeleton and data discovery — 2026-11-04

### Added
- Repository layout for the full platform (`src/`, `tests/`, `infra/`, `adf/`, `notebooks/`, `powerbi/`, `docs/`).
- Tooling: `pyproject.toml`, ruff, pre-commit hooks (including private-key detection), Makefile, GitHub Actions CI running lint and tests.
- `dubai_property.profiling` package and `dld-profile` CLI: reads raw CSVs as strings (UTF-8, BOM-safe), profiles nulls, distinct counts, numeric/date castability, Arabic text and duplicate business keys, and writes a Markdown report.
- Synthetic fixture with one planted example of each data problem, and 21 unit tests.
- `notebooks/01_data_profiling.ipynb` — thin notebook over the package.
- Docs: data sources, data dictionary (expected schema, to confirm), business questions, ADR 0001.

### Fixed
- Date parsing with `dayfirst=True` misread ISO dates (2024-03-01 → 3 January). ISO and day-first strings are now parsed separately; regression test added.
