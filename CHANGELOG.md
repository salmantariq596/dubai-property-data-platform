# Changelog

Each module is merged with a pull request and tagged.

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
