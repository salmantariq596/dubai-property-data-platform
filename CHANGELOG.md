# Changelog

Each module is merged with a pull request and tagged.

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
