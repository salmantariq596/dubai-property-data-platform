# ADR 0003: Silver cleaning with explicit casts, quarantine, and MERGE on the business key

- **Status:** Accepted
- **Date:** 2026-11-19

## Context

Bronze holds every DLD row exactly as delivered, as strings. Silver must turn those rows into typed, trustworthy records that gold can model without re-checking them. That means one row per transaction or contract, consistent area names, and measures in the units the dashboard uses. The rules must survive real files that have not been profiled yet: the data dictionary is still the *expected* schema.

## Decisions

### 1. Rules are data, kept apart from Spark

Every column, its type and whether it is required lives in `silver/rules.py`, together with the date and number patterns, unit constants, outlier thresholds and area aliases. That module imports no Spark, so the rules can be reviewed in one place and unit-tested on any laptop. `silver/transform.py` turns them into Spark expressions. The regexes are shared by both, so the tests check the patterns Spark actually runs.

### 2. Explicit formats, and casts that never crash the job

Each value is trimmed and cast with `try_cast`, so a bad value becomes null rather than failing the job. The rule checks then compare the raw text with the typed result: text present, value null means the cast failed.

- **Dates:** only ISO (`2024-01-05`) and day-first (`05-01-2024`, `5/1/2024`, `05.01.2024`) are accepted. The parts are extracted with a regex and rebuilt as ISO before casting to `DATE`, which rejects impossible dates such as 31 February. Month-first dates are never guessed. Using regex + `CAST` instead of `to_date` patterns also avoids Spark's parser-version differences (`timeParserPolicy`).
- **Numbers:** thousands separators and spaces are removed (`"920,000"` → `920000.00`), then cast to `DECIMAL(18,2)`. Integers must be whole: `"3.0"` passes, `"3.5"` fails.
- **Missing columns** arrive as nulls, so the silver schema is fixed whatever a file contains.

### 3. Quarantine instead of drop

A row that breaks any rule goes to `silver.quarantine` with:

- `failed_rules`: every rule it broke, for example `["invalid_instance_date"]`;
- `raw_record`: the row as delivered (JSON);
- its load id, source file and ingestion time.

Rules: a required field is missing; a value cannot be cast; Auto Loader rescued data from the row; and for rents, the end date is before the start or no rent amount is given. Rejected rows are counted per load in `silver.processed_loads`, so a sudden rise is visible. *Alternative considered:* failing the whole run on a bad row. One bad row in a million would then block every report, so per-row quarantine is used, and a threshold check on quarantine volume belongs to the data quality module (Module 5).

### 4. Outliers are flagged, not removed

Zero sizes, zero values and prices per sqft outside 100–20,000 AED are real registrations, often data-entry errors but sometimes genuine (land, bulk deals). They stay in silver with `is_outlier` and `outlier_reason`, and gold excludes them from medians. Deleting them would hide the problem and change totals.

### 5. One row per business key; the latest ingested version wins

Within a batch, rows are ranked per key by `_ingested_at`, then load id, file and raw content, so the choice is deterministic. Across batches, Delta `MERGE` on the key updates a row only when the incoming `_ingested_at` is the same or newer. Replaying an old load can therefore never overwrite newer data.

### 6. Incremental loads by load id, idempotent by design

Silver processes bronze `_load_id`s not yet in `silver.processed_loads`. Every write is repeatable:

- the silver tables use MERGE on the business key;
- quarantine MERGEs on a hash of the raw row, load and file;
- `silver.areas` is rebuilt in full;
- the processed-load record is written last.

If a run dies part-way, the next run picks the same loads up again and the repeat changes nothing. *Alternative considered:* streaming from the bronze Delta tables with a checkpoint. That is neat on Databricks but cannot be tested without Delta, and a load-id watermark is easier to inspect and to reset ("reprocess load X" is one `DELETE`).

### 7. Area names: one key per area, English first

`area_key` is the normalised English name (`"business bay"`). Rows with no English name are matched through their Arabic name, which is folded for harmless spelling differences (diacritics, tatweel, alef forms, final yaa, taa marbuta). The pairing is learned from `silver.areas` and from rows in the same batch that carry both names. Known abbreviations are mapped through `AREA_ALIASES`. The names as delivered are kept in `area_name_*_source`, and `silver.areas` lists every variant seen.

The key is the English name rather than DLD's `area_id` because the expected `area_id` has not yet been confirmed in real files, nor shown to be consistent between sales and rents. If profiling confirms it is, the key can switch to `area_id` inside `resolve_areas` without changing silver's schema.

### 8. Units and currency in silver

Sizes stay in m² as delivered and are added in sqft (`× 10.7639`). Values gain `value_usd` at the fixed peg of 3.6725 AED per USD. Rents gain `annual_rent_aed`: `annual_amount` when given, otherwise `contract_amount` scaled to 365 days.

## Consequences

- The dashboard's numbers trace back to explicit, tested rules, and every rejected row can be inspected with the rule it broke.
- The transformations are tested without tables; MERGE behaviour (updates, replays, re-runs) is tested in CI with Delta.
- Rules written against the expected schema must be checked against the first real profile: column names, date layout, and how often area names are missing. Expect `rules.py` and `AREA_ALIASES` to change then.
- Silver needs Delta for MERGE. Local runs without Delta can test transformations only.
