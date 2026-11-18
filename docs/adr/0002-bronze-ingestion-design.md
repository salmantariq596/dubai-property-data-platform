# ADR 0002: Bronze ingestion with Auto Loader, idempotent foreachBatch and a load log

- **Status:** Accepted
- **Date:** 2026-11-08

## Context

Bronze must land every DLD file exactly once, keep it exactly as delivered, and show what happened on every run. The pipeline will be re-run: by the daily schedule, by retries after failures, and by hand while developing.

## Decisions

### 1. Auto Loader with `trigger(availableNow=True)`

Auto Loader records every file it has processed in a checkpoint, so a re-run only picks up new files. `availableNow` processes everything new and then stops, which gives a batch job for a daily schedule without an always-on cluster.

*Alternatives considered:*
- **Plain `spark.read` of a folder:** re-reads everything, so we would have to track loaded files ourselves.
- **`COPY INTO`:** also idempotent per file, but SQL-only and weaker on schema evolution; Auto Loader is the more common skill in job ads.
- **ADF copy straight into tables:** moves files well but adds no file tracking or schema handling inside the lakehouse. ADF lands files; Auto Loader ingests them (Module 8).

### 2. Every column is a string

`cloudFiles.inferColumnTypes = false`. Type casting is a business decision made in silver, where failures can be quarantined and counted. If bronze cast types, a value like `"920,000"` or `"00123"` could be changed or lost before anyone saw it.

### 3. Append-only, enforced by Delta

`delta.appendOnly = true` on bronze tables and the load log. Bronze is the replayable record of what DLD delivered; silver and gold can always be rebuilt from it. Corrections arrive as new files in new `load_date` folders, never as edits.

### 4. Idempotent writes inside `foreachBatch`

The checkpoint alone does not cover one failure window: a micro-batch is written, then the job crashes before the checkpoint is committed. The next run replays the same batch. Each write therefore carries `txnAppId` (one per dataset) and `txnVersion` (the batch id), and Delta skips a write it has already committed. The load log uses the same keys.

`txnAppId` includes a **checkpoint generation** (`v1`). If a checkpoint is ever deleted, batch ids restart at 0, and Delta would wrongly skip them as already written. Bumping the generation to `v2` gives the new stream a new identity.

### 5. Read-back reconciliation and a load log

After each write, rows are counted back from the table per file and compared with rows read. Each file gets a `load_log` row: load id, file, load date, batch id, rows read, written and rescued, status, timings. Runs that find nothing log `NO_NEW_FILES`, and failed runs log `FAILED` with the error, so silence is never ambiguous.

A `MISMATCH` fails the run. Investigate before re-running, because the retry will see the already-committed write as a replay and pass.

### 6. Snake_case column names at bronze

DLD headers may contain spaces or characters Delta rejects. Renaming once at bronze gives every later layer stable names. Raw header text can always be recovered from the source file.

### 7. Schema evolution: `addNewColumns`

If DLD adds a column, Auto Loader stops the stream with an error naming the new column and updates the stored schema. The run is logged as `FAILED`; the next run succeeds and the new column appears in bronze. Values that don't fit the schema go to `_rescued_data` instead of being dropped.

## Consequences

- Re-running any load is always safe, which simplifies scheduling and recovery.
- Storage grows with every load; bronze is never compacted by deleting rows.
- **Local tests** use Spark's file source in place of Auto Loader (which exists only on Databricks). Its checkpoint gives the same skip-processed-files behaviour. Unlike Auto Loader, it maps CSV columns by position from the first file, so local tests use one header layout per dataset.
- Local tests without Delta fall back to Parquet; the Delta-only guarantees (replay skipping, append-only) are tested in CI, where `REQUIRE_DELTA=1` makes a missing Delta setup fail the build.
