# Runbook: bronze ingestion

## Normal run

1. Upload the new file to `/Volumes/<catalog>/raw/landing/<dataset>/load_date=YYYY-MM-DD/`.
2. Run `notebooks/02_bronze_ingestion` with the dataset selected.
3. Check the newest `load_log` rows: every file should show `SUCCESS` with `rows_read = rows_written`.

```sql
SELECT * FROM dubai.bronze.load_log ORDER BY finished_at DESC LIMIT 20;
```

## Statuses

| Status | Meaning | Action |
|---|---|---|
| `SUCCESS` | File written and read back with the same row count | None |
| `NO_NEW_FILES` | Nothing new in the landing folder | None; expected on re-runs |
| `FAILED` | The run raised an error; see `error_message` | See below |
| `MISMATCH` | Rows read back differ from rows read | Stop and investigate before re-running |

## When a run fails

| Error mentions | Cause | Fix |
|---|---|---|
| `UnknownFieldException` / new columns | DLD added a column; Auto Loader updated the schema | Re-run. The new column appears in bronze. Add it to `docs/data_dictionary.md`. |
| Path does not exist | Volume or folder missing | Run `notebooks/00_setup_unity_catalog`. |
| Daily usage limit (Free Edition) | Compute quota reached | Wait for the reset; nothing is lost and the next run resumes. |
| Anything else | | Fix the cause and re-run. Re-running is always safe: committed batches are skipped. |

## Rules

- **Never edit, rename or replace a file after it is loaded.** Auto Loader ignores changed files by default, so the change would be silently missed. Send corrections as a new file in a new `load_date` folder.
- **Never delete a checkpoint** unless you mean to reload everything. If you do, also bump `checkpoint_generation` (for example `v1` → `v2`) in `BronzeSettings`, and load into new or dropped tables, or the data will be duplicated.

## Proving idempotency

Run the notebook twice on the same files. The second run reports `NO_NEW_FILES`, the row counts per `_load_id` do not change, and the duplicate-load check returns zero rows. Screenshot this for the README.
