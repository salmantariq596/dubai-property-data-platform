# Runbook: silver cleaning

## Normal run

1. Run bronze first (`notebooks/02_bronze_ingestion`).
2. Run `notebooks/03_silver_cleaning` with the datasets selected.
3. Check the printed result line for each dataset, then the checks in the notebook: no duplicate keys, quarantine by rule, outliers, unmapped areas.

```sql
SELECT * FROM dubai.silver.processed_loads ORDER BY processed_at DESC LIMIT 20;
```

## Result statuses

| Status | Meaning | Action |
|---|---|---|
| `SUCCESS` | New bronze loads cleaned and merged | Review quarantine counts |
| `NO_NEW_LOADS` | Every bronze load was already processed, or bronze is empty | None; expected on re-runs |

## Reading quarantine

```sql
SELECT dataset, rule, count(*) AS rows
FROM dubai.silver.quarantine LATERAL VIEW explode(failed_rules) r AS rule
GROUP BY ALL ORDER BY rows DESC;
```

| Rule | Cause | Fix |
|---|---|---|
| `missing_<column>` | A required value is empty | Usually a source problem: report it, wait for a corrected file |
| `invalid_<column>` | Value cannot be cast (`"n/a"`, `31-02-2024`, a new date layout) | If a whole column fails, the layout changed: extend the pattern in `silver/rules.py`, then reprocess |
| `rescued_data` | Auto Loader could not fit a value to the schema | Check the source file; see the bronze runbook |
| `end_before_start` | Rent contract ends before it starts | Source error; leave in quarantine |
| `missing_rent_amount` | Neither annual nor contract amount | Source error; leave in quarantine |

A handful of rejected rows per load is normal. A large share means a format change, so stop and look before publishing gold.

## Reprocessing a load

After changing a rule, reprocess the loads it affects. MERGE makes this safe: no duplicates are created, and a newer version of a row is never overwritten by an older one.

```sql
DELETE FROM dubai.silver.processed_loads WHERE load_id = '<load id>';
```

Then run `03_silver_cleaning` again. Rows that now pass are merged into silver. Their old quarantine rows stay as history. To reprocess everything, delete all rows for the dataset.

## Unmapped or split areas

- `is_area_mapped = false`: the row has no English name and its Arabic name has not been seen with one. Add the pairing to `AREA_ALIASES` in `silver/rules.py`, or wait until a later row carries both names, then reprocess.
- One area under two keys (for example an abbreviation): add the variant to `AREA_ALIASES` and reprocess.

`silver.areas` lists every spelling seen per key (`area_name_en_variants`, `area_name_ar_variants`).

## Rules

- **Never edit silver tables by hand.** Change the rule, then reprocess the load.
- **Outliers are not errors.** They stay in silver, flagged; gold excludes them from medians.
