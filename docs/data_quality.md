# Data quality

Checks run between the layers and are recorded in `quality.dq_results`. A `FAIL` stops the pipeline before the next layer, so bad data never reaches the dashboard. Design and the reasoning behind each decision: [ADR 0005](adr/0005-data-quality.md).

```
bronze → [bronze checks] → silver → [silver checks] → gold → [gold checks]
                                          │ FAIL
                                          └─ gold is not rebuilt; the dashboard keeps the last good data
```

## Three ways to handle bad data

| Response | Used for | Where |
|---|---|---|
| **Quarantine the row** | One bad record: a value that cannot be cast, a missing key, a contract ending before it starts | Silver, row by row (`silver.quarantine`, Module 3) |
| **Warn** | Something to look at that does not make the data wrong: a few outliers, a load smaller than usual, an unmapped area | Checks below, `WARN` |
| **Fail the run** | The data in the next layer would be wrong: broken keys, lost rows, or a load that is mostly bad | Checks below, `FAIL` |

Each check counts rows checked and rows failed. A check WARNs or FAILs when the failed share is above its threshold. "Any failure" means a single failed row is enough.

## The checks

### Bronze (4 checks)

| Rule | What it checks | WARN | FAIL |
|---|---|---|---|
| `bronze_transactions_row_count_change` | Latest transactions load is within 30% of the previous load's row count | any failure | never |
| `bronze_transactions_last_load_ok` | Latest transactions load finished without FAILED or MISMATCH files | never | any failure |
| `bronze_rents_row_count_change` | Latest rents load is within 30% of the previous load's row count | any failure | never |
| `bronze_rents_last_load_ok` | Latest rents load finished without FAILED or MISMATCH files | never | any failure |

### Silver (13 checks)

| Rule | What it checks | WARN | FAIL |
|---|---|---|---|
| `silver_transactions_key_not_null` | Every transactions row has a transaction_id | never | any failure |
| `silver_transactions_key_unique` | transaction_id is unique in silver | never | any failure |
| `silver_transactions_area_mapped` | Transactions rows are mapped to an area | over 1% | over 20% |
| `silver_transactions_quarantine_rate` | Share of rows quarantined in the latest transactions silver run | over 1% | over 20% |
| `silver_transactions_date_in_calendar` | Transaction dates fall inside dim_date (2000-2030) | any failure | over 5% |
| `silver_transactions_date_not_future` | No transaction dated more than a month ahead | any failure | never |
| `silver_transactions_positive_size` | Sizes are positive (zero sizes are flagged outliers, kept in silver) | any failure | over 20% |
| `silver_transactions_positive_value` | Values are positive (zero values are flagged outliers, kept in silver) | any failure | over 20% |
| `silver_rents_key_not_null` | Every rents row has a contract_id | never | any failure |
| `silver_rents_key_unique` | contract_id is unique in silver | never | any failure |
| `silver_rents_area_mapped` | Rents rows are mapped to an area | over 1% | over 20% |
| `silver_rents_quarantine_rate` | Share of rows quarantined in the latest rents silver run | over 1% | over 20% |
| `silver_rents_positive_rent` | Every contract has a positive annual rent | any failure | over 20% |

### Gold (8 checks)

| Rule | What it checks | WARN | FAIL |
|---|---|---|---|
| `gold_fact_sales_reconciles` | fact_sales has exactly one row per silver sale | never | any failure |
| `gold_fact_rents_reconciles` | fact_rents has exactly one row per silver contract | never | any failure |
| `gold_fact_sales_key_unique` | transaction_id is unique in fact_sales | never | any failure |
| `gold_fact_sales_keys_resolve` | Every fact_sales key finds its dimension row | never | any failure |
| `gold_fact_rents_keys_resolve` | Every fact_rents key finds its dimension row | never | any failure |
| `gold_dim_area_one_current_version` | Every area has exactly one current version | never | any failure |
| `gold_fact_sales_known_area` | Sales point at a real area, not the Unknown row | over 5% | never |
| `gold_agg_yield_plausible` | Published gross yields lie between 1% and 20% | any failure | never |

The table above is generated from `src/dubai_property/quality/checks.py`; the code is the source of truth.

## dq_results

One row per check per run.

| Column | Meaning |
|---|---|
| `run_id` | One data quality run (one stage) |
| `load_id` | The pipeline run that produced the data (Data Factory passes its run id) |
| `stage` | `bronze`, `silver` or `gold` |
| `rule`, `table_name`, `description` | Which check, on which table |
| `rows_checked`, `rows_failed`, `failed_pct` | The measurement |
| `warn_above`, `fail_above` | Thresholds in force when it ran |
| `status` | `PASS`, `WARN`, `FAIL`, or `SKIPPED` when a table is not built yet |
| `checked_at` | When it ran (UTC) |

```sql
-- What needs attention
SELECT checked_at, stage, rule, status, rows_failed, failed_pct
FROM dubai.quality.dq_results
WHERE status IN ('WARN', 'FAIL')
ORDER BY checked_at DESC;
```

## When a check fails

| Rule fails | Likely cause | What to do |
|---|---|---|
| `*_quarantine_rate` | The new file has a different layout or date format, or is corrupt | Look at `silver.quarantine` for the latest load (`failed_rules`, `raw_record`). If the format changed, update `silver/rules.py`, then reprocess the load (silver runbook). If the file is bad, ask for a corrected one |
| `*_key_not_null`, `*_key_unique` | A bug in silver: keys are guaranteed there | Stop and investigate the code; do not publish gold |
| `*_last_load_ok` | Bronze could not write or reconcile a file | See the bronze runbook |
| `gold_*_reconciles`, `gold_*_keys_resolve` | Gold lost or duplicated rows, or a dimension is missing keys | Re-run gold; if it persists, investigate the code |
| `gold_dim_area_one_current_version` | `dim_area` was edited by hand | Restore it with Delta time travel (`RESTORE TABLE ... TO VERSION AS OF ...`) |
| `*_positive_*`, `*_area_mapped`, `*_date_in_calendar` over the FAIL line | Most of a load is unusable | Same as a high quarantine rate: inspect the file first |

After the cause is fixed, run the whole pipeline again (`notebooks/06_run_pipeline`): every step is safe to repeat.

A `WARN` needs no immediate action. Review warnings weekly: a warning whose `failed_pct` keeps rising is a FAIL in the making.
