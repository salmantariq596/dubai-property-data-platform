# ADR 0005: Data quality checks between layers, recorded in dq_results, failing before the next layer

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

Silver already rejects bad *rows* (ADR 0003). That does not catch problems that only show at table or load level. A file can be mostly unusable while every rejected row is handled correctly. A load can arrive half the usual size. A code change can make gold lose or duplicate rows. The dashboard is where investors would notice, which is too late. The plan asks for checks between layers, a per-rule decision to warn, quarantine or fail, and a `dq_results` table.

## Decisions

### 1. Three responses, each where it fits

| Response | When | Why |
|---|---|---|
| Quarantine the row | One record is broken (bad cast, missing key, impossible contract) | One bad row should not block a million good ones; the row is kept for inspection (silver, ADR 0003) |
| Warn | The data is usable but something changed or looks odd | Worth a look, not worth stopping the dashboard: a few outliers, a smaller load, a new area name |
| Fail the run | The next layer would be wrong | Broken keys, lost or duplicated rows, or a load that is mostly rejected would put wrong numbers in front of users |

### 2. Thresholds on the failed share, not on a yes/no

Each check counts rows checked and rows failed, and has a WARN and a FAIL threshold on the failed percentage. Some rules are binary by nature: keys, reconciliation and dimension integrity FAIL on a single row. Some are tolerant by design:

- **Zero sizes and values:** silver keeps them as flagged outliers (ADR 0003). The check warns on any and fails only above 20%, the point where a load is no longer a normal file with some odd rows.
- **Quarantine rate and unmapped areas:** warn above 1%, fail above 20%, for the same reason.
- **Dates outside dim_date** (2000–2030): warn on any, fail above 5%. A few old registrations are plausible; many mean a date-format problem.

The thresholds sit in code next to the SQL, are tested, and are written to every `dq_results` row. A result can therefore always be judged against the rule that was in force.

### 3. Row-count change warns; it does not fail

The plan's "within 30% of the previous load" is a WARN. DLD files are downloaded by hand for now, so their coverage varies: a month, a quarter, a year. A size change is often legitimate. When a smaller file *is* broken, its rows fail other checks (quarantine rate) and the run stops there. Once Data Factory delivers same-sized daily files (Module 8), this can be tightened to FAIL.

### 4. Checks are SQL, kept as data

Every check is one SQL query returning `rows_checked` and `rows_failed`, with table names as placeholders. The rule, its query and its thresholds read together in one place (`quality/checks.py`) and need no Spark to review. The unit tests render every query against real table names. The Spark tests run each check against small tables built to break it. *Alternatives considered:*

- **Great Expectations:** powerful, but a heavy dependency and configuration layer for about 25 rules. Its suites are harder to read in a code review than SQL.
- **Databricks DLT expectations:** they tie the pipeline to Delta Live Tables, which ADR 0001 avoids so the same code runs locally, in CI and on Azure Databricks.

### 5. Results are written before anything fails

`run_quality` records every result, PASS, WARN, FAIL and SKIPPED alike, and only then raises `DataQualityError`. A failed run always leaves its evidence: which rule, which table, how many rows, which load. A check whose tables do not exist yet (rents not loaded) is SKIPPED, not failed, so the pipeline can start with one dataset.

### 6. One orchestrated run, failing before the next layer

`orchestrate.run_pipeline` runs bronze → checks → silver → checks → gold → checks. A FAIL after silver means gold is not rebuilt: the dashboard keeps showing the last good data instead of wrong data. Because every step is idempotent (ADRs 0002–0004), recovery is to fix the cause and run the whole pipeline again. Data Factory (Module 8) calls one notebook (`06_run_pipeline`) and gets a failed activity, which triggers its alert.

### 7. Integration tests on fixtures that break things on purpose

The synthetic transactions and rents fixtures plant one example of each problem. The integration tests run the full flow on them and assert the exact outcome: rows per layer, statuses per stage, the aggregate. They also land a mostly broken second file and assert that the run stops at silver with gold untouched. This is the plan's checkpoint, "one bad-data case and how the pipeline handles it", as an executable test.

## Consequences

- Bad loads are caught between layers, recorded, and kept away from the dashboard; good data is never blocked by a few bad rows.
- `dq_results` gives a history per rule, which Module 9's monitoring reads (rising `failed_pct` on a WARN).
- Thresholds were chosen without real data. Expect to tune them after the first real loads, and record each change in the CHANGELOG.
- The SQL checks depend on table and column names. A schema change that breaks a check shows up as a test failure, not in production.
