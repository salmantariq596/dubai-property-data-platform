# Design questions, answered

Short answers to the questions a reviewer is most likely to ask, with links to the full reasoning. Each answer says what was chosen and what it costs.

## Ingestion

**Why does Data Factory only copy files, instead of loading tables directly?**
Data Factory is good at moving files on a schedule, with retries and managed identity. It is weak at tracking which files are already loaded, evolving a schema and making writes replay-safe. Auto Loader does all three inside the lakehouse. The cost is two tools instead of one. [ADR 0002](adr/0002-bronze-ingestion-design.md), [ADR 0008](adr/0008-orchestration.md)

**What happens if the same file arrives twice, or a run crashes halfway?**
Auto Loader's checkpoint skips files it has already loaded. Each micro-batch is written with a transaction id (`txnAppId` + batch id), so a batch replayed after a crash is ignored by Delta. Silver then MERGEs on the business key, so even a re-sent file with a new name changes nothing. [ADR 0002](adr/0002-bronze-ingestion-design.md), [bronze runbook](runbook_bronze.md)

**Why keep every bronze column as a string?**
Bronze is an exact record of what arrived. If a price comes in as `"1,200,000"` or `"n/a"`, a typed bronze table would either reject the file or silently null the value. As strings, nothing is lost, and the cast happens in silver where a failure is quarantined with its raw value. [ADR 0002](adr/0002-bronze-ingestion-design.md)

## Cleaning and quality

**Why quarantine bad rows instead of failing the run?**
One malformed row should not hold back a million good ones. A load that is mostly bad should, so the quarantine rate itself is a check with WARN and FAIL thresholds. [ADR 0003](adr/0003-silver-cleaning-design.md), [ADR 0005](adr/0005-data-quality.md)

**How do you stop older data from overwriting newer data?**
Silver MERGEs on the business key and only updates when the incoming row was ingested later than the stored one. A backfill of last year cannot overwrite a correction loaded yesterday. [ADR 0003](adr/0003-silver-cleaning-design.md)

**Why a home-grown quality framework instead of a library?**
25 checks written as SQL in one file are easy to read and review, have no dependency to upgrade, and record every result in `quality.dq_results`, where the dashboard and the health check can query them. The trade-off is no ready-made profiling UI. [ADR 0005](adr/0005-data-quality.md), [data quality](data_quality.md)

**What does a failed check do?**
It raises before the next layer runs. Gold is not rebuilt, Power BI keeps showing the last good data, the job run fails, Databricks sends an email, and Data Factory posts to a Teams or Slack webhook if one is set. [runbook](runbook.md)

## Modelling

**Why a star schema and not one wide table?**
Power BI's engine is built for stars: small dimensions filter large facts, and measures stay simple. Sales and rents share the same area, date and property-type dimensions, so one slicer filters both. [ADR 0004](adr/0004-gold-star-schema.md), [gold model](gold_model.md)

**Why hash surrogate keys instead of identity columns?**
A hash of the business key gives the same key every time. Gold can be rebuilt from silver without re-keying the facts, and two runs produce identical tables. The cost is a 64-bit hash's chance of collision, which is negligible for dimensions of a few thousand rows. [ADR 0004](adr/0004-gold-star-schema.md)

**Why SCD Type 2 only for areas?**
Areas are renamed and merged, and a report on 2021 should show 2021's names. Property types and projects change rarely and nobody needs their history, so they stay Type 1. [ADR 0004](adr/0004-gold-star-schema.md)

**Why is there an aggregate table if Power BI can aggregate?**
Medians and rental yields do not add up across months or areas, so DAX cannot build them from monthly totals. `agg_area_monthly` computes them once in Spark, with an "All types" row so the median across all types is a real median, not an average of medians. [gold model](gold_model.md)

**Why rebuild gold fully instead of merging into it?**
At this volume a full rebuild takes minutes, and it is always correct: no late-arriving-dimension edge cases. Incremental MERGE into the facts is the next step once volume or refresh frequency makes it worth the complexity. [ADR 0004](adr/0004-gold-star-schema.md)

## Platform

**Why one Databricks job instead of one Data Factory activity per layer?**
Each layer as its own activity would start a cluster per layer. One job with chained tasks starts one cluster, shows the whole run in one place, and can be re-run from the failed task. [ADR 0008](adr/0008-orchestration.md), [orchestration](orchestration.md)

**How do you load three years of history?**
The backfill pipeline lands every day's files first, then runs the job once. Because loads are incremental, three years is one run, not about 1,000. [ADR 0008](adr/0008-orchestration.md)

**Where are the secrets?**
There are none. Data Factory and Databricks reach the lake through managed identities, Data Factory starts the job through its identity, and GitHub Actions deploys through OIDC federation. Nothing has to be stored, rotated or leaked. [ADR 0007](adr/0007-azure-infrastructure.md), [ADR 0009](adr/0009-cicd-and-monitoring.md)

**How would you know if the pipeline silently stopped running?**
A failure alert only fires when something runs. A separate daily health check reads the pipeline's own run log and reports STALE when the last complete run is more than 26 hours old, and FAILED or WARN from the latest run. [ADR 0009](adr/0009-cicd-and-monitoring.md), [runbook](runbook.md)

**Why five Terraform stacks instead of one?**
Each stack has a different lifetime and blast radius: the state storage and budget outlive everything, the workspace must exist before Unity Catalog can be configured, and the CI identity should be changeable without touching the lake. [ADR 0007](adr/0007-azure-infrastructure.md), [infra](../infra/README.md)

**What does it cost, and how is that controlled?**
A budget alert is created first. One small single-node cluster serves all steps, schedules deploy switched off, every job has a timeout and runs one at a time, and the environment is destroyed when idle. Measured numbers: [cost](cost.md).

## Testing

**How is Spark code tested without a cluster?**
Business logic lives in `src/`, as plain functions over DataFrames, with thin notebooks that call it. Unit tests cover the logic without Spark; Spark and integration tests run the same code on local Spark with Delta in CI, which refuses to run without Delta. [ADR 0001](adr/0001-local-first-development.md)

**How do you know a gold change will not break the dashboard?**
The Power BI model is stored as TMDL text. A test compares its tables, columns and types with the gold schema, as Power BI will see them, so a renamed or retyped column fails CI before it reaches the report. [ADR 0006](adr/0006-power-bi-model.md)
