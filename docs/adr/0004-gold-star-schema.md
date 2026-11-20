# ADR 0004: Gold star schema with hash surrogate keys, SCD Type 2 areas and full rebuilds

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

Power BI needs a model analysts can use without knowing how DLD files look: facts at a clear grain, conformed dimensions shared by sales and rents, and medians and yields that are expensive to compute in DAX. Area names change over time (DLD renames and corrects areas), and reports must show both the name an area had when a sale happened and its name today.

## Decisions

### 1. Two facts at the finest useful grain

`fact_sales` holds one registered sale, `fact_rents` one Ejari contract. Mortgages and gifts are not market prices and are excluded from `fact_sales`; they stay in silver. Each fact keeps its business key as a degenerate dimension, so any figure can be traced back to a DLD record.

### 2. Surrogate keys from a hash of the natural key

Keys are `xxhash64` of the natural key, not sequence numbers. *Alternatives considered:*

- **Identity columns or `row_number()`:** depend on run order. Rebuilding a dimension re-numbers it, so every fact must be re-keyed with it.
- **Natural keys in the facts:** strings are larger and slower to join, and SCD Type 2 needs one key per *version*, which a natural key cannot give.

Hash keys are deterministic: rebuilding gives the same keys, so dimensions and facts can be rebuilt independently. The collision risk for a few thousand rows in a 64-bit space is negligible. Two reserved keys sit beside them: `-1` Unknown in every dimension, so facts never fall out of inner joins, and `0` All types for the aggregate.

### 3. dim_area as SCD Type 2, with the first version open to the past

Only `dim_area` tracks history: the plan names areas as the attribute that changes, and Power BI must show both the historical and the current name. A version's key is the hash of the area key and its start date.

The first version of an area is valid from 1900-01-01, not from the day the warehouse first saw it. Facts are dated by their registration date, which is usually years before the load date. A first version starting on the load date would leave all historical sales with no area.

The SCD logic computes the whole new dimension from the current one plus today's `silver.areas` snapshot, and the table is overwritten. The dimension is small (hundreds of rows), so this is cheap. It is also testable without Delta, and idempotent: the same snapshot twice changes nothing. A second change on the same day updates that day's version instead of creating a one-day version. *Alternative considered:* a Delta `MERGE` with the "staged updates" union. That suits large dimensions but needs more code and Delta in every test.

*Type 1 vs Type 2:* `dim_property_type` and `dim_project` are Type 1. They are rebuilt from silver and show their latest attributes, because nobody reports by "the project name at the time of sale".

### 4. Facts and aggregates are rebuilt from silver on every run

Silver already holds one clean row per key, so gold rebuilds facts and aggregates in full. A rebuild of a few million rows takes minutes on a small cluster, needs no watermark, and is always consistent with silver. *Alternative considered:* incremental MERGE into the facts. That is worth it at much larger volumes or with strict latency targets, neither of which applies here, and it is noted as a next step if run times grow.

### 5. A monthly aggregate for medians and yield

Medians cannot be summed or averaged across groups, so DAX would have to scan the facts for every visual. `agg_area_monthly` precomputes, per area, property type and month:

- median price per sqft, with outliers excluded;
- median sale price and median annual rent, with multi-unit contracts excluded;
- counts, and year-on-year change;
- gross yield.

An "All types" row per area and month is computed from the facts, not from the per-type rows. Gross yield is published only when both medians rest on at least 10 records (`MIN_RECORDS_FOR_YIELD`). Below that, one unusual contract moves the yield by whole percentage points.

### 6. Liquid clustering by date, then OPTIMIZE

Dashboard queries filter by date range first. On Databricks the facts are created with `CLUSTER BY (date_key)` and compacted with `OPTIMIZE`. Liquid clustering replaces fixed partitions: with a few million rows, partitioning by month would create many small files, and clustering keeps file sizes healthy without choosing a partition column up front. Local tests use plain Delta tables (`cluster_facts = False`), because open-source Delta support for clustering varies by version. `OPTIMIZE` itself is tested in CI. Timings before and after are recorded in `docs/performance.md` from a real Databricks run.

### 7. Sales and rents share property types through a mapping

Ejari and sales name property types differently. `PROPERTY_TYPE_ALIASES` and `PROPERTY_SUB_TYPE_ALIASES` map rent-side names to sales names (for example *Apartment* to *Flat*), so both facts share `dim_property_type` rows and yield compares like with like. The mappings must be completed after profiling real files.

## Consequences

- Power BI gets a clean star: one relationship per dimension, no unmatched keys, and medians ready to use.
- `dim_area` is the only stateful gold table. Losing it loses rename history, so it must never be rebuilt from scratch by hand.
- The yield threshold and type mappings are explicit constants that can be tuned once real data shows how many records each area has.
- Cross-checking monthly medians against the official DLD price index (plan, Phase 3 step 6) needs the index download and is open until then.
