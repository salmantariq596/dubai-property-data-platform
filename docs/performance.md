# Performance: gold facts before and after OPTIMIZE

> **Status:** to be filled from the first full run on Databricks. Numbers below are placeholders until then; do not quote them.

## How to measure

1. Load at least one year of transactions through bronze and silver.
2. Run `notebooks/04_gold_star_schema`. Its last cell times a typical dashboard query, then runs `OPTIMIZE` on the facts and times it again.
3. Copy the printed table here, with the run details.

The benchmark query: median price per sqft by area and month for the latest year, joining `fact_sales` to `dim_area` and `dim_date`, outliers excluded. It is the query behind the dashboard's main page.

## Results

| Measure | Before OPTIMIZE | After OPTIMIZE |
|---|---:|---:|
| `fact_sales` files | _ | _ |
| Benchmark query (best of 3) | _ s | _ s |

| Run detail | Value |
|---|---|
| Date | _ |
| Compute | _ (for example Databricks Free Edition serverless) |
| `fact_sales` rows | _ |
| Data range | _ |

## Notes

- Liquid clustering (`CLUSTER BY (date_key)`) is applied when the facts are created on Databricks. `OPTIMIZE` both compacts small files and clusters the data.
- Small data sets may show little difference: the gain grows with the number of files and the share of data a date filter can skip.
- Record the cost of a full daily run here too, once it runs on Azure (Module 9).
