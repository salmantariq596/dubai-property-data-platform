# Test fixtures

`sample_transactions.csv` and `sample_rents.csv` are **synthetic**. It follows the shape of the DLD transactions export but every row is invented (IDs start with `SYN-`). It is small on purpose and contains one example of each problem the pipeline must handle:

| Row | Planted issue |
|---|---|
| SYN-0002 | Thousands separator in `actual_worth` (`"920,000"`) |
| SYN-0003 | Same transaction appears twice (exact duplicate) |
| SYN-0006 | Missing English area name, Arabic name present |
| SYN-0007 | Impossible date (`31-02-2024`) |
| SYN-0008 | Non-numeric value (`n/a`) and empty `meter_sale_price` |
| SYN-0010 | Zero size and zero price |
| SYN-0011 | Trailing space in area name (`"Business Bay "`) |

### `sample_rents.csv`

| Row | Planted issue |
|---|---|
| SYN-R-0002 | Thousands separators in both amounts (`"120,000"`) |
| SYN-R-0003 | Same contract appears twice; Ejari sub-type `Apartment` (mapped to `Flat`) |
| SYN-R-0004 | Annual amount missing: annualised from the contract amount |
| SYN-R-0005 | Contract ends before it starts |
| SYN-R-0006 | Neither annual nor contract amount |
| SYN-R-0007 | Multi-unit contract (12 units), left out of rent medians |
| SYN-R-0008 | Missing English area name, Arabic name present |

Expected silver result: 8 contracts, 2 quarantined (SYN-R-0005, SYN-R-0006), 1 duplicate removed.

Real DLD data is never committed to the repo.
