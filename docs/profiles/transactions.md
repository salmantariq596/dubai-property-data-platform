# Profile: DLD sales transactions (Dubai Real Estate Data on Kaggle, snapshot 2026-11-02)

Generated 2026-11-22 19:45 UTC by `dld-profile`.

## Summary

| Metric | Value |
|---|---|
| Source file | `dld_sales_transactions.csv` |
| Rows | 1,382,848 |
| Columns | 20 |
| Exact duplicate rows | 0 |
| Rows sharing a `transaction_id` | 0 |

## Columns

| Column | Inferred type | Null % | Distinct | Range | Top values |
|---|---|---:|---:|---|---|
| `transaction_id` | text | 0.0 | 1,382,848 |  | 1-11-1975-2 (1), 1-11-1990-1 (1), 1-11-1992-1 (1) |
| `instance_date` | date | 0.0 | 6,167 | 1975-01-01 – 2026-11-02 | 2023-09-11 (2,528), 2010-03-03 (2,227), 2025-12-29 (1,858) |
| `reg_type_label` | text | 0.0 | 2 |  | Ready (736,939), Off-Plan (645,909) |
| `area_id` | numeric | 0.0 | 244 | 229 – 531 | 330 (105,711), 441 (95,166), 526 (81,858) |
| `area_name_en` | text | 0.0 | 244 |  | Marsa Dubai (105,711), Al Barsha South Fourth (95,166), Business Bay (81,858) |
| `project_name` | text | 20.6 | 3,427 |  | REMRAAM (8,283), SKY COURTS (7,452), INTERNATIONAL CITY EMARATI (4,564) |
| `master_project_name` | text | 18.3 | 185 |  | Jumeirah Village Circle (88,279), Business Bay (83,257), Dubai Marina (71,451) |
| `building_name` | text | 29.6 | 4,701 |  | Seven City JLT (3,522), PRINCESS TOWER (2,404), Burj Khalifa (2,335) |
| `property_type_label` | text | 0.0 | 4 |  | Unit (1,066,079), Villa (179,189), Land (113,807) |
| `property_sub_type` | text | 14.3 | 35 |  | Flat (954,688), Villa (110,864), Office (57,386) |
| `usage_label` | text | 0.0 | 3 |  | Residential (1,190,250), Commercial (132,317), Other (60,281) |
| `rooms_bucket` | numeric | 20.3 | 6 | 0 – 5 | 1 (413,725), 2 (262,904), 0 (239,132) |
| `has_parking` | text | 3.2 | 2 |  | true (973,319), false (365,587) |
| `area_sqm` | numeric | 0.0 | 82,629 | 0.01 – 342,103,430.80 | 45.0 (9,817), 144.0 (9,429), 1393.55 (4,606) |
| `price_aed` | numeric | 0.0 | 317,557 | 1 – 3,869,204,024 | 1000000 (6,974), 2000000 (6,770), 1200000 (6,160) |
| `price_psf` | numeric | 0.0 | 301,822 | 0 – 1,207,963.20 | 1000.0 (1,335), 1.0 (1,122), 500.0 (1,104) |
| `procedure_id` | numeric | 1.9 | 18 | 4 – 852 | 102 (635,947), 11 (491,089), 41 (156,483) |
| `procedure_name_en` | text | 1.9 | 18 |  | Sell - Pre registration (635,947), Sell (491,089), Delayed Sell (156,483) |
| `is_pre_registration` | text | 1.9 | 2 |  | false (709,968), true (645,909) |
| `quality_flags` | numeric | 0.0 | 9 | 0 – 16 | 0 (1,354,155), 4 (27,356), 2 (737) |

## Flags for the data dictionary

- `project_name` is 20.6% null.
- `building_name` is 29.6% null.
- `rooms_bucket` is 20.3% null.
- `rooms_bucket` has zero or negative values (min 0).
- `price_psf` has zero or negative values (min 0).
- `quality_flags` has zero or negative values (min 0).
