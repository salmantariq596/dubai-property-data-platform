# Data sources

All data comes from official Dubai Land Department (DLD) open data. No listing sites are scraped: listing prices are asking prices, while DLD records are registered transaction prices.

## Datasets

| Dataset | Business key | Role in the platform | Primary source | Fallback |
|---|---|---|---|---|
| Transactions (sales, mortgages, gifts) | `transaction_id` | `fact_sales` | DLD open data portal — data.dubailand.gov.ae | Dubai Pulse `dld_transactions-open` |
| Rents (Ejari contracts) | `contract_id` | `fact_rents`, rental yield | DLD open data portal | Dubai Pulse |
| Units, buildings, projects | per table | Dimensions | DLD open data portal | Dubai Pulse |
| Developers, brokers | per table | Dimensions | DLD open data portal | Dubai Pulse |
| Residential Sale Price Index | month | Validate own price trends | Dubai Pulse `dld_residential_sale_index-open` | — |

## Status of each source

**Dubai Pulse is not reachable from outside the UAE.** In November 2026 its pages timed out from Pakistan and from a Databricks Free Edition workspace (hosted outside the UAE), while dubailand.gov.ae and other sites answered from both. The DLD website's open-data page hands its downloads off to Dubai Pulse. In production, Data Factory runs in Azure's UAE North region, inside the UAE.

| Dataset | Source checked | Date checked | Years available | Format | File size | Rows | Update frequency | Notes |
|---|---|---|---|---|---|---|---|---|
| Transactions | Dubai Pulse | 2026-11-22 | — | — | — | — | — | Timed out from outside the UAE |
| Transactions (sales) | Kaggle: Dubai Real Estate Data | 2026-11-23 | 1975–2 Nov 2026 (thin before 2004) | CSV + Parquet | 241 MB CSV | 1,382,848 | Monthly | Sales only; publisher's column names (see below) |
| Rents | | | | | | | | |
| Projects | | | | | | | | |
| Price index | | | | | | | | |

### Redistributed DLD data: Dubai Real Estate Data (Kaggle)

"Dubai Real Estate Sales Transactions 2004-2026" on Kaggle, published by Dubai Real Estate Data ([dubairealestatedata.com](https://www.dubairealestatedata.com)), an independent site built on DLD open data. It is DLD's registered sales, pulled from Dubai Pulse and the DLD gateway, one row per transaction, with nothing deleted: rows failing the publisher's checks carry a `quality_flags` bitmask instead.

- **License: CC BY 4.0.** Every published figure, screenshot or report built on it carries this credit: *"Dubai Real Estate Data (dubairealestatedata.com), based on Dubai Land Department open data."*
- **Sales only.** Mortgages and gifts are excluded, and there is no `trans_group_en` column. Silver labels every row of a file without that column `Sales` (`MISSING_COLUMN_DEFAULTS` in `silver/rules.py`).
- **Its own column names.** `price_aed`, `area_sqm`, `reg_type_label`, `property_type_label`, `rooms_bucket` and others are read as their DLD equivalents through `SOURCE_ALIASES` in `silver/rules.py`. Bronze stores the file exactly as delivered.
- **Area names are DLD's cadastral names** (for example "Al Barsha South Fourth", not "Jumeirah Village Circle"), the same as a direct DLD export.
- **Only the CSV is landed.** The download also contains Parquet, a data dictionary, a manifest with SHA-256 checksums and the license; keep them with the download, not in the landing folder.

## Download log

Record every file you land in `data/raw/`, so any load can be reproduced.

| Load date | Dataset | File name | Source URL | Rows | Notes |
|---|---|---|---|---|---|
| 2026-11-23 | transactions | `dld_sales_transactions.csv` | Kaggle: "Dubai Real Estate Sales Transactions 2004-2026" (Dubai Real Estate Data) | 1,382,848 | Snapshot generated 2026-11-02; SHA-256 `1fa1d38d…b1b9eb` (`export-manifest.json`) |

## Known caveats

- **Download paths and formats change.** Note the exact URL and date of every download.
- **Bilingual columns.** Arabic and English names sit side by side. Read files as UTF-8 (`utf-8-sig` strips a byte-order mark) and keep both name columns.
- **Area names are inconsistent** across datasets (spelling, spacing, Arabic vs English). Silver will use a mapping table, `silver.areas`.
- **Units.** DLD records sizes in square metres (`procedure_area`, `meter_sale_price`). The dashboard reports price per square foot; 1 m² = 10.7639 ft².
- **Currency.** Values are in AED. A USD column uses the fixed peg of 3.6725 AED per USD.
- **Late registrations.** Off-plan sales can be registered weeks after month-end, so the latest month is preliminary and volumes revise upward.
- **Licence.** Check and record the terms of use shown on the portal before publishing any derived figures.
