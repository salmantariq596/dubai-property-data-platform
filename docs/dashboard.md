# Dashboard: Dubai property market

Four pages, one message each. Every page answers questions from [business_questions.md](business_questions.md). The report is built: `powerbi/DubaiPropertyMarket.pbip` ([how to open it](../powerbi/README.md)). This page is its specification.

## On every page

- **Dates up to today:** a report filter keeps `dim_date[date]` in the last 27 years up to today, so the Year slicer and every date axis stop at the current year. The date table itself runs to 2030, because rent contracts end in the future.
- **Slicers** (top bar, synced across pages): `dim_date[year]` (between), `dim_area[current_area_name_en]` (dropdown, search on), `dim_property_type[property_type]` (dropdown).
- **Arabic tooltip:** every visual that shows areas has the `Area (Arabic)` measure in its tooltip.
- **Footer text:** "Source: Dubai Land Department open data. Registered transactions; outliers excluded from medians. Latest month is preliminary."
- **Colours:** theme only. Teal (`#0F4C5C`) for the main series, orange (`#E36414`) for comparison (off-plan, last year).

Use area names from `dim_area[current_area_name_en]` in visuals and slicers. Areas keep one name across renames there, while `area_name_en` shows the name at the time of each sale.

## Page 1 — Market overview

**Message:** how active and how expensive the market is, and where it is heading. *(Questions 1, 4, 6)*

| Visual | Fields |
|---|---|
| 4 cards | `Sales`, `Sales Value AED`, `Median Price per Sqft`, `Median Price per Sqft YoY %`; two decimal places (1.38M, not 1M) |
| Line chart: price trend | X `dim_date[month_start]`, Y `Median Price per Sqft` and `Median Price per Sqft LY` |
| Column chart: activity | X `dim_date[month_start]`, Y `Sales`, line `Off-plan Share` (combo chart) |
| Bar chart: fastest risers | Y `dim_area[current_area_name_en]`, X `Median Price per Sqft YoY %`, top 10 by value, visual filter `Priced Sales ≥ 50` |

## Page 2 — Area deep-dive

**Message:** what a square foot costs, by area and property type. *(Questions 2, 7)*

| Visual | Fields |
|---|---|
| Matrix | Rows `dim_area[current_area_name_en]`, columns `dim_property_type[property_type]`, values `Median Price per Sqft`; conditional formatting by background colour |
| Scatter | Each dot an area: X `Sales`, Y `Median Price per Sqft`, size `Sales Value AED` |
| Table: top projects | `dim_project[project_name_en]`, `dim_project[master_project_en]`, `Sales`, `Sales Value AED`, `Median Price per Sqft`; top 15 by `Sales` |

## Page 3 — Off-plan vs ready

**Message:** whether off-plan sells at a premium, and how much of the market it is. *(Questions 5, 6)*

| Visual | Fields |
|---|---|
| 3 cards | `Off-plan Share`, `Off-plan Premium %`, `Off-plan Sales` |
| Line chart | X `dim_date[month_start]`, Y `Median Price per Sqft Off-plan` and `Median Price per Sqft Ready` |
| Bar chart | Y `dim_area[current_area_name_en]`, X `Off-plan Premium %`, visual filter `Off-plan Sales ≥ 20` |
| 100% stacked column | X `dim_date[year]`, Y `Sales`, legend `fact_sales[is_offplan]` |

## Page 4 — Rental yield

**Message:** what gross yield an investor can expect, and where. *(Question 3)*

| Visual | Fields |
|---|---|
| 3 cards | `Gross Yield %`, `Median Annual Rent AED`, `Median Ready Sale Price AED` (two decimal places) |
| Bar chart: highest yields | Y `dim_area[current_area_name_en]`, X `Gross Yield %`, top 15 (areas below the 10-record minimum show blank and drop out) |
| Scatter | Each dot an area: X `Median Ready Sale Price AED`, Y `Median Annual Rent AED` |
| Text box | "Gross yield = median annual rent / median sale price for the same area, type and period, shown only when each side has at least 10 records. Before service charges, vacancy and fees." |

## Measures

All in the model (`powerbi/model/definition/tables/fact_*.tmdl`), with the same rules as `gold.agg_area_monthly`.

| Measure | Definition |
|---|---|
| `Sales`, `Sales Value AED`, `Sales Value USD` | Count and sum of registered sales (mortgages and gifts are not in `fact_sales`) |
| `Median Price per Sqft`, `Median Sale Price AED`, `Priced Sales` | Medians and count over **homes only**: residential apartments (`Unit`) and villas, outliers excluded. Land trades on plot area and whole buildings sell wholesale, and `Unit` also covers shops and offices; all of them distort a per-sqft median. Counts and values (`Sales`, `Sales Value AED`) keep every registered sale |
| `Median Price per Sqft LY` | Same period last year (`SAMEPERIODLASTYEAR`), blank where the current period has no priced sales |
| `Median Price per Sqft YoY %` | The last 12 months against the 12 before, both ending at the latest sale in the selected years (helpers in *Prices\Helpers*). A same-period comparison over a multi-year selection compares almost the same years |
| `Off-plan Sales`, `Off-plan Share` | Off-plan count, and its share of all sales |
| `Median Price per Sqft Off-plan` / `Ready` | Medians per registration type (homes only), blank below 20 priced sales so thin months don't spike the chart |
| `Off-plan Premium %` | Like for like: over the last 12 months, the off-plan premium within each property type (apartments vs apartments, villas vs villas), weighted by off-plan sales. Over all years, ready sales reach back to 2000 while off-plan sales cluster in recent, pricier years, and off-plan is mostly apartments while ready includes cheaper villas; a plain gap between the two medians showed 64% overall and over 900% in single areas |
| `Contracts`, `Median Annual Rent AED`, `Priced Contracts` | Contract count, and median rent over single-unit contracts for homes (residential apartments and villas), matching the sale side |
| `Median Ready Sale Price AED` | Median ready home sale price in the months that have rents: the price the yield divides by. The Rental yield page shows it instead of the all-years `Median Sale Price AED`, so the cards agree with the yield |
| `Gross Yield %` | Like for like: for each month with rents and each property type (apartments, villas), median annual rent / median **ready** sale price, kept only where both sides have at least 10 records, then averaged weighted by contracts. Off-plan homes cannot be let, and a sale median over other years distorts the price: a plain rent / all-years price ratio showed up to 14.6% in single areas |
| `Price Growth % (50+ sales)` | `Median Price per Sqft YoY %`, blank unless both 12-month periods have at least 50 priced sales: keeps tiny areas off the "fastest risers" chart |
| `Off-plan Premium % (20+ off-plan)` | Off-plan premium, blank below 20 off-plan sales |
| `Area (Arabic)` | The area's Arabic name, for tooltips |

`fact_sales[Registration]` ("Off-plan" / "Ready") labels the share chart. The Property type slicer hides "All types", which exists only for the monthly aggregate's roll-up and has no facts; the projects table hides "Unknown".

## Checklist before export

- [ ] Every number comes from a measure, never an implicit sum (the model discourages implicit measures)
- [ ] Slicers synced across all four pages
- [ ] Arabic tooltip set on every area visual
- [ ] Footer note on every page
- [ ] Latest month marked preliminary (or filtered out of trend charts)

## Insights

The plan's checkpoint: three real insights, with numbers, from the built dashboard. To be written once it runs on real DLD data.

1. _
2. _
3. _
