# ADR 0006: Power BI model as code, import mode over gold, measures in DAX

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

The dashboard is the part a hiring manager or client actually sees, and Power BI models are usually built by clicking in Power BI Desktop. That leaves no reviewable history and no way to check that the model still matches gold after a change. Compute is also limited: Databricks Free Edition may not offer a SQL warehouse Power BI can connect to.

## Decisions

### 1. The semantic model lives in the repo as TMDL

The model is plain-text TMDL (tables, relationships, measures, parameters) in `powerbi/model/definition/`. It is reviewed in pull requests like any code. Tests check it against gold:

- **Unit tests:** every DAX reference resolves, relationships join existing columns, keys are hidden, the indentation is valid TMDL.
- **Spark test in CI:** every model column has the name and type that gold actually produces. Renaming a gold column without updating the model fails the build.

The report pages are built in Power BI Desktop, which is the right tool for layout, and saved as a Power BI Project (`.pbip`), so they are text too.

### 2. Import mode, one source function, two sources

Tables are imported, not DirectQuery: gold is small (a few million fact rows), refreshes daily, and medians over imported data are fast. All seven tables load through one M function, `GoldTable(name)`. A parameter `GoldSource` switches it between:

- **Databricks:** the SQL warehouse, through the `Databricks.Catalogs` connector;
- **Parquet:** files exported by `notebooks/07_export_gold`. This is the plan's fallback when compute is limited.

Switching source changes one parameter, not seven queries.

### 3. Measures in DAX, with gold's rules

Medians cannot be added up, so every number on the report is a DAX measure that recomputes from the facts for whatever is filtered. The measures apply the same rules as `agg_area_monthly`:

- outliers are excluded from price medians;
- multi-unit contracts are excluded from rent medians;
- gross yield is blank unless both sides have at least 10 records.

A test checks those rules are present. `agg_area_monthly` is in the model too, for matrix visuals at exactly its grain and for checking DAX results against gold.

### 4. SCD Type 2 areas, sliced by today's name

`dim_area` keeps one row per area *version*, and facts point at the version valid on their date. Slicing by `area_name_en` would list a renamed area twice, so two calculated columns give every version its area's **current** names in English and Arabic. Slicers and charts use those columns; the historical name stays available for detail tables.

### 5. The project in the repo is the single source of truth

The model was first written as TMDL and copied into a blank project created by Power BI Desktop, so Desktop generated the report scaffolding in the format of the installed version. The pages were then written in that format (PBIR) and checked by opening them in Desktop. From then on, the project (`powerbi/DubaiPropertyMarket.*`) is the only copy: the tests read its model directly.

### 6. Build the report on synthetic data, publish only real data

`tools/make_sample_gold.py` writes synthetic gold tables with the exact model schema and gold's rules (outliers, multi-unit contracts, an SCD Type 2 rename), so the report can be built and checked before real data has been through the pipeline. The files are git-ignored, and screenshots, the PDF and insights are made only from real data.

### 7. The Databricks connector is looked up at run time

Power BI checks every name in a query before running it, and it does not know `Databricks.Catalogs` until that connector has been used once. A direct reference would break the Parquet route too, so the source function finds the connector in `#shared` only when `GoldSource` is `Databricks`.

## Consequences

- Model changes are reviewed and tested; a gold change that would break the report is caught in CI.
- Renaming a model column in Desktop breaks the link with gold; the tests catch it, and labels are renamed in visuals instead.
- The dashboard needs real data before its insights mean anything. The page specs (`docs/dashboard.md`) are ready, and the insights are written after the first real load.
