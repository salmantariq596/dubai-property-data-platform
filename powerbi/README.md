# Power BI

The dashboard reads only the gold tables. This folder holds:

| Path | What it is |
|---|---|
| `model/definition/` | The semantic model in TMDL: 7 gold tables, 11 relationships, 17 DAX measures, connection parameters |
| `theme/dubai-market.json` | Colours and fonts for every page |
| *(you add)* `DubaiPropertyMarket.pbip` + folders | The Power BI project, once the pages are built |
| *(you add)* `DubaiPropertyMarket.pdf` | PDF export of the report |

Page designs: [docs/dashboard.md](../docs/dashboard.md). Design decisions: [ADR 0006](../docs/adr/0006-power-bi-model.md).

## 1. Create the project with the model

Power BI Desktop (free, Windows) builds the report files itself, so start from a blank project and drop the model in:

1. In Power BI Desktop, **File → Options → Preview features**: make sure *Power BI Project (.pbip) save option* is on (it is on by default in recent versions).
2. Start a blank report and **File → Save as → Power BI project files (*.pbip)**, into this folder, as `DubaiPropertyMarket`. Close Power BI Desktop.
3. Copy everything inside `powerbi/model/definition/` into `powerbi/DubaiPropertyMarket.SemanticModel/definition/`, replacing the files there.
4. Open `DubaiPropertyMarket.pbip`. The model view should show 7 tables and 11 relationships.

If Power BI reports a problem with a file in step 4, it names the file and line. Tell me and I'll fix it: the model was written without Power BI Desktop to hand.

## 2. Point it at the data

**Home → Transform data → Edit parameters**:

| Parameter | Databricks SQL warehouse | Parquet files (fallback) |
|---|---|---|
| `GoldSource` | `Databricks` | `Parquet` |
| `DatabricksHost` | Server hostname from the warehouse's *Connection details* | (ignored) |
| `DatabricksHttpPath` | HTTP path from the same page | (ignored) |
| `GoldCatalog` | `dubai` (or `workspace` on Free Edition) | (ignored) |
| `GoldFolder` | (ignored) | Folder holding one sub-folder per table, e.g. `C:\dubai-gold` |

- **Databricks:** sign in with a personal access token or Microsoft Entra ID when asked.
- **Parquet:** run `notebooks/07_export_gold` on Databricks, then download each table folder from the `gold.exports` volume into `GoldFolder`. Use this when no SQL warehouse is available, for example on Free Edition.

Then **Close & Apply** and **Refresh**.

## 3. Apply the theme

**View → Themes → Browse for themes** → `powerbi/theme/dubai-market.json`.

## 4. Build the pages

Follow [docs/dashboard.md](../docs/dashboard.md): four pages, the slicers, and the Arabic tooltip. Use the measures, not raw columns, for every number. Medians cannot be summed, and the measures apply the same outlier and sample-size rules as gold.

## 5. Export and commit

1. **File → Export → Export to PDF** → `powerbi/DubaiPropertyMarket.pdf`.
2. Screenshot each page into `docs/img/dashboard-<page>.png` for the README.
3. Save, then commit the `.pbip` file and both `DubaiPropertyMarket.*` folders. `.gitignore` already excludes the local cache and per-user settings, so no data or credentials are committed.
4. Write the three insights in `docs/dashboard.md`.

Once the pages exist, the copy in `model/definition/` can be deleted: the project's own `SemanticModel` folder becomes the source of truth.
