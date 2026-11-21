# Power BI

The dashboard reads only the gold tables. It is a Power BI Project (`.pbip`): the report and the semantic model are text files, reviewed in pull requests and tested against gold in CI.

| Path | What it is |
|---|---|
| `DubaiPropertyMarket.pbip` | Open this in Power BI Desktop |
| `DubaiPropertyMarket.SemanticModel/` | The model (TMDL): 7 gold tables, 11 relationships, 20 DAX measures, connection parameters |
| `DubaiPropertyMarket.Report/` | The four report pages |
| `theme/dubai-market.json` | Colours and fonts |
| *(to add)* `DubaiPropertyMarket.pdf` | PDF export, made from real data |

Page designs: [docs/dashboard.md](../docs/dashboard.md). Design decisions: [ADR 0006](../docs/adr/0006-power-bi-model.md).

## Open it

Power BI Desktop is free (Windows). Double-click `DubaiPropertyMarket.pbip`, then click **Refresh** if the yellow bar asks.

## Choose the data

**Home → Transform data → Edit parameters**:

| Parameter | Real data: Databricks SQL warehouse | Real data: Parquet export | Sample data |
|---|---|---|---|
| `GoldSource` | `Databricks` | `Parquet` | `Parquet` |
| `DatabricksHost` | Server hostname (warehouse → *Connection details*) | (ignored) | (ignored) |
| `DatabricksHttpPath` | HTTP path (same page) | (ignored) | (ignored) |
| `GoldCatalog` | `dubai` (`workspace` on Free Edition) | (ignored) | (ignored) |
| `GoldFolder` | (ignored) | Folder with one sub-folder per table | `<repo>\data\sample_gold` |

Then **Close & Apply** and **Refresh**.

- **Databricks:** sign in with a personal access token or Microsoft Entra ID when asked.
- **Parquet export:** run `notebooks/07_export_gold`, then download each table folder from the `gold.exports` volume into `GoldFolder`.
- **Sample data:** `python tools/make_sample_gold.py` writes synthetic gold tables to `data/sample_gold/` (git-ignored). It is for building and checking the report before real data exists. **Every number in it is invented: never publish screenshots, PDFs or insights made from it.**

## Change the report or model

- **Pages and visuals:** edit in Power BI Desktop and save; the `.Report` folder changes.
- **Measures and columns:** edit in Desktop, or edit the `.tmdl` files with Desktop closed (TMDL indents with tabs).
- **Always run the tests** (`make test-fast`). They check that every DAX reference resolves and every relationship joins existing columns; CI also checks every model column against what gold produces.
- **Rename columns in the report, not the model.** Renaming a model column (double-clicking it in the *Data* pane) breaks the link with gold and fails CI. To change a label, rename the field in the visual's *Fields* box instead.

## Publish

Once real data is loaded:

1. **File → Export → Export to PDF** → `powerbi/DubaiPropertyMarket.pdf`.
2. Screenshot each page into `docs/img/dashboard-<page>.png` for the README.
3. Write the three insights in `docs/dashboard.md`.

`.gitignore` keeps Power BI's data cache (`.pbi/cache.abf`) and per-user settings out of the repo, so no data or credentials are committed.
