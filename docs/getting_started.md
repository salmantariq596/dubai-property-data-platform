# Getting started: from clone to dashboard on real data

This walks through the platform from a fresh clone to the Power BI dashboard on real DLD data. Every step was run in that order in November 2026, and the traps found on the way are written in. Parts 1 to 4 cost nothing; part 5 (Azure) is the production deployment.

| Part | What you get | Time | Cost |
|---|---|---|---|
| [1. Local](#part-1-clone-and-test-locally) | Code installed, unit tests passing, sample data | 10 min | Free |
| [2. Data](#part-2-get-the-data) | 1.38 million real DLD sales on your PC | 10 min | Free |
| [3. Databricks](#part-3-run-the-pipeline-on-databricks-free-edition) | Bronze, silver and gold built from real data, every check recorded | 30 min | Free |
| [4. Power BI](#part-4-the-dashboard-in-power-bi) | The four-page dashboard on real data | 20 min | Free |
| [5. Azure](#part-5-the-full-platform-on-azure) | Daily scheduled runs, monitoring, deploy on merge | 1–2 h | Azure credit |

## What you need

**Software**

| Software | For | Needed in |
|---|---|---|
| Git | Cloning | Part 1 |
| Python 3.10 or newer | Running and testing the code | Part 1 |
| GNU Make (Windows: `choco install make`) | The `make` shortcuts; each target is also a plain command in the `Makefile` | Part 1 |
| Power BI Desktop (Windows, free) | The dashboard | Part 4 |
| Terraform 1.6+, Azure CLI, Databricks CLI | Building and deploying Azure | Part 5 |
| Java 17 | Only for running the Spark tests locally (they always run in CI) | Optional |

**Accounts**

| Account | Cost | Needed in |
|---|---|---|
| Kaggle | Free | Part 2 (download the data) |
| Databricks Free Edition (sign up with an email at databricks.com) | Free, no card | Part 3 |
| Azure | Free account with time-limited credit; needs a card and a phone | Part 5 only. Activate it when you are ready to deploy: the credit starts counting down at once |

## Part 1: clone and test locally

```powershell
git clone https://github.com/salmantariq596/dubai-property-data-platform.git
cd dubai-property-data-platform
make setup                                # virtual environment, dependencies, pre-commit hooks
make test-fast                            # unit tests, no Spark needed
make lint
.venv\Scripts\python tools\make_sample_gold.py   # synthetic gold tables (macOS/Linux: .venv/bin/python)
```

**Expected:** `make test-fast` ends with every unit test passed and 2 skipped (the Spark test folders skip themselves when PySpark is not installed). `make_sample_gold.py` prints seven tables and writes them to `data/sample_gold/`. Every number in the sample is invented: never publish anything made from it.

`make profile-sample` profiles the bundled synthetic file and rewrites `docs/profiles/sample_transactions.md`. Only its "Generated" timestamp changes; discard it with `git restore docs/profiles/sample_transactions.md`.

## Part 2: get the data

**Where DLD publishes it.** The Dubai Land Department's bulk open data is on Dubai Pulse (`dubaipulse.gov.ae`), and its website's open-data page hands downloads off to Dubai Pulse. **Dubai Pulse does not answer from outside the UAE**: in November 2026 it timed out from Pakistan and from Databricks Free Edition, while other sites loaded. From inside the UAE, download `dld_transactions-open` there. From anywhere else, use the redistributed copy below. In production, Data Factory runs in Azure's UAE North region.

**The redistributed copy.** On Kaggle, "Dubai Real Estate Sales Transactions 2004-2026" by Dubai Real Estate Data: 1,382,848 registered DLD sales, CC BY 4.0. Download it and unzip. Only `dld_sales_transactions.csv` (about 230 MB) goes into the pipeline; keep the rest of the download (data dictionary, checksums, license) beside it. Silver reads this file's own column names through `SOURCE_ALIASES` in `src/dubai_property/silver/rules.py`, and treats it as sales only because it has no `trans_group_en` column. Every figure published from it must carry the credit in [data sources](data_sources.md).

**Optional: profile it locally** (about 3 minutes):

```powershell
.venv\Scripts\dld-profile "C:\path\to\dld_sales_transactions.csv" --key transaction_id --output docs\profiles\transactions.md
```

> **PowerShell trap:** quote any path that contains `=`. `mkdir data\raw\transactions\load_date=2026-11-23` creates a folder named just `load_date`. Use `mkdir "data\raw\transactions\load_date=2026-11-23"`.

## Part 3: run the pipeline on Databricks Free Edition

**1. Bring the repo in as a Git folder.** In Databricks: **Workspace → Create → Git folder**, URL `https://github.com/salmantariq596/dubai-property-data-platform`, provider GitHub. The repo is public: no token needed.

**2. Create the catalog, schemas and volumes.** Open `notebooks/00_setup_unity_catalog`, connect to **Serverless**, **Run all**. It ends with `Landing folders: ['projects', 'rents', 'transactions', 'units']`. If it cannot create the `dubai` catalog, set the `catalog` box to `workspace` and run again, and use `workspace` everywhere below.

**3. Land the file.** **Catalog → dubai → raw → Volumes → landing → transactions → Create directory** `load_date=YYYY-MM-DD` (today's date, spelled exactly like that), open it, **Upload to this volume**, and upload only the CSV.

**4. Run the pipeline.** Open `notebooks/06_run_pipeline`, connect to **Serverless**, **Run all**. Parameter boxes appear above the notebook after the first cell has run; the defaults (`dubai`, `transactions,rents`) are fine, because a dataset with no files is simply skipped.

**Expected** on the full sales file, in about 4 minutes:

```
[SUCCESS] transactions load ...: 1 file(s), 1,382,848 rows, 0 rescued
[SUCCESS] transactions: 1 load(s), 1,382,848 rows read, 1,382,848 merged, 0 quarantined, 0 duplicates
[SUCCESS] gold ...: dim_area 244, ..., fact_sales 1,382,848, fact_rents 0, agg_area_monthly 78,785
[PASS] bronze data quality    [WARN] silver data quality    [PASS] gold data quality
```

The two silver warnings are expected: 2,262 sales (0.16%) dated before 2000, outside the date dimension, and 2 rows without a size. A **FAIL** would stop the run before the next layer.

**5. Prove that re-runs are safe.** Click **Run all** again without new files: bronze reports `NO_NEW_FILES`, silver `NO_NEW_LOADS`, and gold rebuilds with the same counts, in about a minute.

**Trying the bundled sample first?** Upload `tests/fixtures/sample_transactions.csv` and `sample_rents.csv` the same way. Expected: bronze PASS, silver WARN (the planted bad rows are quarantined), gold PASS with 7 sales and 8 contracts. Before loading real data, clear the sample out in a scratch notebook, then run `00_setup_unity_catalog` again:

```python
load_date = "YYYY-MM-DD"  # the folder the sample files were uploaded to
for schema in ["bronze", "silver", "gold", "quality"]:
    spark.sql(f"DROP SCHEMA IF EXISTS dubai.{schema} CASCADE")
for dataset in ["transactions", "rents"]:
    dbutils.fs.rm(f"/Volumes/dubai/raw/landing/{dataset}/load_date={load_date}", True)
```

**Updating the code in Databricks.** In the Git folder: branch button → **Pull**. Two traps:
- **"Conflict checking out branch due to outstanding changes":** running a notebook saves widget settings into it. In the Git window, discard the listed changes, then pull or switch branch.
- **An open notebook keeps running the old code after a pull**, because Python has already loaded the package. Use **Serverless → Reset session** before running again.

## Part 4: the dashboard in Power BI

Open `powerbi/DubaiPropertyMarket.pbip` in Power BI Desktop. The report reads the gold tables one of two ways, chosen with **Home → Transform data → Edit parameters** (details in [powerbi/README.md](../powerbi/README.md)).

**Parquet export: works everywhere, used for this walkthrough.**
1. In Databricks, run `notebooks/07_export_gold`. It prints the rows per table.
2. **Catalog → dubai → gold → Volumes → exports**: each table folder holds one `part-….parquet` file. Download each into a folder named after its table, outside the repo:

   ```powershell
   "agg_area_monthly","dim_area","dim_date","dim_project","dim_property_type","fact_rents","fact_sales" | ForEach-Object { New-Item -ItemType Directory -Force -Path "C:\dubai-gold\$_" }
   ```

3. Parameters: `GoldSource` = `Parquet`, `GoldFolder` = `C:\dubai-gold`. **OK → Apply changes.**

**Databricks SQL warehouse: live, if your Power BI has the connector.** Parameters: `GoldSource` = `Databricks`, host and HTTP path from **SQL Warehouses → (warehouse) → Connection details**, `GoldCatalog` = `dubai`. Sign in with a personal access token (**profile → Settings → Developer → Access tokens**); it stays in Power BI's credential store, never in the repo. If **Get data** finds no "Databricks" source, or the load fails with *"We cannot convert the value null to type Function"*, this Power BI has no Databricks connector: use the Parquet export.

**Expected:** Market overview shows 1.38M sales, a median of about 1,310 AED per sqft (homes only), and a ranking of the fastest-rising areas. The Rental yield page stays empty until rents are loaded. Measure definitions and why they are built that way: [dashboard](dashboard.md).

## Part 5: the full platform on Azure

Terraform builds everything, in this order; each stack's variables and backend files are explained where linked.

| Step | Command | Guide |
|---|---|---|
| Budget alert, Terraform state | `terraform -chdir=infra/bootstrap apply` | [infra](../infra/README.md) |
| Lake, Databricks, Key Vault, Data Factory | `terraform -chdir=infra/platform apply` | [infra](../infra/README.md) |
| Catalog, schemas, volumes | `terraform -chdir=infra/unity-catalog apply` | [infra](../infra/README.md) |
| The daily job and the health check | `databricks bundle deploy -t azure` | [orchestration](orchestration.md) |
| Data Factory pipelines and the daily trigger | `terraform -chdir=infra/orchestration apply` | [orchestration](orchestration.md) |
| Deploy on merge from GitHub (OIDC, no secrets) | `terraform -chdir=infra/cicd apply` | [CI/CD](cicd.md) |

Choose the **UAE North** region so Data Factory can reach Dubai Pulse. When something fails: [runbook](runbook.md). Run `terraform destroy` on `platform` and `unity-catalog` when you are done, to stop the costs.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'pyarrow'` | A clone from before the fix | `git pull`, then `make setup` |
| Dubai Pulse pages time out | Not reachable from outside the UAE | Use the Kaggle copy (part 2), or deploy in UAE North (part 5) |
| A folder created as `load_date` without the date | PowerShell split the unquoted `=` | Quote the path |
| `DefaultValueNotInChoicesList` in `06_run_pipeline` | Old notebook version | Pull, reset the session |
| `NOT_SUPPORTED_WITH_SERVERLESS: REFRESH TABLE` | Old code, or old code still loaded | Pull, **Reset session**, run again |
| A fix pulled into Databricks has no effect | The session still runs the old code | **Serverless → Reset session** |
| Git folder: "conflict ... outstanding changes" | Notebook settings saved by a run | Discard the changes in the Git window |
| Power BI: "cannot convert the value null to type Function" | No Databricks connector in this Power BI | Parquet export (part 4) |
| Power BI shows synthetic numbers | `GoldFolder` still points at `data\sample_gold` | Point it at the real export |
