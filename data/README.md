# data/

Local working copies of the raw DLD downloads, used for profiling. **Nothing under `data/raw/` is committed** (see `.gitignore`): the files are large and can always be re-downloaded from the official source.

Use the same layout as the Databricks landing volume (`/Volumes/<catalog>/raw/landing/`), so files can be uploaded as they are:

```
data/raw/
├── transactions/load_date=YYYY-MM-DD/<file>.csv
└── rents/load_date=YYYY-MM-DD/<file>.csv
```

Record where and when each file was downloaded in `docs/data_sources.md`.
