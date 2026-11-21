"""Datasets and environment settings shared by every layer.

Nothing in here touches Spark, so it can be imported and tested anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    """One DLD source dataset and where it lands in bronze."""

    name: str  # folder name under the landing volume, e.g. "transactions"
    bronze_table: str  # table name inside the bronze schema
    business_key: str | None  # used by silver for dedup; recorded here for one source of truth
    description: str
    silver_table: str | None = None  # table name inside the silver schema, if cleaned there


DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in (
        Dataset(
            "transactions",
            "dld_transactions_raw",
            "transaction_id",
            "Sales, mortgages, gifts",
            silver_table="transactions",
        ),
        Dataset(
            "rents", "dld_rents_raw", "contract_id", "Ejari rental contracts", silver_table="rents"
        ),
        Dataset("units", "dld_units_raw", None, "Unit reference data"),
        Dataset("projects", "dld_projects_raw", "project_number", "Project reference data"),
    )
}


def get_dataset(name: str) -> Dataset:
    try:
        return DATASETS[name]
    except KeyError:
        raise KeyError(f"Unknown dataset {name!r}. Known: {sorted(DATASETS)}") from None


@dataclass(frozen=True)
class BronzeSettings:
    """Where bronze reads from and writes to.

    Production values come from :meth:`for_databricks`. Tests build their own
    with temporary folders, a throwaway schema and, where Delta Lake is not
    installed, ``table_format="parquet"``.
    """

    catalog: str
    landing_root: str  # <landing_root>/<dataset>/load_date=YYYY-MM-DD/*.csv
    checkpoint_root: str  # one checkpoint folder per dataset
    bronze_schema: str = "bronze"
    source: str = "autoloader"  # "autoloader" on Databricks, "files" for local Spark
    table_format: str = "delta"
    # Part of the idempotency key for writes. Bump it (v1 -> v2) if a
    # checkpoint is ever deleted, otherwise Delta would skip the "new" batch 0
    # as already written. See docs/adr/0002-bronze-ingestion-design.md.
    checkpoint_generation: str = "v1"

    def __post_init__(self) -> None:
        if self.source not in {"autoloader", "files"}:
            raise ValueError(f"source must be 'autoloader' or 'files', got {self.source!r}")

    @classmethod
    def for_databricks(cls, catalog: str = "dubai") -> BronzeSettings:
        return cls(
            catalog=catalog,
            landing_root=f"/Volumes/{catalog}/raw/landing",
            checkpoint_root=f"/Volumes/{catalog}/bronze/checkpoints",
        )

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{name}"

    @property
    def load_log_table(self) -> str:
        return self.table("load_log")

    def landing_path(self, dataset: Dataset) -> str:
        return f"{self.landing_root.rstrip('/')}/{dataset.name}"

    def checkpoint_path(self, dataset: Dataset) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{dataset.name}"

    def write_app_id(self, dataset: Dataset) -> str:
        return f"bronze_{dataset.name}_{self.checkpoint_generation}"


@dataclass(frozen=True)
class SilverSettings:
    """Where silver reads bronze from and writes its tables.

    Silver loads with Delta ``MERGE``, so ``table_format`` must be ``"delta"``
    for a run; tests of the pure transformations need no tables at all.
    """

    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    table_format: str = "delta"

    @classmethod
    def for_databricks(cls, catalog: str = "dubai") -> SilverSettings:
        return cls(catalog=catalog)

    def bronze_table(self, dataset: Dataset) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{dataset.bronze_table}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{name}"

    def silver_table(self, dataset: Dataset) -> str:
        if dataset.silver_table is None:
            raise ValueError(f"Dataset {dataset.name!r} has no silver table")
        return self.table(dataset.silver_table)

    @property
    def quarantine_table(self) -> str:
        return self.table("quarantine")

    @property
    def areas_table(self) -> str:
        return self.table("areas")

    @property
    def processed_loads_table(self) -> str:
        return self.table("processed_loads")


GOLD_TABLES = (
    "dim_date",
    "dim_area",
    "dim_property_type",
    "dim_project",
    "fact_sales",
    "fact_rents",
    "agg_area_monthly",
)


@dataclass(frozen=True)
class GoldSettings:
    """Where gold reads silver from and writes the star schema.

    ``cluster_facts`` liquid-clusters the fact tables by date. It is on for
    Databricks and off for local tests, where open-source Delta may not
    support it.
    """

    catalog: str
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    table_format: str = "delta"
    cluster_facts: bool = False

    @classmethod
    def for_databricks(cls, catalog: str = "dubai") -> GoldSettings:
        return cls(catalog=catalog, cluster_facts=True)

    def silver_table(self, name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{name}"

    def table(self, name: str) -> str:
        if name not in GOLD_TABLES:
            raise KeyError(f"Unknown gold table {name!r}. Known: {list(GOLD_TABLES)}")
        return f"{self.catalog}.{self.gold_schema}.{name}"


@dataclass(frozen=True)
class QualitySettings:
    """Where data quality checks read every layer and write ``dq_results``."""

    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    quality_schema: str = "quality"
    table_format: str = "delta"

    @classmethod
    def for_databricks(cls, catalog: str = "dubai") -> QualitySettings:
        return cls(catalog=catalog)

    @property
    def dq_results_table(self) -> str:
        return f"{self.catalog}.{self.quality_schema}.dq_results"

    @property
    def pipeline_runs_table(self) -> str:
        """One row per job step per run: SUCCESS, or FAILED with the error."""
        return f"{self.catalog}.{self.quality_schema}.pipeline_runs"

    def tables(self) -> dict[str, str]:
        """Placeholder -> table name, for the SQL of the checks: ``{silver_transactions}``."""
        layers = {
            "bronze": (self.bronze_schema, ["load_log"]),
            "silver": (
                self.silver_schema,
                ["transactions", "rents", "areas", "quarantine", "processed_loads"],
            ),
            "gold": (self.gold_schema, list(GOLD_TABLES)),
        }
        return {
            f"{layer}_{name}": f"{self.catalog}.{schema}.{name}"
            for layer, (schema, names) in layers.items()
            for name in names
        }


@dataclass(frozen=True)
class PipelineSettings:
    """Settings for every layer, kept consistent: one catalog, one table format."""

    bronze: BronzeSettings
    silver: SilverSettings
    gold: GoldSettings
    quality: QualitySettings

    @classmethod
    def for_databricks(cls, catalog: str = "dubai") -> PipelineSettings:
        return cls(
            bronze=BronzeSettings.for_databricks(catalog),
            silver=SilverSettings.for_databricks(catalog),
            gold=GoldSettings.for_databricks(catalog),
            quality=QualitySettings.for_databricks(catalog),
        )
