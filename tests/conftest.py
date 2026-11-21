"""Fixtures for every test folder.

Spark fixtures import PySpark lazily, so the unit tests run without it. The
Spark folders (``tests/spark``, ``tests/integration``) skip themselves when
local Spark is not available; see ``tests/spark_guard.py``.

* Uses Delta Lake when its jars can be downloaded; otherwise falls back to
  Parquet tables and skips the Delta-only tests. CI sets REQUIRE_DELTA=1 so a
  missing Delta setup fails the build instead of being skipped silently.
* Every test gets fresh, randomly named schemas for each layer.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from dubai_property.config import (
    BronzeSettings,
    GoldSettings,
    PipelineSettings,
    QualitySettings,
    SilverSettings,
)

FIXTURES = Path(__file__).parent / "fixtures"

_BASE_CONF = {
    "spark.sql.shuffle.partitions": "2",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
    # One session runs every test; gold tests build deep plans. Room to spare, and
    # plan strings capped so logging a plan never fills the heap.
    "spark.driver.memory": "3g",
    "spark.sql.maxPlanStringLength": "100000",
    "spark.sql.ui.retainedExecutions": "20",
    # Delta rebuilds a table's state from its log in 50 tasks by default, sized for a
    # cluster. Test tables have a handful of commits: 2 tasks is much faster.
    "spark.databricks.delta.snapshotPartitions": "2",
    "spark.default.parallelism": "2",
}


@pytest.fixture
def sample_csv() -> Path:
    return FIXTURES / "sample_transactions.csv"


# --- Spark session ------------------------------------------------------------------


def _builder(warehouse: Path):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[2]").appName("dpdp-tests")
    for key, value in {**_BASE_CONF, "spark.sql.warehouse.dir": str(warehouse)}.items():
        builder = builder.config(key, value)
    return builder


def _delta_session(warehouse: Path):
    from delta import configure_spark_with_delta_pip

    builder = (
        _builder(warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark_env(tmp_path_factory):
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    try:
        spark = _delta_session(warehouse)
        spark.range(1).write.format("delta").save(str(warehouse / "_probe"))
        table_format = "delta"
    except Exception as exc:
        if os.environ.get("REQUIRE_DELTA") == "1":
            raise RuntimeError("REQUIRE_DELTA=1 but Delta Lake could not start") from exc
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
        spark = _builder(warehouse).getOrCreate()
        table_format = "parquet"
    yield spark, table_format
    spark.stop()


@pytest.fixture
def spark(spark_env):
    return spark_env[0]


@pytest.fixture
def table_format(spark_env):
    return spark_env[1]


@pytest.fixture
def requires_delta(table_format):
    if table_format != "delta":
        pytest.skip("needs Delta Lake (runs in CI)")


# --- settings: one throwaway schema per layer, per test -----------------------------------


def _schema(layer: str) -> str:
    return f"{layer}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def settings(tmp_path, table_format) -> BronzeSettings:
    """Fresh landing folder, checkpoint folder and bronze schema."""
    return BronzeSettings(
        catalog="spark_catalog",
        landing_root=str(tmp_path / "landing"),
        checkpoint_root=str(tmp_path / "checkpoints"),
        bronze_schema=_schema("bronze"),
        source="files",
        table_format=table_format,
    )


@pytest.fixture
def silver_settings(settings) -> SilverSettings:
    return SilverSettings(
        catalog=settings.catalog,
        bronze_schema=settings.bronze_schema,
        silver_schema=_schema("silver"),
        table_format=settings.table_format,
    )


@pytest.fixture
def gold_settings(silver_settings) -> GoldSettings:
    return GoldSettings(
        catalog=silver_settings.catalog,
        silver_schema=silver_settings.silver_schema,
        gold_schema=_schema("gold"),
        table_format=silver_settings.table_format,
    )


@pytest.fixture
def quality_settings(settings, silver_settings, gold_settings) -> QualitySettings:
    return QualitySettings(
        catalog=settings.catalog,
        bronze_schema=settings.bronze_schema,
        silver_schema=silver_settings.silver_schema,
        gold_schema=gold_settings.gold_schema,
        quality_schema=_schema("quality"),
        table_format=settings.table_format,
    )


@pytest.fixture
def pipeline_settings(settings, silver_settings, gold_settings, quality_settings):
    return PipelineSettings(settings, silver_settings, gold_settings, quality_settings)
