"""Local SparkSession for the bronze tests.

* Skipped when PySpark or Java is missing, and on Windows unless
  RUN_SPARK_TESTS=1 (local Spark on Windows needs winutils; use WSL or CI).
* Uses Delta Lake when its jars can be downloaded; otherwise falls back to
  Parquet tables and skips the Delta-only tests. CI sets REQUIRE_DELTA=1 so a
  missing Delta setup fails the build instead of being skipped silently.
"""

from __future__ import annotations

import os
import platform
import shutil
import uuid
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

if platform.system() == "Windows" and os.environ.get("RUN_SPARK_TESTS") != "1":
    pytest.skip(
        "Spark tests run in CI/WSL; set RUN_SPARK_TESTS=1 to force", allow_module_level=True
    )
if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
    pytest.skip("Java is required for local Spark", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from dubai_property.config import BronzeSettings  # noqa: E402

_BASE_CONF = {
    "spark.sql.shuffle.partitions": "2",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}


def _builder(warehouse: Path):
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


@pytest.fixture
def settings(tmp_path, table_format) -> BronzeSettings:
    """Fresh landing folder, checkpoint folder and schema for every test."""
    return BronzeSettings(
        catalog="spark_catalog",
        landing_root=str(tmp_path / "landing"),
        checkpoint_root=str(tmp_path / "checkpoints"),
        bronze_schema=f"bronze_{uuid.uuid4().hex[:8]}",
        source="files",
        table_format=table_format,
    )
