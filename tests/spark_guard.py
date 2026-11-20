"""Skip a Spark test folder when local Spark cannot run.

Called at the top of the Spark folders' ``conftest.py``. Skipped when PySpark
or Java is missing, and on Windows unless ``RUN_SPARK_TESTS=1`` (local Spark
on Windows needs winutils; use WSL or CI).
"""

from __future__ import annotations

import os
import platform
import shutil

import pytest


def require_local_spark() -> None:
    pytest.importorskip("pyspark")
    if platform.system() == "Windows" and os.environ.get("RUN_SPARK_TESTS") != "1":
        pytest.skip(
            "Spark tests run in CI/WSL; set RUN_SPARK_TESTS=1 to force", allow_module_level=True
        )
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java is required for local Spark", allow_module_level=True)
