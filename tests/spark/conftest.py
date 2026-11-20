"""Spark tests: skipped where local Spark cannot run. Fixtures live in tests/conftest.py."""

from spark_guard import require_local_spark

require_local_spark()
