"""CI runs Spark tests in named groups: every Spark test file must be in exactly one."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
WORKFLOW = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")


def test_every_spark_test_file_runs_in_ci_exactly_once():
    listed = re.findall(r"tests/(?:spark/test_\w+\.py|integration)\b", WORKFLOW)
    spark_files = (ROOT / "tests" / "spark").glob("test_*.py")
    expected = [p.relative_to(ROOT).as_posix() for p in spark_files]
    expected.append("tests/integration")
    assert sorted(listed) == sorted(expected), "update the spark matrix in .github/workflows/ci.yml"


def test_unit_tests_run_without_spark():
    assert "pytest tests/unit " in WORKFLOW
