"""The pipeline must run on serverless compute (Databricks Free Edition, serverless jobs),
which rejects some Spark APIs. Local tests can't catch that, so this scans the source.

A line that is only reached outside Databricks (for example Parquet tables in local
tests) may use one if it carries a `# local-only` comment."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
SOURCES = sorted((ROOT / "src").rglob("*.py")) + sorted((ROOT / "notebooks").glob("*.py"))

NOT_ON_SERVERLESS = {
    r"\brefreshTable\(|REFRESH TABLE": "REFRESH TABLE",
    r"\bsparkContext\b": "SparkContext",
    r"\.cache\(\)|\.persist\(|CACHE TABLE": "caching",
    r"\blocalCheckpoint\(|setCheckpointDir\(": "RDD checkpoints",
    r"\binput_file_name\(": "input_file_name (use _metadata.file_path)",
    r"\._jvm\b|\._jsc\b|\._jdf\b": "JVM access",
}


def test_no_api_that_serverless_rejects():
    found = []
    for path in SOURCES:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            code, _, comment = line.partition("#")
            if "local-only" in comment:
                continue
            for pattern, what in NOT_ON_SERVERLESS.items():
                if re.search(pattern, code):
                    found.append(f"{path.relative_to(ROOT).as_posix()}:{number}: {what}")
    assert not found, "not supported on serverless:\n" + "\n".join(found)
