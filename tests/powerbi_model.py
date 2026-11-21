"""Read the Power BI semantic model (TMDL) well enough to check it against gold.

Not a full TMDL parser: it reads the parts this project writes (tables,
columns, measures, partitions, relationships, expressions).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

MODEL = Path(__file__).parents[1] / "powerbi" / "DubaiPropertyMarket.SemanticModel" / "definition"

_NAME = r"('[^']+'|[A-Za-z_][\w]*)"


def _unquote(name: str) -> str:
    return name[1:-1] if name.startswith("'") else name


@dataclass
class Column:
    name: str
    data_type: str | None = None
    hidden: bool = False
    expression: str | None = None  # calculated columns only


@dataclass
class Table:
    name: str
    columns: dict[str, Column] = field(default_factory=dict)
    measures: dict[str, str] = field(default_factory=dict)  # name -> DAX
    partition_source: str | None = None


def read_table(path: Path) -> Table:
    lines = path.read_text(encoding="utf-8").splitlines()
    table = Table(re.match(rf"table {_NAME}$", lines[0]).group(1))
    current: Column | None = None
    measure: str | None = None
    for line in lines[1:]:
        if m := re.match(rf"\tcolumn {_NAME}(?: = (.+))?$", line):
            current = Column(_unquote(m.group(1)), expression=m.group(2))
            table.columns[current.name] = current
            measure = None
        elif m := re.match(rf"\tmeasure {_NAME} =(?: (.+))?$", line):
            measure, current = _unquote(m.group(1)), None
            table.measures[measure] = m.group(2) or ""
        elif m := re.match(r"\t\tsource = (.+)$", line):
            table.partition_source = m.group(1)
            current = measure = None
        elif line.startswith("\tpartition "):
            current = measure = None
        elif current and (m := re.match(r"\t\tdataType: (\w+)$", line)):
            current.data_type = m.group(1)
        elif current and line == "\t\tisHidden":
            current.hidden = True
        elif measure and line.startswith("\t\t\t"):
            table.measures[measure] += " " + line.strip()
    return table


def read_model(root: Path = MODEL) -> dict[str, Table]:
    return {t.name: t for t in map(read_table, sorted((root / "tables").glob("*.tmdl")))}


def read_relationships(root: Path = MODEL) -> list[tuple[str, str, str, str]]:
    text = (root / "relationships.tmdl").read_text(encoding="utf-8")
    pairs = re.findall(r"fromColumn: (\w+)\.(\w+)\n\ttoColumn: (\w+)\.(\w+)", text)
    return [tuple(p) for p in pairs]


def model_files(root: Path = MODEL) -> list[Path]:
    return sorted(root.rglob("*.tmdl"))


# Spark type (as in DataFrame.dtypes) -> TMDL dataType
SPARK_TO_TMDL = {
    "string": "string",
    "int": "int64",
    "bigint": "int64",
    "double": "double",
    "boolean": "boolean",
    "date": "dateTime",
}


def tmdl_type(spark_type: str) -> str:
    """The type Power BI gives a gold column when it imports it.

    Power Query loads source decimals as "Decimal Number", which TMDL stores as
    ``double``; only an explicit currency type would keep them fixed-point.
    """
    return "double" if spark_type.startswith("decimal") else SPARK_TO_TMDL[spark_type]
