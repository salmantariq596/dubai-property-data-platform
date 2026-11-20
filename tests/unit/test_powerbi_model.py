"""The Power BI semantic model is consistent with itself. No Spark, no Power BI needed."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from dubai_property.config import GOLD_TABLES
from powerbi_model import MODEL, model_files, read_model, read_relationships

TABLES = read_model()
MEASURES = {m: t.name for t in TABLES.values() for m in t.measures}
THEME = Path(__file__).parents[2] / "powerbi" / "theme" / "dubai-market.json"


def test_every_gold_table_is_in_the_model():
    assert set(TABLES) == set(GOLD_TABLES)
    refs = re.findall(r"^ref table (\w+)$", (MODEL / "model.tmdl").read_text(), re.M)
    assert sorted(refs) == sorted(GOLD_TABLES)


@pytest.mark.parametrize("path", model_files(), ids=lambda p: p.name)
def test_indentation_is_tabs(path):
    """TMDL nests by tabs; a leading space breaks the file."""
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        assert not line.startswith(" "), f"{path.name}:{number} starts with a space"


@pytest.mark.parametrize("table", TABLES.values(), ids=lambda t: t.name)
def test_each_table_loads_its_own_gold_table(table):
    assert table.partition_source == f'GoldTable("{table.name}")'


@pytest.mark.parametrize("table", TABLES.values(), ids=lambda t: t.name)
def test_columns_are_typed_and_keys_hidden(table):
    for column in table.columns.values():
        assert column.data_type in {"string", "int64", "double", "decimal", "boolean", "dateTime"}
        if column.name.endswith(("_sk", "_key")) or column.name.startswith("_"):
            assert column.hidden, f"{table.name}.{column.name} should be hidden"


def test_relationships_join_existing_columns_many_to_one():
    relationships = read_relationships()
    assert len(relationships) == 11
    for from_table, from_col, to_table, to_col in relationships:
        assert from_col in TABLES[from_table].columns
        assert to_col in TABLES[to_table].columns
        assert to_table.startswith("dim_")  # facts and the aggregate point at dimensions


def test_measure_names_are_unique():
    names = [m for t in TABLES.values() for m in t.measures]
    assert len(names) == len(set(names))


def _dax():
    for table in TABLES.values():
        for name, expression in table.measures.items():
            yield f"{table.name}[{name}]", expression
        for column in table.columns.values():
            if column.expression:
                yield f"{table.name}.{column.name}", column.expression


@pytest.mark.parametrize(("where", "dax"), list(_dax()), ids=lambda v: str(v)[:40])
def test_dax_references_exist(where, dax):
    for table, column in re.findall(r"\b(\w+)\[([^\]]+)\]", dax):
        assert table in TABLES, f"{where}: unknown table {table}"
        assert column in TABLES[table].columns, f"{where}: unknown column {table}[{column}]"
    for measure in re.findall(r"(?<![\w\]])\[([^\]]+)\]", dax):
        assert measure in MEASURES, f"{where}: unknown measure [{measure}]"


def test_the_plan_measures_exist():
    """Phase 7: median price per sqft, YoY change, sales volume, gross yield."""
    assert {
        "Median Price per Sqft",
        "Median Price per Sqft YoY %",
        "Sales",
        "Sales Value AED",
        "Gross Yield %",
        "Off-plan Share",
        "Median Annual Rent AED",
    } <= set(MEASURES)


def test_dax_medians_match_gold_rules():
    """Outliers and multi-unit contracts are left out of medians, as in agg_area_monthly."""
    sales = TABLES["fact_sales"].measures
    assert "fact_sales[is_outlier] = FALSE ()" in sales["Median Price per Sqft"]
    rents = TABLES["fact_rents"].measures
    assert "fact_rents[is_multi_unit] = FALSE ()" in rents["Median Annual Rent AED"]
    assert ">= 10" in rents["Gross Yield %"]  # gold.rules.MIN_RECORDS_FOR_YIELD


def test_slicers_can_use_todays_area_name():
    columns = TABLES["dim_area"].columns
    assert columns["current_area_name_en"].expression
    assert columns["current_area_name_ar"].expression  # Arabic tooltip


def test_parameters_and_source_function():
    text = (MODEL / "expressions.tmdl").read_text(encoding="utf-8")
    for parameter in ("GoldSource", "DatabricksHost", "DatabricksHttpPath", "GoldCatalog"):
        assert f"expression {parameter} = " in text
    assert "expression GoldTable =" in text
    assert "Databricks.Catalogs(" in text and "Parquet.Document(" in text


def test_theme():
    theme = json.loads(THEME.read_text(encoding="utf-8"))
    assert theme["name"]
    assert len(theme["dataColors"]) >= 6
    assert all(re.fullmatch(r"#[0-9A-F]{6}", c) for c in theme["dataColors"])
