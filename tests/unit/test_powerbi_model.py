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
REPORT = Path(__file__).parents[2] / "powerbi" / "DubaiPropertyMarket.Report" / "definition"


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
    local = set(re.findall(r'"(\w+)",', dax))  # columns added in the expression (ADDCOLUMNS)
    clashes = sorted(local & set(MEASURES))
    assert not clashes, (
        f"{where}: added columns {clashes} share a measure's name (ambiguous in DAX)"
    )
    for measure in re.findall(r"(?<![\w\]])\[([^\]]+)\]", dax):
        assert measure in MEASURES or measure in local, f"{where}: unknown measure [{measure}]"


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


HOMES = (
    'KEEPFILTERS ( fact_sales[property_usage_en] = "Residential" )',
    'KEEPFILTERS ( dim_property_type[property_type] IN { "Unit", "Villa" } )',
)


@pytest.mark.parametrize(
    "measure", ["Priced Sales", "Median Price per Sqft", "Median Sale Price AED"]
)
def test_prices_are_for_homes_only(measure):
    """Land (plot area) and whole buildings (wholesale) distort per-sqft medians, and
    "Unit" also covers shops and offices. KEEPFILTERS keeps the property type slicer working."""
    dax = TABLES["fact_sales"].measures[measure]
    assert all(rule in dax for rule in HOMES)


def test_price_growth_compares_the_last_12_months_with_the_12_before():
    """Same period last year over a multi-year selection compares almost the same years."""
    sales = TABLES["fact_sales"].measures
    assert "SAMEPERIODLASTYEAR" not in sales["Median Price per Sqft YoY %"]
    assert "[Median Price per Sqft 12M]" in sales["Median Price per Sqft YoY %"]
    assert "-12 ) && dim_date[date] <= LastDay" in sales["Median Price per Sqft 12M"]
    assert (
        "-24 ) && dim_date[date] <= EDATE ( LastDay, -12 )"
        in sales["Median Price per Sqft Prior 12M"]
    )
    growth = sales["Price Growth % (50+ sales)"]
    assert "[Priced Sales 12M] >= 50" in growth and "[Priced Sales Prior 12M] >= 50" in growth


def test_thin_months_and_missing_data_are_blank():
    sales = TABLES["fact_sales"].measures
    assert sales["Median Price per Sqft LY"].startswith("IF ( [Priced Sales] > 0")
    for side in ("Off-plan", "Ready"):
        assert ">= 20" in sales[f"Median Price per Sqft {side}"]


def test_offplan_premium_compares_like_with_like():
    """Over all years, ready sales reach back to 2000 while off-plan sales cluster in recent,
    pricier years, and off-plan is mostly apartments while ready includes cheaper villas."""
    premium = TABLES["fact_sales"].measures["Off-plan Premium %"]
    assert "VALUES ( dim_property_type[property_type] )" in premium  # within each type
    assert "EDATE ( LastDay, -12 )" in premium  # the last 12 months
    assert "* [Weight]" in premium  # weighted by off-plan sales


@pytest.mark.parametrize("measure", ["Priced Contracts", "Median Annual Rent AED"])
def test_rents_are_for_homes_only(measure):
    """The sale side of the yield is homes only; the rent side must match it."""
    dax = TABLES["fact_rents"].measures[measure]
    assert 'KEEPFILTERS ( fact_rents[property_usage_en] = "Residential" )' in dax
    assert HOMES[1] in dax


def test_gross_yield_compares_like_with_like():
    """Rent and sale price from the same month and property type, ready sales only:
    off-plan homes cannot be let, and sales from other years distort the price."""
    yield_dax = TABLES["fact_rents"].measures["Gross Yield %"]
    assert "CROSSJOIN ( Months, Types )" in yield_dax
    assert "VALUES ( dim_date[month_key] ), fact_rents" in yield_dax  # months with rents
    assert yield_dax.count("fact_sales[is_offplan] = FALSE ()") == 2  # price and count
    assert "* [CellContracts]" in yield_dax  # weighted by contracts


def test_parquet_route_reads_only_the_newest_export():
    """A re-download next to an old export file must not double the rows."""
    text = (MODEL / "expressions.tmdl").read_text(encoding="utf-8")
    assert '"Date modified", Order.Descending' in text
    assert "Table.Combine" not in text


def test_parameter_defaults_are_not_machine_specific():
    text = (MODEL / "expressions.tmdl").read_text(encoding="utf-8")
    folder = re.search(r'expression GoldFolder = "([^"]*)"', text).group(1)
    assert "dubai-property-data-platform" not in folder and "Users" not in folder


def test_dates_stop_today_on_every_page():
    """The date table runs to 2030 for future rent end dates; slicers and axes must not."""
    report = json.loads((REPORT / "report.json").read_text(encoding="utf-8"))
    filters = report["filterConfig"]["filters"]
    dates = [f for f in filters if f["field"]["Column"]["Property"] == "date"]
    assert [f["type"] for f in dates] == ["RelativeDate"]
    assert '"Now"' in json.dumps(dates[0]["filter"])


def test_slicers_can_use_todays_area_name():
    columns = TABLES["dim_area"].columns
    assert columns["current_area_name_en"].expression
    assert columns["current_area_name_ar"].expression  # Arabic tooltip


def test_parameters_and_source_function():
    text = (MODEL / "expressions.tmdl").read_text(encoding="utf-8")
    for parameter in ("GoldSource", "DatabricksHost", "DatabricksHttpPath", "GoldCatalog"):
        assert f"expression {parameter} = " in text
    assert "expression GoldTable =" in text
    assert "Databricks.Catalogs" in text and "Parquet.Document(" in text
    assert "Connector(DatabricksHost" in text  # looked up at run time, see expressions.tmdl


def test_theme():
    theme = json.loads(THEME.read_text(encoding="utf-8"))
    assert theme["name"]
    assert len(theme["dataColors"]) >= 6
    assert all(re.fullmatch(r"#[0-9A-F]{6}", c) for c in theme["dataColors"])
