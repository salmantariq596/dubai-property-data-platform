"""Notebook widgets checked without Databricks: a dropdown or multiselect whose default
is not one of its choices fails on the first run, before any pipeline code runs."""

from __future__ import annotations

import ast
import contextlib
import importlib
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
NOTEBOOKS = sorted((ROOT / "notebooks").glob("*.py"))
CHOICE_WIDGETS = {"dropdown", "multiselect", "combobox"}
RUNTIME_NAMES = {"dbutils", "spark", "display"}


def module_constant(module: str, name: str):
    """The value of `name` in `module`: imported if possible, else read from the source,
    so modules that import PySpark still work in the unit tests."""
    try:
        return getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError):  # AttributeError: a submodule that needs PySpark
        source = ROOT / "src" / Path(*module.split(".")).with_suffix(".py")
        if not source.exists():
            return None
        for node in ast.parse(source.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets
            ):
                return ast.literal_eval(node.value)
        return None  # a function or class: never a widget argument


def widget_calls(path: Path) -> tuple[dict, list[ast.Call]]:
    """The notebook's imported names and simple assignments, and its widget calls."""
    namespace: dict = {}
    calls: list[ast.Call] = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("dubai_property"):
            for alias in node.names:
                namespace[alias.asname or alias.name] = module_constant(node.module, alias.name)
        elif isinstance(node, ast.Assign) and not names & RUNTIME_NAMES:
            # Values built from widgets or Spark can't exist here; a widget that needed
            # one would fail below with a NameError, so skipping them hides nothing.
            with contextlib.suppress(NameError, AttributeError, TypeError):
                exec(compile(ast.Module([node], []), str(path), "exec"), namespace)
        elif (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and ast.unparse(node.value.func.value) == "dbutils.widgets"
        ):
            calls.append(node.value)
    return namespace, calls


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.stem)
def test_choice_widget_defaults_are_among_the_choices(path: Path):
    namespace, calls = widget_calls(path)
    for call in calls:
        if call.func.attr not in CHOICE_WIDGETS:
            continue
        name, default, choices = (eval(ast.unparse(a), dict(namespace)) for a in call.args[:3])
        assert default in list(choices), (
            f"{path.name}: widget {name!r} default {default!r} is not one of {list(choices)}"
        )


def test_the_check_catches_a_joined_multiselect_default(tmp_path: Path):
    notebook = tmp_path / "bad.py"
    notebook.write_text(
        'CHOICES = ["transactions", "rents"]\n'
        'dbutils.widgets.multiselect("datasets", ",".join(CHOICES), CHOICES)\n',
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="not one of"):
        test_choice_widget_defaults_are_among_the_choices(notebook)
