"""The documentation checked as files: every relative link resolves, every ADR is indexed."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SKIP_DIRS = {".git", ".venv", "node_modules", "data", ".pytest_cache"}
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")


def markdown_files() -> list[Path]:
    return sorted(
        p for p in ROOT.rglob("*.md") if not SKIP_DIRS.intersection(p.relative_to(ROOT).parts)
    )


def relative_links(path: Path) -> list[str]:
    text = re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.DOTALL)
    links = [m.group(1) for m in LINK.finditer(text)]
    return [link for link in links if not re.match(r"[a-z]+:|#", link)]


@pytest.mark.parametrize("path", markdown_files(), ids=lambda p: p.relative_to(ROOT).as_posix())
def test_relative_links_resolve(path: Path):
    broken = [
        link for link in relative_links(path) if not (path.parent / link.split("#")[0]).exists()
    ]
    assert not broken, f"broken links in {path.relative_to(ROOT).as_posix()}: {broken}"


def test_every_adr_is_in_the_readme_decisions_table():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    adrs = sorted((ROOT / "docs" / "adr").glob("[0-9][0-9][0-9][0-9]-*.md"))
    assert adrs
    missing = [a.name for a in adrs if f"docs/adr/{a.name}" not in readme]
    assert not missing, f"ADRs not linked from README.md: {missing}"
