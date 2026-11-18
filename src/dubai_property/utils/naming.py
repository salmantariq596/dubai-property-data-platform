"""Turn raw CSV headers into safe, consistent column names.

Delta Lake rejects names containing spaces or any of `` ,;{}()\\n\\t= ``, and
DLD headers are not guaranteed to be clean. Renaming once, at bronze, means
every later layer can rely on snake_case names.
"""

from __future__ import annotations

import re

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    """``"Actual Worth (AED)"`` -> ``"actual_worth_aed"``.

    Leading underscores are stripped on purpose: names starting with ``_`` are
    reserved for metadata columns added by the pipeline.
    """
    text = name.replace("﻿", "").strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)  # camelCase -> camel_Case
    text = _NON_ALNUM.sub("_", text).strip("_").lower()
    return text


def sanitize_column_names(names: list[str]) -> list[str]:
    """Snake-case every name, fill blanks and make duplicates unique.

    Order is preserved, so the result can be passed straight to ``df.toDF``.
    """
    result: list[str] = []
    seen: set[str] = set()
    for position, raw in enumerate(names, start=1):
        name = to_snake_case(raw) or f"column_{position}"
        if name[0].isdigit():
            name = f"c_{name}"
        candidate, suffix = name, 2
        while candidate in seen:
            candidate, suffix = f"{name}_{suffix}", suffix + 1
        seen.add(candidate)
        result.append(candidate)
    return result
