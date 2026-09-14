"""Construcción de notebooks .ipynb (sin depender de nbformat en el host)."""
from __future__ import annotations

import json
import uuid
from pathlib import Path

CELDA_0 = """import sys
from pathlib import Path

_here = Path.cwd().resolve()
ROOT = next(
    p
    for p in [_here, *_here.parents]
    if (p / "labs" / "_shared" / "session.py").is_file()
)
sys.path.insert(0, str(ROOT / "labs" / "_shared"))

from paths import RAW, STAGING, CURATED
from session import get_spark

print("ROOT   ", ROOT)
print("RAW    ", RAW, "existe:", RAW.is_dir())
print("STAGING", STAGING)
print("CURATED", CURATED)
"""


def _cell(cell_type: str, source: str) -> dict:
    lines = source.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    cell = {
        "cell_type": cell_type,
        "metadata": {},
        "source": lines,
    }
    if cell_type == "code":
        cell["execution_count"] = None
        cell["outputs"] = []
        cell["id"] = uuid.uuid4().hex[:8]
    return cell


def md(text: str) -> dict:
    return _cell("markdown", text)


def code(text: str) -> dict:
    return _cell("code", text)


def write_notebook(path: Path, cells: list) -> None:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python (NovaShop)",
                "language": "python",
                "name": "novashop",
            },
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "cells": cells,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
