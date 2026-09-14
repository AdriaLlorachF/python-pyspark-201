"""Punto único de arranque para tus notebooks y para `_qa`."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from paths import COUNTS, CURATED, RAW, ROOT, STAGING, ensure_dirs
from session import get_spark

__all__ = [
    "COUNTS",
    "CURATED",
    "RAW",
    "ROOT",
    "STAGING",
    "canonical",
    "ensure_dirs",
    "get_spark",
    "locate_root",
]


def locate_root(start: Path | None = None) -> Path:
    here = (start or Path.cwd()).resolve()
    for cand in [here, *here.parents]:
        if (cand / "labs" / "_shared" / "session.py").is_file():
            return cand
    raise FileNotFoundError(
        "No encuentro el repo python-pyspark-201. "
        "En el Codespace abre la carpeta raíz del repositorio (File → Open Folder)."
    )


def canonical() -> dict:
    return json.loads(COUNTS.read_text(encoding="utf-8"))


def bind_shared(root: Path | None = None) -> Path:
    repo = locate_root(root)
    shared = str(repo / "labs" / "_shared")
    if shared not in sys.path:
        sys.path.insert(0, shared)
    return repo
