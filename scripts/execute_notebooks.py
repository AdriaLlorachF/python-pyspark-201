#!/usr/bin/env python3
"""Ejecuta notebooks/validacion en orden, en un solo proceso (sin kernel Jupyter)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VDIR = ROOT / "notebooks" / "validacion"

ORDER = [
    "00-entorno.ipynb",
    "M01-01-sesion-spark.ipynb",
    "M02-01-ingesta-csv-json.ipynb",
    "M02-02-schema-tipos.ipynb",
    "M02-03-calidad-limpieza.ipynb",
    "M03-01-enriquecimiento.ipynb",
    "M03-02-reglas-negocio.ipynb",
    "M04-01-joins.ipynb",
    "M04-02-kpis.ipynb",
    "M04-03-segmentacion.ipynb",
    "M05-01-ranking-ventana.ipynb",
    "M05-02-acumulados.ipynb",
    "M06-01-explain-dag.ipynb",
    "M06-02-cache-particionado.ipynb",
    "M07-01-parquet-layout.ipynb",
]


def run_notebook(path: Path) -> None:
    nb = json.loads(path.read_text(encoding="utf-8"))
    ns: dict = {}
    for i, cell in enumerate(nb["cells"], 1):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source") or [])
        if not src.strip():
            continue
        try:
            exec(compile(src, f"{path.name}:cell{i}", "exec"), ns, ns)
        except Exception as exc:
            raise RuntimeError(f"{path.name} celda {i}: {exc}") from exc


def main() -> int:
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT / "labs" / "_shared"))
    names = ORDER + (["99-pipeline-completo.ipynb"] if "--all" in sys.argv else [])
    for name in names:
        path = VDIR / name
        print(f"\n######## {name} ########")
        run_notebook(path)
        print(f"OK {name}")
    print("\nNOTEBOOKS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
