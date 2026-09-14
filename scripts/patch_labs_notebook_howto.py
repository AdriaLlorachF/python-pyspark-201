#!/usr/bin/env python3
"""Inserta el bloque 'Tu notebook' en cada lab si aún no está."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LABS = ROOT / "labs"

NOTEBOOKS = {
    "M01-01-sesion-spark-primer-dataframe.md": "M01-01-sesion-spark.ipynb",
    "M02-01-ingesta-csv-json.md": "M02-01-ingesta-csv-json.ipynb",
    "M02-02-schema-tipos.md": "M02-02-schema-tipos.ipynb",
    "M02-03-calidad-limpieza.md": "M02-03-calidad-limpieza.ipynb",
    "M03-01-enriquecimiento.md": "M03-01-enriquecimiento.ipynb",
    "M03-02-reglas-negocio.md": "M03-02-reglas-negocio.ipynb",
    "M04-01-joins.md": "M04-01-joins.ipynb",
    "M04-02-kpis.md": "M04-02-kpis.ipynb",
    "M04-03-segmentacion.md": "M04-03-segmentacion.ipynb",
    "M05-01-ranking-ventana.md": "M05-01-ranking-ventana.ipynb",
    "M05-02-acumulados.md": "M05-02-acumulados.ipynb",
    "M06-01-explain-dag.md": "M06-01-explain-dag.ipynb",
    "M06-02-cache-particionado.md": "M06-02-cache-particionado.ipynb",
    "M07-01-parquet-layout.md": "M07-01-parquet-layout.ipynb",
}

BLOCK = """
## Tu notebook

El alumno **crea su propio notebook**. No abras ni copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`]({rel}notebooks/README.md) |
| Nombre | `{name}` |
| Cómo crearlo | Explorador → carpeta `notebooks/alumno` → clic derecho → **New File…** → pega el nombre de arriba (con `.ipynb`) → Enter |
| Kernel | **Python (NovaShop)** · paleta `Notebook: Select Notebook Kernel` si no aparece |
| Organización y Celda 0 | [notebooks/README.md]({rel}notebooks/README.md) |

**Celda 0** (primera celda, idéntica en todos los labs). Ejecútala antes de cualquier otra:

```python
import sys
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
```

Después, **una celda nueva por cada paso** (`### 1`, `### 2`…). Usa `RAW`, `STAGING` y `CURATED` (no `Path("data/raw")`).

"""

MARKER = "## Tu notebook"


def rel_to_repo(md: Path) -> str:
    depth = len(md.relative_to(ROOT).parts) - 1
    return "../" * depth


def main() -> None:
    for md in LABS.rglob("M*.md"):
        if md.name not in NOTEBOOKS:
            continue
        text = md.read_text(encoding="utf-8")
        if MARKER in text:
            print("skip", md.relative_to(ROOT))
            continue
        name = NOTEBOOKS[md.name]
        block = BLOCK.format(name=name, rel=rel_to_repo(md))
        needle = "> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).\n"
        if needle not in text:
            raise SystemExit(f"no encuentro ancla en {md}")
        text = text.replace(needle, needle + "\n" + block, 1)
        md.write_text(text, encoding="utf-8")
        print("patched", md.relative_to(ROOT))


if __name__ == "__main__":
    main()
