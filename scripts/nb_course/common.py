"""Piezas comunes de los notebooks del curso (tuteo, labs paso a paso)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from nbutil import CELDA_0, code, md, write_notebook  # noqa: E402

NB = ROOT / "notebooks"
TRABAJO = "notebooks/trabajo"

REPO = "https://github.com/my-it-labs/python-pyspark-201"


def fence(src: str) -> str:
    return "```python\n" + src.strip() + "\n```"


def nav(prev: str, nxt: str) -> str:
    return f"[← Anterior]({prev}) · [Siguiente →]({nxt})"


def lab_abre(code_id: str, title: str, filename: str, goal: str, prev: str, nxt: str) -> str:
    return f"""# {code_id} — {title}

{nav(prev, nxt)}

Este fichero es el **guion**. No lo rellenes aquí: **crea tu propio notebook** y ve construyéndolo celda a celda.

## Qué vas a hacer

{goal}

## 0 — Crea tu notebook

1. En el explorador, abre la carpeta `{TRABAJO}/`.
2. Clic derecho → **New File…**
3. Nombre exacto: `{filename}` (incluye `.ipynb`).
4. Ábrelo. Arriba a la derecha (o `F1` → `Notebook: Select Notebook Kernel`) elige **Python (NovaShop)**.
5. Deja **este** guion a un lado (pestaña) y escribe **solo** en el tuyo.

## Cómo copiar desde este guion

El texto y el código que tienes que llevarte van en **celdas propias** (no dentro de un párrafo). Así se copian bien:

1. Clic en la celda (código o el Markdown corto del paso).
2. `Ctrl+A` (o `Cmd+A`) y `Ctrl+C` / `Cmd+C`. En Codespace también sale el icono de copiar en la barra de la celda.
3. Pega en **tu** notebook. La celda de código pégala como código; la de texto, como Markdown (`Esc` luego `M`).

No copies desde un recuadro gris a medias: usa la celda entera.

## Cómo organizar *tu* notebook (siempre)

En cada paso creas **dos celdas**, en este orden:

1. **Markdown** — qué vas a hacer y por qué, con tus palabras. Puedes partir del texto corto del paso.
2. **Código** — pegas la celda de código, la **ejecutas** (`Shift+Enter`), **miras** la salida y, si no cuadra, la **mejoras**.

No dejes un muro de código sin explicación. Un notebook se lee de arriba abajo, como un cuaderno.

> Kernel **Python (NovaShop)**. Si no aparece: terminal → `bash .devcontainer/setup.sh` → vuelve a elegir kernel.
"""


def paso(
    n: str,
    title: str,
    md_hint: str,
    src: str,
    check: str,
    why: str,
    if_fail: str = "",
    extra: str = "",
) -> list:
    """Instrucciones + celda de texto copiable + celda de código copiable."""
    fail = f"\n\n**Si no sale.** {if_fail}" if if_fail else ""
    more = f"\n\n{extra}" if extra else ""
    return [
        md(
            f"""### Paso {n} — {title}

**1.** En *tu* notebook, crea una celda Markdown. Copia **la celda de texto** que viene ahora (clic → `Ctrl+A` → `Ctrl+C`) y pégala. Luego déjala en tus palabras si quieres.

**2.** Crea debajo una celda de código. Copia **la celda de código** siguiente entera (igual: clic → seleccionar todo → copiar) y pégala.

**3.** Ejecuta en *tu* notebook (`Shift+Enter`). Espera a que deje de verse `[*]`.

**4. Comprueba.** {check}

**Por qué este paso.** {why}{fail}{more}"""
        ),
        md(md_hint.strip()),
        code(src.strip()),
    ]


def comprueba(text: str) -> str:
    return f"""## Comprueba

Antes de dar el lab por cerrado, vuelve a ejecutar de arriba abajo (**Run All**) y verifica:

{text}
"""


def reto(title: str, brief: str, solucion: str) -> list:
    """Enunciado + solución copiable (celda de código si hay un fence python)."""
    cells = [
        md(
            f"""## Mejora — {title}

{brief}

Si te atasca, copia la celda siguiente (clic → `Ctrl+A` → `Ctrl+C`) y pégala en *tu* notebook."""
        )
    ]
    text = solucion.strip()
    if text.startswith("```python"):
        body = text.removeprefix("```python").removesuffix("```").strip()
        cells.append(code(body))
    elif text.startswith("```"):
        body = text.split("\n", 1)[-1]
        if body.endswith("```"):
            body = body[: -3].strip()
        cells.append(md(body))
    else:
        cells.append(md(text))
    return cells


def errores(rows: list[tuple[str, str, str]]) -> str:
    lines = [
        "## Si algo falla",
        "",
        "| Qué ves | Suele ser | Qué haces |",
        "|---------|-----------|-----------|",
    ]
    for a, b, c in rows:
        lines.append(f"| {a} | {b} | {c} |")
    return "\n".join(lines)


def siguiente(path: str, label: str) -> str:
    return f"""## Siguiente

Cuando hayas **comprobado** y (si quieres) **mejorado**, abre [{label}]({path}).
"""


def teoria_head(title: str, intro: str, prev: str, nxt: str) -> str:
    return f"""# {title}

{nav(prev, nxt)}

{intro}

Ejecuta las celdas **aquí**, en este mismo fichero. No lo copies a otro sitio.

Kernel: **Python (NovaShop)**.
"""


def boot_cells(app: str) -> list:
    return [
        md("## Arranque\n\nEjecuta estas dos celdas. Localizan el repo y dejan una `SparkSession` lista."),
        code(CELDA_0),
        code(f"spark = get_spark('{app}')\nprint(spark.version, spark.sparkContext.master)"),
    ]
