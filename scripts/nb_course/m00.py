"""M00 — entorno, fork, Codespace, qué es un notebook."""
from __future__ import annotations

from .common import (
    CELDA_0,
    REPO,
    TRABAJO,
    code,
    comprueba,
    errores,
    lab_abre,
    md,
    paso,
    reto,
    siguiente,
    teoria_head,
)


def teoria() -> list:
    return [
        md(
            teoria_head(
                "M00 — Tu entorno y los notebooks",
                """Este es el **Lab 0**. Aquí no hay Spark todavía: aprendes a trabajar como vas a trabajar todo el curso.

En clase abrimos este fichero juntos. Tú ejecutas las mismas celdas.""",
                "../../README.md",
                "02-lab-primer-notebook.ipynb",
            )
        ),
        md(
            """## Qué es un notebook

Un `.ipynb` es un cuaderno: **celdas** una debajo de otra. Dos tipos:

| Celda | Para qué |
|-------|----------|
| **Markdown** | Explicar. Títulos, listas, porqués. Se *lee*. |
| **Código** | Python. Se *ejecuta*. Debajo aparece la salida. |

El estado se acumula: si en una celda haces `spark = …`, en la siguiente `spark` ya existe. Si cambias una celda de arriba, **vuelve a ejecutar** desde ahí (o **Run All**).

Atajos que vas a usar:

- `Shift+Enter` — ejecuta la celda y pasa a la siguiente.
- `Esc` luego `A` / `B` — celda nueva arriba / abajo.
- `Esc` luego `M` — esta celda es Markdown.
- `Esc` luego `Y` — esta celda es código.
- `Esc` luego `DD` — borra la celda.

Prueba ahora: la siguiente celda es código. Ejecútala."""
        ),
        code(
            """print("Hola. Esta salida la genera el kernel, no es un print de mentira.")
print("2 + 2 =", 2 + 2)"""
        ),
        md(
            """Si viste las dos líneas debajo de la celda, el kernel responde. Si viste un error de kernel: `F1` → `Notebook: Select Notebook Kernel` → **Python (NovaShop)**.

## Buena práctica (todo el curso)

Un notebook no es un script con comentarios. **Antes de cada bloque de código, una celda Markdown** que diga qué vas a hacer y por qué.

Mal: diez celdas de código seguidas y ni un título.

Bien: `# Cargo pedidos` → código → `## Compruebo el count` → código.

En los labs te pediré esas celdas Markdown a propósito. Escríbelas con tus palabras; no copies solo el código."""
        ),
        md(
            f"""## El repo y el fork

El material vive en GitHub: [{REPO}]({REPO}).

**Haz un fork** a tu usuario (botón **Fork** arriba a la derecha). Así:

- tus notebooks de trabajo quedan en *tu* copia;
- puedes commitear sin tocar el repo del curso;
- el Codespace sale de *tu* fork.

Si el formador te da otro flujo (org, classroom), úsalo. La idea es la misma: trabajas sobre una copia tuya.

## Codespace

El curso está pensado para **GitHub Codespaces** (Python 3.11, Java 17, PySpark, kernel NovaShop).

1. En **tu fork**, pestaña **Code** → **Codespaces** → **Create codespace on main**.
2. Espera a que termine el setup (barra o terminal: Java, pip, dataset). La primera vez tarda.
3. Cuando el explorador muestre `notebooks/`, `data/`, `labs/`, ya puedes abrir este fichero.

Spark UI: pestaña **Ports** → puerto **4040**.

Si el kernel no aparece o pide `ipykernel`:

```bash
bash .devcontainer/setup.sh
```

Luego vuelve a elegir **Python (NovaShop)**.

¿Trabajas en local? Python 3.11 + JDK **17** y el mismo `setup.sh`. `java -version` no puede ser 21/25.

## Cómo ejecutar un notebook de teoría

1. Ábrelo desde el explorador (doble clic en el `.ipynb`).
2. Elige el kernel **Python (NovaShop)**.
3. Sitúate en la primera celda → `Shift+Enter` → siguiente → `Shift+Enter`.
4. Si algo queda a medias, **Run All**.

La **teoría** se ejecuta *en el fichero del curso* (este). El **lab** es otro fichero: el guion te dice cómo **crear el tuyo** en `{TRABAJO}/`."""
        ),
        md(
            """## Tres sitios (no los mezcles)

| Dónde | Qué haces |
|-------|-----------|
| `notebooks/M0x/01-teoria.ipynb` | Lees y ejecutas con la clase. |
| `notebooks/M0x/0N-lab-….ipynb` | Lees el guion. **No** lo rellenas. |
| `notebooks/trabajo/` | **Creas tu** `.ipynb` y trabajas ahí. |

`notebooks/_qa/` no lo abras: es una batería interna del repo.

## Arranque que usarás en todos los labs

La celda de abajo localiza el repo aunque tu notebook esté en `trabajo/`. Ejecútala aquí una vez para ver que el entorno responde."""
        ),
        code(CELDA_0),
        md(
            """Debes ver `ROOT` apuntando a este curso y `RAW … existe: True`. Si `RAW` es `False`, en la terminal:

```bash
python3 scripts/generate_novashop.py
```

**Siguiente:** el lab de este módulo — creas tu primer notebook."""
        ),
    ]


def lab() -> list:
    return [
        md(
            lab_abre(
                "M00-01",
                "Tu primer notebook",
                "M00-01-mi-primer-notebook.ipynb",
                "Crear un notebook tuyo, escribir celdas Markdown y de código, elegir kernel y comprobar que el entorno ve el dataset.",
                "01-teoria.ipynb",
                "../M01-fundamentos-entorno/01-teoria.ipynb",
            )
        ),
        md(
            paso(
                "1",
                "Título y propósito",
                "M00 — mi primer notebook. Voy a comprobar el kernel y las rutas del curso.",
                'print("este es mi notebook")',
                'Debajo de la celda aparece `este es mi notebook`. Si el kernel pide instalar algo, elige **Python (NovaShop)** y reintenta.',
                "Confirmas que *tu* fichero ejecuta, no el guion.",
                "Sin kernel: `F1` → Select Notebook Kernel → Python (NovaShop). Si no está: `bash .devcontainer/setup.sh`.",
            )
        ),
        md(
            paso(
                "2",
                "Explica el entorno (Markdown)",
                "Un notebook mezcla explicación (Markdown) y código. El estado se guarda entre celdas. Voy a localizar el repo.",
                "print('esta celda solo recuerda: el Markdown va ARRIBA, el código ABAJO')",
                "Tienes **dos** celdas nuevas: primero el Markdown del recuadro, después este `print`. El orden se lee de arriba abajo.",
                "Te acostumbras a no empezar por el código.",
            )
        ),
        md(
            paso(
                "3",
                "Celda de arranque (cópiala tal cual)",
                "Celda 0: localizo ROOT, RAW, STAGING y CURATED. La usaré en todos los labs.",
                CELDA_0,
                "`RAW` existe `True`. `ROOT` termina en `python-pyspark-201` (o el nombre de tu fork/Codespace).",
                "Sin esto, las rutas `data/raw` fallan cuando el notebook no está en la raíz del repo.",
                "Si `RAW` es False: `python3 scripts/generate_novashop.py` en la terminal y reejecuta la celda.",
            )
        ),
        md(
            paso(
                "4",
                "Lista lo que hay en raw",
                "Compruebo que NovaShop está generado: customers, products, orders, order_items, events.",
                """print(sorted(p.name for p in RAW.iterdir() if p.is_file()))""",
                "Aparecen al menos `customers.csv`, `products.json`, `orders.csv`, `order_items.csv`, `events.jsonl`.",
                "Antes de Spark, confirmas que los ficheros existen.",
            )
        ),
        md(
            comprueba(
                """- Tu fichero se llama `notebooks/trabajo/M00-01-mi-primer-notebook.ipynb`.
- Hay celdas **Markdown** intercaladas (no solo código).
- `RAW` existe y listaste los ficheros.
- **Run All** sigue funcionando de arriba abajo."""
            )
        ),
        md(
            reto(
                "Una frase tuya",
                "Añade al final una celda Markdown (mínimo 3 líneas) que explique, con tus palabras, la diferencia entre este guion y *tu* notebook. No copies este párrafo.",
                "No hay código que pegar: es solo Markdown. Si el formador lo pide, es lo que se mira primero.",
            )
        ),
        md(
            errores(
                [
                    ("No aparece Python (NovaShop)", "Setup a medias", "`bash .devcontainer/setup.sh` y reelige kernel"),
                    ("`RAW` False", "Dataset no generado", "`python3 scripts/generate_novashop.py`"),
                    ("Editaste este guion", "Trabajaste en el fichero del curso", "Crea el de `trabajo/` y deja el guion en solo lectura"),
                    ("El print no sale", "No ejecutaste la celda", "`Shift+Enter` en *tu* notebook"),
                ]
            )
        ),
        md(siguiente("../M01-fundamentos-entorno/01-teoria.ipynb", "M01 — teoría")),
    ]
