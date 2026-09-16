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

**Contexto.** NovaShop es una tienda online inventada (clientes, catálogo, pedidos, clics). Todos los labs usan esos ficheros. El Codespace trae PySpark en `local[*]`: un proceso en esta máquina, no un clúster. El pipeline del curso es leer el raw sucio, limpiar, cruzar, agregar y dejar Parquet en `data/curated/`.

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
            """# Shift+Enter. Lo que sale debajo lo genera el kernel, no es texto del markdown.
print("Hola. Esta salida la genera el kernel, no es un print de mentira.")
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

Hay un **lab extra** (M02, Mongo vivo) que necesita el contenedor `mongo` del compose. Un `git pull` en un Codespace viejo **no** lo levanta: hay que **Rebuild Container** o crear Codespace nuevo. Detalle: [infra/README.md](../../infra/README.md). El pipeline de ficheros no depende de eso.

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
        *paso(
                "1",
                "Título y propósito",
                "M00 — mi primer notebook. Voy a comprobar el kernel y las rutas del curso.",
                'print("este es mi notebook")',
                'Debajo de la celda aparece `este es mi notebook`. Si el kernel pide instalar algo, elige **Python (NovaShop)** y reintenta.',
                "Confirmas que *tu* fichero ejecuta, no el guion.",
                "Sin kernel: `F1` → Select Notebook Kernel → Python (NovaShop). Si no está: `bash .devcontainer/setup.sh`.",
            ),
        *paso(
                "2",
                "Explica el entorno (Markdown)",
                "Un notebook mezcla explicación (Markdown) y código. El estado se guarda entre celdas. Voy a localizar el repo.",
                "print('esta celda solo recuerda: el Markdown va ARRIBA, el código ABAJO')",
                "Tienes **dos** celdas nuevas: primero el Markdown del recuadro, después este `print`. El orden se lee de arriba abajo.",
                "Te acostumbras a no empezar por el código.",
            ),
        *paso(
                "3",
                "Celda de arranque (cópiala tal cual)",
                "Celda 0: localizo ROOT, RAW, STAGING y CURATED. La usaré en todos los labs.",
                CELDA_0,
                "`RAW` existe `True`. `ROOT` termina en `python-pyspark-201` (o el nombre de tu fork/Codespace).",
                "Sin esto, las rutas `data/raw` fallan cuando el notebook no está en la raíz del repo.",
                "Si `RAW` es False: `python3 scripts/generate_novashop.py` en la terminal y reejecuta la celda.",
            ),
        *paso(
                "4",
                "Lista lo que hay en raw",
                "Compruebo que NovaShop está generado: customers, products, orders, order_items, events.",
                """print(sorted(p.name for p in RAW.iterdir() if p.is_file()))""",
                "Aparecen al menos `customers.csv`, `products.json`, `orders.csv`, `order_items.csv`, `events.jsonl`.",
                "Antes de Spark, confirmas que los ficheros existen.",
            ),
        md(
            comprueba(
                """- Tu fichero se llama `notebooks/trabajo/M00-01-mi-primer-notebook.ipynb`.
- Hay celdas **Markdown** intercaladas (no solo código).
- `RAW` existe y listaste los ficheros.
- **Run All** sigue funcionando de arriba abajo."""
            )
        ),
        *reto(
                "Una frase tuya",
                "Añade al final una celda Markdown (mínimo 3 líneas) que explique, con tus palabras, la diferencia entre este guion y *tu* notebook. No copies este párrafo.",
                "No hay código que pegar: es solo Markdown. Si el formador lo pide, es lo que se mira primero.",
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
        md(siguiente("03-python-recordatorio.ipynb", "Python de bolsillo")),
    ]


def python_sheet() -> list:
    return [
        md(
            teoria_head(
                "Python de bolsillo",
                """Hoja de recordatorio. **No es un curso de Python.** Es lo mínimo que vas a leer y escribir en los labs de PySpark.

Ejecuta las celdas **aquí**. Si ya te suena, recorre en diagonal. Si no, quédate hasta `None` y los `import`.

Pandas y Spark se ven en M01. Aquí no hay DataFrames.""",
                "02-lab-primer-notebook.ipynb",
                "../M01-fundamentos-entorno/01-teoria.ipynb",
            )
        ),
        md(
            """## Variables y tipos simples

Un nombre guarda un valor. No declaras el tipo: Python lo ve en lo que asignas.

Al ejecutar: `str`, `int`, `float`, `bool`."""
        ),
        code(
            """# = asigna. == compara (más abajo).
pedido = "O90001"       # str  — texto
unidades = 3            # int  — entero
precio = 19.90          # float — decimal
cobrado = True          # bool — True / False (mayúscula)

print(pedido, type(pedido))
print(unidades, type(unidades))
print(precio, type(precio))
print(cobrado, type(cobrado))"""
        ),
        md(
            """## Tipos compuestos

Un compuesto **agrupa** varios valores. El tipo del objeto entero es `list`, `tuple`, `dict` o `set`; lo de dentro puede ser de otro tipo (una lista de `str`, un dict de `str` → `float`).

| Tipo | Se escribe | Qué es | ¿Se puede cambiar? |
|------|------------|--------|---------------------|
| `list` | `[a, b, c]` | Secuencia ordenada | Sí (`append`, `lista[0] = …`) |
| `tuple` | `(a, b)` o `a, b` | Secuencia ordenada, fija | No (inmutable) |
| `dict` | `{"k": v}` | Clave → valor | Sí |
| `set` | `{a, b}` | Conjunto **sin duplicados** y sin orden | Sí |

Al ejecutar, `type(...)` debe decir exactamente esos cuatro nombres. Fíjate: `{1, 2}` es un `set`; `{"a": 1}` es un `dict` (lleva `:`)."""
        ),
        code(
            """estados = ["paid", "cancelled", "pending"]          # list
punto = ("WEB", "app")                                # tuple
pedido = {"order_id": "O1", "amount": 10.0}           # dict
canales = {"web", "app", "store", "web"}              # set (el "web" repetido se pierde)

print("list ", type(estados), estados)
print("tuple", type(punto), punto)
print("dict ", type(pedido), pedido)
print("set  ", type(canales), canales)  # un solo "web"

print("primer estado:", estados[0])
print("amount:", pedido["amount"])
print("cuántos estados:", len(estados))
print("app está en la tupla?", "app" in punto)"""
        ),
        md(
            """## `print`, comentarios, f-strings

`print` escribe debajo de la celda. `#` es un comentario (Python no lo ejecuta).

Un f-string mete valores dentro del texto: `f"...{nombre}..."`. Lo usarás para mensajes, no para armar SQL a mano."""
        ),
        code(
            """order_id = "O1"
gmv = 49.9
# Esto no corre: es una nota para ti
print("pedido", order_id)
print(f"el pedido {order_id} facturó {gmv}")"""
        ),
        md(
            """## `None` (no hay valor)

En Spark verás nulos. En Python el “no hay nada” se llama `None`. Se compara con `is None`, no con `== None` (aunque a veces funcione)."""
        ),
        code(
            """pais = None
print(pais is None)
print(pais == "")  # False: vacío y None no son lo mismo"""
        ),
        md(
            """## Comparar y decidir (`if`)

`==` `!=` `<` `>` `<=` `>=`. Varias condiciones: `and`, `or`, `not`.

La indentación (espacios a la izquierda) **es** el bloque. Sin ella, Python falla."""
        ),
        code(
            """status = "paid"
discount = 1.5

if status == "paid" and discount > 1:
    print("cobrado, pero el descuento está sucio")
elif status == "cancelled":
    print("no entra en el GMV cobrable")
else:
    print("otro estado:", status)"""
        ),
        md(
            """## Listas (usarlas)

Índice desde **0**. `append` añade al final. En Spark casi no recorres filas con `for`; sí montas listas cortas de `Row(...)`."""
        ),
        code(
            """estados = ["paid", "cancelled", "pending"]
print("primero:", estados[0])
print("último:", estados[-1])
print("trozo [0:2]:", estados[0:2])  # paid, cancelled (sin el 2)

estados.append("refunded")
for s in estados:
    print("estado:", s)"""
        ),
        md(
            """## Tuplas

Como una lista, pero **no** le puedes hacer `append` ni `punto[0] = "x"`. Aparecen al devolver dos valores (`a, b = …`) y en APIs (`isin(("web", "app"))` en Spark es “¿está en esta tupla?”)."""
        ),
        code(
            """punto = ("WEB", "app")
print(punto[0], len(punto))
a, b = punto  # desempaquetar
print(a, b)
# punto.append("store")  # AttributeError si lo descomentas"""
        ),
        md(
            """## Diccionarios (usarlos)

Clave → valor. En Pandas una fila parece un dict. En Spark, `Row(campo=valor)` es la misma idea."""
        ),
        code(
            """pedido = {"order_id": "O1", "status": "paid", "amount": 10.0}
print(pedido["order_id"])
print(list(pedido.keys()))
pedido["channel"] = "web"  # alta o pisa

for clave, valor in pedido.items():
    print(clave, "=", valor)"""
        ),
        md(
            """## Sets

Útiles para “valores únicos” y para `in` rápido. No hay `canales[0]`: no hay posición."""
        ),
        code(
            """canales = {"web", "app", "store", "web"}
print(canales)
canales.add("other")
print("marketplace" in canales)  # False
print(canales & {"web", "marketplace"})  # intersección: {'web'}"""
        ),
        md(
            """## Texto: lo que más ensucia un CSV

Mayúsculas, espacios, vacío. En los labs verás `lower`, `trim` *en Spark*; aquí es el equivalente Python para que sepas qué significa."""
        ),
        code(
            """canal = "  WEB "
print(canal.strip())          # quita espacios de los bordes
print(canal.strip().lower())  # web
print("".strip() == "")       # texto vacío
print(bool("WEB"), bool(""))  # True, False"""
        ),
        md(
            """## Funciones

`def` nombra un trozo reutilizable. En el curso casi todo será API de Spark (`filter`, `count`). Una función tuya aparece poco; sí verás `from x import y`."""
        ),
        code(
            """def es_cobrable(status):
    return status == "paid"

print(es_cobrable("paid"))
print(es_cobrable("pending"))"""
        ),
        md(
            """## `import`: usar código de otro sitio

`import pandas as pd` carga el módulo. `from pyspark.sql.functions import col` trae **un** nombre.

La Celda 0 de los labs hace esto con `paths` y `session` del repo. No hace falta que la inventes: la pegas."""
        ),
        code(
            """import math
from pathlib import Path

print(math.sqrt(9))
print(Path("data") / "raw")  # unir trozos de ruta; en labs usa RAW, no esto"""
        ),
        md(
            """## Lo que **no** hagas en PySpark

Un `for` sobre todas las filas (`for row in df.collect():`) baja todo al driver y se come la RAM. En este curso: columnas (`withColumn`, `filter`, `groupBy`), no bucles de filas.

`collect()` y `toPandas()` solo con `limit(...)` para mirar.

## Mini chequeo

Ejecuta la celda. Debes ver `True` cuatro veces. Si no, repasa tipos simples, `list` y `dict`."""
        ),
        code(
            """ok_tipos = type(3.14) is float
ok_none = None is None
ok_list = type(["paid", "pending"]) is list
ok_dict = {"a": 1}["a"] == 1
print(ok_tipos, ok_none, ok_list, ok_dict)"""
        ),
        md(
            """**Siguiente:** [M01 — teoría](../M01-fundamentos-entorno/01-teoria.ipynb) (Pandas vs Spark, mismas cinco filas)."""
        ),
    ]
