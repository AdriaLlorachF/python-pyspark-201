# Notebooks del curso

Hay **dos carpetas** y no se mezclan.

```text
notebooks/
├── README.md                 ← este fichero
├── alumno/                   ← TÚ creas aquí un .ipynb por laboratorio
│   └── (vacía al empezar)
└── validacion/               ← referencia ya resuelta (formador / QA)
    ├── 00-entorno.ipynb
    ├── M01-01-sesion-spark.ipynb
    ├── …
    └── 99-pipeline-completo.ipynb
```

**No copies** `notebooks/validacion/` a tu carpeta. El aprendizaje es escribir las celdas siguiendo el markdown de `labs/`.

## Qué crea el alumno (nombres fijos)

| Laboratorio | Carpeta | Nombre exacto del fichero |
|-------------|---------|---------------------------|
| M01-01 | `notebooks/alumno/` | `M01-01-sesion-spark.ipynb` |
| M02-01 | `notebooks/alumno/` | `M02-01-ingesta-csv-json.ipynb` |
| M02-02 | `notebooks/alumno/` | `M02-02-schema-tipos.ipynb` |
| M02-03 | `notebooks/alumno/` | `M02-03-calidad-limpieza.ipynb` |
| M03-01 | `notebooks/alumno/` | `M03-01-enriquecimiento.ipynb` |
| M03-02 | `notebooks/alumno/` | `M03-02-reglas-negocio.ipynb` |
| M04-01 | `notebooks/alumno/` | `M04-01-joins.ipynb` |
| M04-02 | `notebooks/alumno/` | `M04-02-kpis.ipynb` |
| M04-03 | `notebooks/alumno/` | `M04-03-segmentacion.ipynb` |
| M05-01 | `notebooks/alumno/` | `M05-01-ranking-ventana.ipynb` |
| M05-02 | `notebooks/alumno/` | `M05-02-acumulados.ipynb` |
| M06-01 | `notebooks/alumno/` | `M06-01-explain-dag.ipynb` |
| M06-02 | `notebooks/alumno/` | `M06-02-cache-particionado.ipynb` |
| M07-01 | `notebooks/alumno/` | `M07-01-parquet-layout.ipynb` |

Un laboratorio = **un** notebook. No reutilices `sandbox`. No pongas espacios ni tildes en el nombre.

## Cómo crear cada notebook (Codespace)

1. En el explorador de la izquierda, abre la carpeta `notebooks/alumno`.
2. Clic derecho sobre `alumno` → **New File…**
3. Escribe **exactamente** el nombre de la tabla (incluido `.ipynb`) y Enter.
4. Arriba a la derecha, kernel: elige **Python (NovaShop)**.
   - Si no aparece: paleta (`F1`) → `Python: Select Interpreter` → `/usr/local/bin/python`.
   - Luego paleta → `Notebook: Select Notebook Kernel` → **Python (NovaShop)** o `Python 3.11.x` **del mismo** `/usr/local/bin/python`.
5. Si el kernel pide instalar `ipykernel`: acepta, o en la terminal del Codespace:

```bash
python3 -m pip install --user -r requirements.txt
python3 -m ipykernel install --user --name novashop --display-name "Python (NovaShop)"
```

6. Primera celda = **Celda 0** (abajo). Ejecútala con <kbd>Shift</kbd>+<kbd>Enter</kbd>.
7. Añade **una celda nueva por cada paso** del lab (`### 1`, `### 2`…). Pega el bloque **Acción** del markdown.
8. Compara la salida con **Resultado esperado**. Luego haz **Comprueba** y el **Reto**.

## Celda 0 (idéntica en todos tus notebooks)

Cópiala tal cual. Localiza el repo aunque el notebook viva en `notebooks/alumno/`.

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
print("STAGING", STAGING)
print("CURATED", CURATED)
```

Salida esperada: `existe: True` y `ROOT` termina en `python-pyspark-201`.

A partir de ahí, en **otra** celda:

```python
spark = get_spark("novashop-m01")   # cambia m01 por el lab (m02, m03…)
```

Usa siempre `RAW`, `STAGING` y `CURATED` de la celda 0. **No** escribas `Path("data/raw")`: si el notebook no está en la raíz, falla.

## Orden y dependencias

| Notebooks | Leen | Escriben |
|-----------|------|----------|
| M01-01 | nada (filas en memoria) | — |
| M02-01, M02-02 | `data/raw/` | — |
| M02-03 | raw tipado | `data/staging/*_clean` |
| M03-01 | staging clean | — |
| M03-02 | líneas de M03-01 | `data/staging/fact_lines` |
| M04-* | fact + customers | `data/staging/customer_gmv` (M04-03) |
| M05-* | fact + customer_gmv | — |
| M06-* | fact | — |
| M07-01 | fact + dims | `data/curated/sales_analytics` |

Si pierdes el staging: reabre `M02-03` y `M03-02` y vuelve a ejecutar todas las celdas. O en la terminal:

```bash
python3 scripts/run_pipeline.py
```

## Kernel y Spark UI

| Qué | Dónde |
|-----|--------|
| Kernel | **Python (NovaShop)** |
| Spark UI | puerto **4040** → pestaña Ports del Codespace |
| Una sola sesión | `get_spark(...)` usa `getOrCreate()`; no crees otra `SparkSession()` |

## Validación (formador)

```bash
python3 scripts/run_pipeline.py
python3 scripts/execute_notebooks.py
```
