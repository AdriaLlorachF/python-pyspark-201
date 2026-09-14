# M06-01 — Explain y DAG

[← Página anterior](README.md) · [Siguiente página →](M06-02-cache-particionado.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook**. No abras ni copies `notebooks/validacion/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M06-01-explain-dag.ipynb` |
| Cómo crearlo | Explorador → carpeta `notebooks/alumno` → clic derecho → **New File…** → pega el nombre de arriba (con `.ipynb`) → Enter |
| Kernel | **Python (NovaShop)** · paleta `Notebook: Select Notebook Kernel` si no aparece |
| Organización y Celda 0 | [notebooks/README.md](../../notebooks/README.md) |

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


### Objetivo

Demostrar que cinco transformaciones no lanzan job, y señalar scan + filtro en el plan formateado.

### Prerrequisitos

- `data/staging/fact_lines`.
- Spark UI en el puerto **4040** del Codespace (pestaña Ports).

### En qué consiste

Carga (solo construir el plan) → acción → validación en UI y `explain`.

### 1 — Plan sin ejecutar

**Acción:**

```python
from pyspark.sql.functions import col

spark = get_spark("novashop-m06")
# Anota el último Job Id que ves ahora en Spark UI (puede ser 0 o el de labs anteriores).

planned = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .where(col("is_billable"))
    .where(col("gmv_line") > 0)
    .where(col("channel_norm").isin("web", "app"))
    .select("order_id", "customer_id", "gmv_line", "order_month")
)
print(planned)  # no es una acción
```

**Por qué:** imprimir el objeto DataFrame no dispara jobs.

**Resultado esperado:** el Job Id **más alto** de Spark UI no cambia al ejecutar esta celda.

### 2 — Una acción, un DAG

**Acción:**

```python
print(planned.count())
```

**Por qué:** `count` obliga a recorrer las particiones.

**Resultado esperado:** un job nuevo. En la pestaña Jobs, el DAG muestra al menos un stage. El count es el de líneas cobrables web/app con GMV > 0 (varios cientos).

### 3 — Leer el plan

**Acción:**

```python
planned.explain("formatted")
```

**Por qué:** el plan es el mapa. Buscas *FileScan parquet* (o `Scan`) y *Filter*.

**Resultado esperado:** aparece el path `fact_lines` y predicados `is_billable` / `gmv_line` / `channel_norm`. No hace falta traducir cada operador Catalyst.

## Comprueba tu entendimiento

**Lazy de verdad**
Vuelve a encadenar un `.where(...)` extra **sin** `count` y mira Jobs.
→ No hay job nuevo.

## Reto

### 1 — `explain(True)` vs `formatted`

Compara `explain(True)` (plan lógico + físico) con `explain("formatted")`. ¿Dónde se ve el filtro empujado al scan?

<details>
<summary>Ver solución</summary>

En el físico / formatted, el `PushedFilters` o el `Filter` junto al `FileScan` indica *predicate pushdown*. Si el filtro no aparece, lo aplicaste *después* de un `select` que ya tiró la columna.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| UI vacía / 404 | Puerto 4040 no reenviado | Pestaña Ports del Codespace → 4040 |
| Cada celda crea un job | Tienes un `.show()` de debug | Comenta los `show` mientras mides |
| Dos sesiones | `SparkSession()` extra | Solo `get_spark()` |
