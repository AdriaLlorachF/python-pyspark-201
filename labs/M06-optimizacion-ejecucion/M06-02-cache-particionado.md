# M06-02 — Cache y particionado

[← Página anterior](M06-01-explain-dag.md) · [Siguiente página →](../M07-persistencia-datos/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook** aquí. En clase usamos `notebooks/clase/`; no copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M06-02-cache-particionado.ipynb` |
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

Materializar un cache con una acción y ver cómo `repartition("order_month")` cambia el número de particiones.

### Prerrequisitos

- M06-01: sabes cuándo se crea un job.
- Para “dataset más grande”: un `union` del fact consigo mismo (no hace falta otro fichero).

### En qué consiste

Carga → ampliar volumen en memoria → dos `count` sin/con cache → `repartition` → validación en UI / `getNumPartitions`.

### 1 — Un fact más largo

**Acción:**

```python
from pyspark.sql.functions import col, lit

spark = get_spark("novashop-m06")
base = spark.read.parquet(str(STAGING / "fact_lines"))
# 8 copias: suficiente para notar el cache en local, sin saturar el Codespace
xl = base.withColumn("_copy", lit(-1))
for i in range(7):
    xl = xl.unionByName(base.withColumn("_copy", lit(i)))
print("particiones iniciales", xl.rdd.getNumPartitions())
```

**Por qué:** 1980 × 8 ≈ 15 840 filas. Poco para un clúster; bastante para ver Storage y shuffles en local.

**Resultado esperado:** varias particiones (> 1). Count esperado **15 840** cuando lo lances.

### 2 — Dos counts sin cache

**Acción:**

```python
import time

def timed_count(df, label):
    t0 = time.perf_counter()
    n = df.where(col("is_billable")).count()
    print(label, n, f"{time.perf_counter() - t0:.2f}s")

timed_count(xl, "1er count frío")
timed_count(xl, "2º count frío")
```

**Por qué:** cada acción **relee** el plan desde el Parquet + unions. En Spark UI: dos jobs de coste parecido.

**Resultado esperado:** dos tiempos del mismo orden. El número cobrable = 1127 × 8 = **9016**.

### 3 — Cache materializado

**Acción:**

```python
warm = xl.where(col("is_billable")).cache()
timed_count(warm, "calentamiento (materializa cache)")
timed_count(warm, "caliente")
```

**Por qué:** la primera acción llena Storage; la segunda debería leer memoria.

**Resultado esperado:** en Spark UI → Storage aparece el DataFrame. El segundo tiempo **no empeora**; en local a veces el primero ya es tan corto que la diferencia es pequeña: lo que importa es la pestaña Storage, no el cronómetro.

> [!TIP]
> `warm.unpersist()` al terminar para no dejar basura en el driver del Codespace.

### 4 — Repartition por mes

**Acción:**

```python
by_month = warm.repartition(12, col("order_month"))
print("particiones", by_month.rdd.getNumPartitions())
by_month.groupBy("order_month").count().orderBy("order_month").show()
```

**Por qué:** `repartition(12, "order_month")` hace shuffle hacia 12 particiones alineadas al mes. Es preparación para escribir (M07), no una window.

**Resultado esperado:** `particiones 12`. Doce meses en el `groupBy`.

## Comprueba tu entendimiento

**Cache perezoso**
Ejecuta solo `.cache()` y mira Storage **antes** de cualquier `count`.
→ Vacío. Luego un `count` y aparece.

## Reto

### 1 — `coalesce` vs `repartition`

Pasa a 1 partición con `coalesce(1)` y con `repartition(1)`. ¿Cuál declara shuffle en el plan?

<details>
<summary>Ver solución</summary>

`repartition(1)` siempre shufflea. `coalesce(1)` reduce particiones **sin** shuffle amplio (las fusiona). Útil para un único fichero de entrega; malo como hábito de pipeline (M07).

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Storage vacío tras `cache()` | No hubo acción | `count()` o `show()` |
| OOM en el Codespace | `union` de 50 copias + `cache` | Quédate en 8 copias; `unpersist` |
| 1980 particiones | `repartition("order_month")` sin `n` | En 3.5, `repartition(col)` usa `spark.sql.shuffle.partitions` (200 por defecto) |
