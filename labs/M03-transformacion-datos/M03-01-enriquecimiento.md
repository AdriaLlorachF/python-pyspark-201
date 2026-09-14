# M03-01 — Enriquecimiento

[← Página anterior](README.md) · [Siguiente página →](M03-02-reglas-negocio.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook**. No abras ni copies `notebooks/validacion/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M03-01-enriquecimiento.ipynb` |
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

Añadir `gmv_line` y `order_month` al cruce líneas ⋈ pedidos y detectar GMV negativo (descuento sucio).

### Prerrequisitos

- Staging de M02-03 en `data/staging/`.

### En qué consiste

Carga de staging → join mínimo pedido–línea → columnas nuevas → validación (incluye el “fallo” de negocio).

### 1 — Leer staging

**Acción:**

```python
spark = get_spark("novashop-m03")
orders = spark.read.parquet(str(STAGING / "orders_clean"))
items = spark.read.parquet(str(STAGING / "order_items_clean"))
print(orders.count(), items.count())
```

**Por qué:** el schema ya viaja en Parquet; no re-inferimos.

**Resultado esperado:** `788 2010`.

### 2 — Cruzar para tener fecha

**Acción:**

```python
from pyspark.sql.functions import col

lines = items.join(orders, "order_id", "inner")
print(lines.count())
```

**Por qué:** `order_month` vive en la cabecera. Inner: las líneas de los 12 pedidos sin cliente (ya fuera del staging) no entran.

**Resultado esperado:** **1980** filas (2010 − 30 líneas de pedidos descartados).

### 3 — GMV y mes

**Acción:**

```python
from pyspark.sql.functions import date_format

lines = (
    lines.withColumn(
        "gmv_line",
        col("qty") * col("unit_price") * (1 - col("discount")),
    ).withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
)
lines.select("order_id", "qty", "unit_price", "discount", "gmv_line", "order_month").show(5)
print("gmv nulos", lines.where(col("gmv_line").isNull()).count())
print("gmv < 0", lines.where(col("gmv_line") < 0).count())
```

**Por qué:** la fórmula de negocio es una columna. Si el descuento crudo es `1.50`, el GMV **sale negativo**: no es un bug de Spark, es suciedad que M03-02 tapa.

**Resultado esperado:** `gmv nulos 0` · `gmv < 0` **13**. `order_month` tipo string `2024-01` … `2024-12`.

## Comprueba tu entendimiento

**Mes y nulos**
`order_month` en formato `yyyy-MM` y nulos de `gmv_line` en las 1980 filas.
→ Cero nulos; 12 meses de 2024; **13** GMV negativos.

## Reto

### 1 — Pedido de alto valor

Crea `is_high_value` = `gmv_line >= 500` y cuenta los `true`.

<details>
<summary>Ver solución</summary>

```python
from pyspark.sql.functions import when

lines.withColumn("is_high_value", col("gmv_line") >= 500).where("is_high_value").count()
# 506 (con el GMV aún sin capar descuento)
```

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| 2010 tras el join | Hiciste `left` desde items | Inner contra `orders_clean` → 1980 |
| `gmv_line` string raro | No casteaste `unit_price` en M02 | Relee el staging; debe ser decimal/double |
| `order_month` nulo | `order_ts` no parseó | Vuelve a M02-02 (`coalesce` de dos formatos) |
