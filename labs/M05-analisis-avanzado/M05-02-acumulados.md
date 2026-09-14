# M05-02 — Acumulados por entidad

[← Página anterior](M05-01-ranking-ventana.md) · [Siguiente página →](../M06-optimizacion-ejecucion/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook** aquí. En clase usamos `notebooks/clase/`; no copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M05-02-acumulados.ipynb` |
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

Numerar los pedidos de cada cliente y calcular el GMV cobrable acumulado en el tiempo.

### Prerrequisitos

- Fact + clientes. Grano **pedido** (si acumulas líneas, el “2.º pedido” se rompe).

### En qué consiste

Carga → agregar a pedido → window por cliente ordenada por fecha → validación.

### 1 — Grano pedido

**Acción:**

```python
from pyspark.sql.functions import col, min as fmin, sum as fsum, row_number

orders_gmv = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "order_id")
    .agg(
        fmin("order_ts").alias("order_ts"),
        fsum("gmv_line").alias("gmv"),
    )
)
print(orders_gmv.count())
```

**Por qué:** un pedido con 3 líneas no es 3 visitas.

**Resultado esperado:** **469** pedidos cobrables con cliente (el mismo count que el KPI de M04-02).

### 2 — Número de pedido y acumulado

**Acción:**

```python
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_ts")
hist = (
    orders_gmv.withColumn("order_n", row_number().over(w))
    .withColumn("gmv_running", fsum("gmv").over(w))
)
hist.orderBy("customer_id", "order_n").show(12)
```

**Por qué:** la misma window sirve para el índice y para el `sum` acumulado. El `orderBy` de la window **es** el tiempo.

**Resultado esperado:** `order_n` 1, 2, 3… por cliente; `gmv_running` no decrece dentro del mismo `customer_id`.

### 3 — Primera compra vs repetición

**Acción:**

```python
hist.groupBy((col("order_n") == 1).alias("is_first")).count().show()
```

**Por qué:** `order_n == 1` es la definición operativa de “nuevos” en este curso.

**Resultado esperado:** ~211 primeras compras (un `true` por cliente con paid) y el resto repeticiones.

## Comprueba tu entendimiento

**Monotonía**
En un cliente con `order_n` ≥ 2, `gmv_running` de la fila 2 ≥ fila 1.
→ Si baja, el `orderBy` de la window no es `order_ts`.

## Reto

### 1 — Pedidos hasta superar 1 000 €

Quédate, por cliente, con la primera fila donde `gmv_running >= 1000` (o ninguna).

<details>
<summary>Ver solución</summary>

```python
from pyspark.sql.functions import row_number

w2 = Window.partitionBy("customer_id").orderBy("order_ts")
crossed = hist.where(col("gmv_running") >= 1000)
first_cross = crossed.withColumn("rn", row_number().over(w2)).where(col("rn") == 1)
first_cross.select("customer_id", "order_n", "gmv_running").show()
```

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| `gmv_running` igual en todas las filas del cliente | Window sin `orderBy` (rango unbounded mal leído) | `partitionBy` + `orderBy("order_ts")` |
| 1122 “pedidos” | No agregaste a `order_id` | Paso 1 |
| Acumulado a nivel empresa | Falta `partitionBy` | Añádelo |
