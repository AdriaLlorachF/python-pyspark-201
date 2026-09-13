# M07-01 — Parquet y layout analítico

[← Página anterior](README.md) · [Siguiente página →](../../README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Publicar `data/curated/sales_analytics` en Parquet particionado por mes y demostrar que un filtro de mes no lee el año entero.

### Prerrequisitos

- Staging completo (M02–M04). Si falta, regenera raw y rehaz M02-03 → M03-02 → inner clientes.

### En qué consiste

Carga del fact cobrable → proyección de columnas analíticas → escritura particionada → validación de carpetas, counts y plan.

### 1 — Dataset curated

**Acción:**

```python
import sys
from pathlib import Path
from pyspark.sql.functions import col

sys.path.append(str(Path("labs/_shared").resolve()))
from session import get_spark

spark = get_spark("novashop-m07")
fact = spark.read.parquet("data/staging/fact_lines")
customers = spark.read.parquet("data/staging/customers_clean")
products = (
    spark.read.parquet("data/staging/products_clean")
    .dropDuplicates(["product_id"])
)

sales = (
    fact.join(customers, "customer_id", "inner")
    .join(products, "product_id", "left")
    .where(col("is_billable"))
    .select(
        "order_id",
        "order_ts",
        "order_month",
        "customer_id",
        "country",
        "segment",
        "product_id",
        "category",
        "qty",
        "unit_price",
        "discount",
        "gmv_line",
        "channel_norm",
    )
)
print(sales.count())
```

**Por qué:** left al catálogo conserva líneas `P999` (categoría nula). Inner a clientes quita `CX*`. Solo `paid`.

**Resultado esperado:** **1122** filas (mismo universo que M04-02).

### 2 — Escribir Parquet por mes

**Acción:**

```python
dest = Path("data/curated/sales_analytics")
(
    sales.write.mode("overwrite")
    .partitionBy("order_month")
    .parquet(str(dest))
)
print(sorted(p.name for p in dest.iterdir() if p.is_dir()))
```

**Por qué:** `overwrite` deja el curated idempotente. Doce particiones = doce meses de 2024.

**Resultado esperado:** carpetas `order_month=2024-01` … `order_month=2024-12` (más `_SUCCESS`).

### 3 — Prune al leer un mes

**Acción:**

```python
marzo = spark.read.parquet(str(dest)).where(col("order_month") == "2024-03")
marzo.explain("formatted")
print("marzo", marzo.count(), "total", spark.read.parquet(str(dest)).count())
```

**Por qué:** el plan debe listar solo la partición de marzo (o `PartitionFilters: [order_month=2024-03]`).

**Resultado esperado:** total **1122**. `marzo` es un subconjunto. El formatted menciona `2024-03`, no los doce meses como scan completo.

### 4 — CSV vs Parquet (tamaño)

**Acción:**

```python
import os

csv_dir = Path("data/curated/_csv_compare")
sales.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_dir))

def du(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

print("parquet", du(dest), "csv", du(csv_dir))
```

**Por qué:** columnar + compresión gana a texto. `coalesce(1)` solo existe aquí para comparar *un* CSV, no como patrón.

**Resultado esperado:** Parquet **menor** que el CSV (el ratio varía; en este volumen modestísimo a veces es parecido: lo importante es el schema al releer).

```python
spark.read.parquet(str(dest)).printSchema()
spark.read.option("header", True).csv(str(csv_dir)).printSchema()
```

El Parquet mantiene `decimal`/`timestamp`. El CSV vuelve a string.

## Comprueba tu entendimiento

**Idempotencia**
Vuelve a ejecutar el `write.mode("overwrite")` y cuenta.
→ Sigue **1122**. No se duplica.

## Reto

### 1 — Lectura de un solo país

Añade `partitionBy("order_month", "country")` en una copia `sales_analytics_geo` y lee `order_month=2024-03` ∧ `country=ES`.

<details>
<summary>Ver solución</summary>

```python
geo = Path("data/curated/sales_analytics_geo")
sales.write.mode("overwrite").partitionBy("order_month", "country").parquet(str(geo))
(
    spark.read.parquet(str(geo))
    .where((col("order_month") == "2024-03") & (col("country") == "ES"))
    .explain("formatted")
)
```

Si la cardinalidad de `country` es baja (ES, FR, …, UNK), el layout es razonable. No particiones por `customer_id`.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Miles de `part-000xx` minúsculos | `repartition(200)` residual | `repartition(12, "order_month")` antes del write, o acepta los 12 dirs |
| Count 2244 | `append` en vez de `overwrite` | `mode("overwrite")` |
| Al leer, `order_month` no está | Algunas APIs antiguas la tratan solo como partición | `spark.read.parquet` de 3.5 **sí** la incluye como columna |
| Curated en CSV “para el analista” | Pierdes tipos | Parquet al almacén; CSV solo como extracto puntual |
