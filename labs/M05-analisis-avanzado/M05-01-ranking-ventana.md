# M05-01 — Ranking por ventana

[← Página anterior](README.md) · [Siguiente página →](M05-02-acumulados.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook** aquí. En clase usamos `notebooks/clase/`; no copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M05-01-ranking-ventana.ipynb` |
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

Obtener el top 10 de clientes por GMV y, por cada cliente, su top 3 productos.

### Prerrequisitos

- `data/staging/customer_gmv` y `fact_lines` + `customers_clean`.
- Si no tienes `customer_gmv`, rehaz el `groupBy` de M04-03.

### En qué consiste

Carga → window global de clientes → window `partitionBy(customer_id)` de productos → validación.

### 1 — Top 10 clientes

**Acción:**

```python
from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window

spark = get_spark("novashop-m05")
cust = spark.read.parquet(str(STAGING / "customer_gmv"))
w_global = Window.orderBy(col("gmv").desc())
top10 = cust.withColumn("rn", row_number().over(w_global)).where(col("rn") <= 10)
top10.orderBy("rn").show()
```

**Por qué:** sin `partitionBy`, el ranking es de toda la compañía.

**Resultado esperado:** 10 filas, `rn` de 1 a 10, GMV decreciente. El nº 1 ronda **6 000 €**.

### 2 — Top 3 productos por cliente

**Acción:**

```python
from pyspark.sql.functions import countDistinct

fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
product_gmv = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "product_id")
    .agg(fsum("gmv_line").alias("gmv"))
)
w_prod = Window.partitionBy("customer_id").orderBy(col("gmv").desc())
top3 = product_gmv.withColumn("rn", row_number().over(w_prod)).where(col("rn") <= 3)
top3.where(col("customer_id") == top10.select("customer_id").first()["customer_id"]).show()
print("filas top3", top3.count())
```

**Por qué:** `partitionBy` reinicia el `rn` en cada cliente. `groupBy` previo deja un grano producto-cliente (si no, una misma SKU repetida se rankearía varias veces).

**Resultado esperado:** como mucho 3 filas por cliente; `rn` 1–3. `filas top3` ≤ 211 × 3.

## Comprueba tu entendimiento

**Reinicio de ranking**
Elige un `customer_id` con varios productos y mira sus `rn`.
→ Empiezan en **1**, no continúan el ranking global.

## Reto

### 1 — `rank` vs `row_number`

Fuerza un empate (dos productos con el mismo GMV, o usa `rank` sobre `gmv` de clientes) y compara `rank` con `row_number`.

<details>
<summary>Ver solución</summary>

`row_number` nunca empata (rompe con un orden extra implícito). `rank` repite posición y **salta** (1, 2, 2, 4). `dense_rank` no salta (1, 2, 2, 3).

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Un solo `rn` = 1 en todo el fact | Olvidaste `partitionBy` cuando querías “por cliente” | Añádelo, o quítalo si el ranking es global |
| Top 3 con 30 filas del mismo cliente | Rankeaste líneas, no GMV por producto | `groupBy` cliente+producto antes |
| `Window` sin `orderBy` | Ranking indefinido | Siempre ordena la métrica |
