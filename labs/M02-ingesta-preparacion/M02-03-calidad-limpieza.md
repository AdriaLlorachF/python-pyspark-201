# M02-03 — Calidad y limpieza

[← Página anterior](M02-02-schema-tipos.md) · [Siguiente página →](../M03-transformacion-datos/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook**. No abras ni copies `notebooks/validacion/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M02-03-calidad-limpieza.ipynb` |
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

Aplicar reglas intra-tabla y escribir el staging en Parquet para el resto del curso.

### Prerrequisitos

- DataFrames tipados de M02-02 (`orders`, `items`, `events`, `customers`, `products`).
- Si perdiste el notebook, rehaz la lectura con schema de M02-02.

### En qué consiste

Carga del raw ya tipado → filtros de calidad → validación de counts → escritura en `data/staging/`.

Reglas (cúmplelas **tal cual** para que los counts coincidan):

| Tabla | Regla |
|-------|--------|
| `customers` | `country` vacío o nulo → `"UNK"`. No borres filas. |
| `products` | Tira filas con `list_price` nulo. |
| `orders` | Tira `customer_id` nulo o `""`. **No** tiras huérfanos `CX*`. |
| `order_items` | Tira `product_id` vacío o `qty <= 0`. |
| `events` | Tira `customer_id` nulo. |

### 1 — Clientes y productos

**Acción:**

```python
from pyspark.sql.functions import col, when, trim

customers_clean = customers.withColumn(
    "country",
    when(col("country").isNull() | (trim(col("country")) == ""), "UNK").otherwise(col("country")),
)
products_clean = products.where(col("list_price").isNotNull())
print("customers", customers_clean.count(), "unk", customers_clean.where(col("country") == "UNK").count())
print("products", products_clean.count())
```

**Por qué:** un país vacío es dato recuperable; un producto sin precio no se puede vender.

**Resultado esperado:** customers **250** (5 `UNK`) · products **57**.

### 2 — Pedidos, líneas y eventos

**Acción:**

```python
orders_clean = orders.where(trim(col("customer_id")) != "")
items_clean = items.where((trim(col("product_id")) != "") & (col("qty") > 0))
events_clean = events.where(col("customer_id").isNotNull())
print("orders", orders_clean.count())
print("items", items_clean.count())
print("events", events_clean.count())
```

**Por qué:** claves vacías rompen cualquier join posterior. Los huérfanos (`CX*`, `P999`) siguen dentro: M04 los visibiliza.

**Resultado esperado:** orders **788** · items **2010** · events **2420**.

### 3 — Escribir staging

**Acción:**

```python
STAGING.mkdir(parents=True, exist_ok=True)
pairs = {
    "customers_clean": customers_clean,
    "products_clean": products_clean,
    "orders_clean": orders_clean,
    "order_items_clean": items_clean,
    "events_clean": events_clean,
}
for name, frame in pairs.items():
    dest = STAGING / name
    frame.write.mode("overwrite").parquet(str(dest))
    print(name, dest)
```

**Por qué:** Parquet guarda el schema. El siguiente módulo no vuelve a inferir CSV. M07 explica el formato; aquí ya te ahorra dolor.

**Resultado esperado:** cinco carpetas bajo `data/staging/`. Releer `orders_clean` debe dar 788 y `order_ts` timestamp.

## Comprueba tu entendimiento

**Nulos de clave**
En `orders_clean` y `items_clean`, cuenta nulos de `order_id` / `customer_id` / `product_id`.
→ **0** nulos. Pedidos **788**. Líneas **2010**.

## Reto

### 1 — Filas descartadas

Imprime `antes - después` de cada regla (pedidos, líneas, eventos, productos).

<details>
<summary>Ver solución</summary>

```text
orders     800 - 788 = 12   (customer_id vacío)
items     2046 - 2010 = 36  (21 sin producto ∪ 15 qty 0)
events    2500 - 2420 = 80
products    60 -  57 = 3
```

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| 788 no sale | Filtraste también los `CX*` | Esos 8 se quedan; solo quitas `customer_id` vacío |
| items ≠ 2010 | Aplicaste las reglas en dos pasos y duplicaste el filtro mal | Una sola `where` con producto no vacío **y** `qty > 0` |
| Staging ilegible | Escribiste CSV “para verlo” | Parquet; para espiar: `spark.read.parquet(...).show()` |
