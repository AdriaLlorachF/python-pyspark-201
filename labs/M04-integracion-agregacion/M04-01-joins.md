# M04-01 — Joins

[← Página anterior](README.md) · [Siguiente página →](M04-02-kpis.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook**. No abras ni copies `notebooks/validacion/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M04-01-joins.ipynb` |
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

Medir cuántas líneas y pedidos se pierden al hacer inner contra clientes, y listar los huérfanos.

### Prerrequisitos

- `data/staging/fact_lines` (M03-02) y `customers_clean` (M02-03).

### En qué consiste

Carga → inner y left → validación de counts → listado `CX*`.

### 1 — Cargar fact y clientes

**Acción:**

```python
spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
print(fact.count(), customers.count())
```

**Por qué:** el fact ya trae `customer_id` de la cabecera.

**Resultado esperado:** `1980 250`.

### 2 — Inner frente a left

**Acción:**

```python
inner = fact.join(customers, "customer_id", "inner")
left = fact.join(customers, "customer_id", "left")
print("inner", inner.count(), "left", left.count())
```

**Por qué:** la diferencia **es** el síntoma de las claves huérfanas.

**Resultado esperado:** inner **1956** · left **1980**.

### 3 — Anti-join de huérfanos

**Acción:**

```python
orphans = fact.join(customers, "customer_id", "left_anti")
orphans.select("order_id", "customer_id").distinct().orderBy("order_id").show()
print("líneas", orphans.count(), "pedidos", orphans.select("order_id").distinct().count())
```

**Por qué:** `left_anti` = “está en el fact y no en clientes”. Mejor que un `where` a ciegas.

**Resultado esperado:** **24** líneas · **8** pedidos · `customer_id` tipo `CX*`.

> [!TIP]
> Añade `products_clean` con `left` sobre `product_id`. Las líneas `P999` (catálogo inexistente) aparecen con `name` nulo: mismo patrón.

## Comprueba tu entendimiento

**Pérdida del inner**
`fact.count() - inner.count()`.
→ **24** líneas (8 pedidos).

## Reto

### 1 — Pedidos sin cliente

Haz el mismo inner/left a grano **pedido** (`orders_clean` ⋈ `customers_clean`).

<details>
<summary>Ver solución</summary>

```python
orders = spark.read.parquet(str(STAGING / "orders_clean"))
print("orders", orders.count())
print("inner", orders.join(customers, "customer_id", "inner").count())  # 780
print("left ", orders.join(customers, "customer_id", "left").count())   # 788
```

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Count inner *mayor* que 1980 | Productos duplicados en el join de catálogo | `products` debe tener `product_id` único (`dropDuplicates`) |
| No veo `CX*` | Los filtraste en M02-03 | No debían filtrarse; regenera staging |
| Columnas `customer_id` ambiguas | Join sin `on=` y ambos lados con el mismo nombre mal | Usa `join(..., "customer_id")` |
