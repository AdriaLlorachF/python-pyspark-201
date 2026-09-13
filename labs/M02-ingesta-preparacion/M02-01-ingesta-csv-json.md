# M02-01 — Ingesta CSV/JSON

[← Página anterior](README.md) · [Siguiente página →](M02-02-schema-tipos.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Cargar `customers`, `orders`, `products` y `events` y comprobar que los volúmenes coinciden con el dataset canónico.

### Prerrequisitos

- M01-01 hecho: sabes crear la sesión.
- `data/raw/` generado (`python3 scripts/generate_novashop.py` si falta).

### En qué consiste

Carga de las cuatro fuentes → casi sin transformación → validación de `count`.

### 1 — Sesión y rutas

**Acción:**

```python
import sys
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.append(str(Path("labs/_shared").resolve()))
from session import get_spark

spark = get_spark("novashop-m02")
RAW = Path("data/raw").resolve()
RAW.exists()
```

**Por qué:** todas las lecturas de este curso son rutas locales del repo.

**Resultado esperado:** `True` y una sesión `local[*]`.

### 2 — CSV de clientes y pedidos

**Acción:**

```python
customers = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
orders = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("customers", customers.count(), "orders", orders.count())
orders.printSchema()
orders.show(3, truncate=False)
```

**Por qué:** sin schema, Spark trata **todas** las columnas como string. Eso es lo que quieres ver ahora.

**Resultado esperado:** `customers 250` · `orders 800`. Schema de `orders` con `OrderId`, `CustomerId`, `OrderDate`, `Status`, `Channel` (todo `string`).

### 3 — JSON de catálogo y JSONL de eventos

**Acción:**

```python
products = spark.read.json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()
events.printSchema()
```

**Por qué:** un `.json` array y un `.jsonl` se leen con el mismo método; cambia el fichero, no el API.

**Resultado esperado:** `products 60` · `events 2500`. En productos aparecen `productId` y `listPrice` (camelCase).

> [!TIP]
> Si `products.count()` te da 1, estás leyendo el array como una sola fila. En este dataset el lector de Spark aplana el array: deben ser **60**.

## Comprueba tu entendimiento

**Volúmenes raw**
Cuenta las cuatro fuentes.
→ 250 / 800 / 60 / 2500.

## Reto

### 1 — Líneas de pedido

Lee `order_items.csv` con `header` y cuenta.

<details>
<summary>Ver solución</summary>

```python
items = spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
items.count()  # 2046
items.show(3)
```

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| `PATH not found` | El notebook no está con cwd = raíz del repo | `Path("data/raw").resolve()` y comprueba; en VS Code: “Open Folder” del repo |
| `orders` = 801 | Has contado la cabecera | `option("header", True)` |
| products = 1 | Lectura como texto / un solo documento mal interpretado | `spark.read.json(...)` sobre `products.json` del repo, no `wholetext` |
