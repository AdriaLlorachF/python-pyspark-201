# M02-02 — Schema y tipos

[← Página anterior](M02-01-ingesta-csv-json.md) · [Siguiente página →](M02-03-calidad-limpieza.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook** aquí. En clase usamos `notebooks/clase/`; no copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M02-02-schema-tipos.ipynb` |
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

Leer pedidos, líneas y eventos con schema explícito, nombres en `snake_case` e importes/fechas casteados.

### Prerrequisitos

- M02-01: ya viste los `count` raw y el schema inferido (todo string).

### En qué consiste

Carga con `schema=` → normalización de columnas y tipos → validación con `printSchema`.

### 1 — Schema de pedidos

**Acción:**

```python
from pyspark.sql.types import StructType, StructField, StringType

orders_raw_schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Channel", StringType(), True),
])
orders = (
    spark.read.option("header", True)
    .schema(orders_raw_schema)
    .csv(str(RAW / "orders.csv"))
    .withColumnRenamed("OrderId", "order_id")
    .withColumnRenamed("CustomerId", "customer_id")
    .withColumnRenamed("OrderDate", "order_ts_raw")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("Channel", "channel")
)
```

**Por qué:** el fichero trae camelCase. El pipeline interno habla `snake_case`.

**Resultado esperado:** cinco columnas ya renombradas; `order_ts_raw` sigue siendo string (hay tres fechas `dd/mm/yyyy`).

### 2 — Timestamp tolerante

**Acción:**

```python
from pyspark.sql.functions import col, coalesce, to_timestamp

orders = orders.withColumn(
    "order_ts",
    coalesce(
        to_timestamp(col("order_ts_raw"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_ts_raw"), "dd/MM/yyyy"),
    ),
).drop("order_ts_raw")
orders.select("order_id", "order_ts").where(col("order_ts").isNull()).count()
orders.printSchema()
```

**Por qué:** `to_timestamp` con un solo formato deja nulos en las 3 filas “sucias”. `coalesce` de dos formatos las recupera.

**Resultado esperado:** `0` nulos en `order_ts`. Tipo `timestamp`.

### 3 — Líneas: enteros y decimales

**Acción:**

```python
from pyspark.sql.types import IntegerType, DecimalType

items = (
    spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    .withColumn("qty", col("qty").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
)
items.printSchema()
items.select("unit_price").limit(3).show()
```

**Por qué:** en el CSV `unit_price` es texto (`"19.90"`). El `DecimalType` es el tipo de dinero del curso.

**Resultado esperado:** `qty` integer, `unit_price`/`discount` decimal. `show` ya no pone comillas.

### 4 — Eventos con schema

**Acción:**

```python
from pyspark.sql.types import TimestampType

events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("product_id", StringType(), True),
])
events = spark.read.schema(events_schema).json(str(RAW / "events.jsonl"))
events.printSchema()
print(events.count())
```

**Por qué:** JSONL infiere bien *casi* siempre; el schema evita que `ts` se quede string el día que llegue un fichero raro.

**Resultado esperado:** 2500 filas; `ts` en `timestamp`.

## Comprueba tu entendimiento

**Contrato de tipos**
`printSchema()` de `orders` (tras el paso 2) e `items` (paso 3).
→ `orders.order_ts` timestamp; `items.unit_price` `decimal(10,2)`; `items.qty` int.

## Reto

### 1 — Catálogo en snake_case y decimal

Lee `products.json`, renombra `productId` → `product_id`, `listPrice` → `list_price` y castea `list_price` a `DecimalType(10,2)`.

<details>
<summary>Ver solución</summary>

```python
products = (
    spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    .withColumnRenamed("productId", "product_id")
    .withColumnRenamed("listPrice", "list_price")
    .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
)
products.printSchema()
```

Tres `list_price` nulos (los vacíos del raw): es correcto; se limpian en M02-03.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Casi todas las fechas nulas | Solo un `to_timestamp` ISO | Añade el formato `dd/MM/yyyy` en el `coalesce` |
| `unit_price` sigue string | Casteaste sobre otra variable | Reasigna `items = items.withColumn(...)` |
| `AnalysisException: cannot resolve OrderId` | Renombraste y luego filtraste el nombre viejo | Usa `order_id` a partir de aquí |
