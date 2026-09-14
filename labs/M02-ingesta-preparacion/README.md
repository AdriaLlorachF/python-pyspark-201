# M02 — Ingesta y preparación

[← Página anterior](../M01-fundamentos-entorno/M01-01-sesion-spark-primer-dataframe.md) · [Siguiente página →](M02-01-ingesta-csv-json.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en los laboratorios.

## Qué aprenderás

- Leer CSV, JSON y JSON Lines con `spark.read`.
- Distinguir **inferencia** de **schema explícito**.
- Tipar fechas e importes; normalizar nombres de columna.
- Filtrar suciedad intra-tabla y dejar un staging en Parquet.

## Teoría

NovaShop llega en formatos distintos a propósito. El lector cambia; el DataFrame que obtienes no.

| Fuente | Lector | Detalle |
|--------|--------|---------|
| `customers.csv`, `orders.csv`, `order_items.csv` | `spark.read.csv` | `header=True`. El CSV **siempre** entra como texto si no hay schema. |
| `products.json` | `spark.read.option("multiLine", True).json` | Un array JSON (un documento). |
| `events.jsonl` | `spark.read.json` | Una línea = un objeto. |

**Inferencia** (`inferSchema=True`): Spark mira una muestra y adivina. Útil para explorar. Peligrosa para producir: un precio `"19.90"` puede quedarse string; una fecha `13/01/2024` no es ISO.

**Schema explícito:** tú declaras nombre, tipo y si admite nulos. Es el contrato del pipeline.

Tipos que vas a usar:

| Tipo Spark | Para qué en NovaShop |
|------------|----------------------|
| `StringType` | IDs, canal, estado, país |
| `IntegerType` | `qty` |
| `DoubleType` / `DecimalType(10,2)` | importes |
| `TimestampType` / `DateType` | pedido y alta de cliente |

Calidad en este módulo = **intra-tabla**: nulos de clave, tipos, `qty > 0`. Los IDs huérfanos (cliente `CX*` que no está en `customers`, producto `P999`) se dejan para los joins de M04.

> [!WARNING]
> Inferir el schema es **exploración**, no producción. A partir de M02-02, los pedidos y los eventos se leen con schema declarado.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. Al leer `data/raw/orders.csv` con inferencia, las columnas se llaman `OrderId`, `CustomerId`, `OrderDate`… `OrderDate` queda string; hay filas con fecha `dd/mm/yyyy`.
2. `printSchema()` de esa lectura es el “antes”. A continuación se declara un `StructType` con `order_id`, `customer_id`, `order_ts`, `status`, `channel` y se vuelven a leer las mismas filas, renombrando.
3. El “después” muestra `timestamp` en la fecha y nombres en `snake_case`. El `count` sigue siendo **800**: tipar no borra filas.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M02-01 | [Ingesta CSV/JSON](M02-01-ingesta-csv-json.md) | Cargar las cuatro fuentes y validar volúmenes. |
| M02-02 | [Schema y tipos](M02-02-schema-tipos.md) | Sustituir inferencia; castear importes y fechas. |
| M02-03 | [Calidad y limpieza](M02-03-calidad-limpieza.md) | Filtrar inválidos y escribir `data/staging/`. |

→ Empieza por **[M02-01 — Ingesta CSV/JSON](M02-01-ingesta-csv-json.md)**.
