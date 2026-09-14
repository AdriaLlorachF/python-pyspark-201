#!/usr/bin/env python3
"""Genera notebooks/clase/Mxx/01-teoria.ipynb (dinámica de aula, estilo LPY-102)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from nbutil import CELDA_0, code, md, write_notebook  # noqa: E402

OUT = ROOT / "notebooks" / "clase"


def build_all() -> None:
    specs = [
        ("M01-fundamentos-entorno", m01()),
        ("M02-ingesta-preparacion", m02()),
        ("M03-transformacion-datos", m03()),
        ("M04-integracion-agregacion", m04()),
        ("M05-analisis-avanzado", m05()),
        ("M06-optimizacion-ejecucion", m06()),
        ("M07-persistencia-datos", m07()),
    ]
    for folder, cells in specs:
        dest = OUT / folder / "01-teoria.ipynb"
        write_notebook(dest, cells)
        print("wrote", dest.relative_to(ROOT))


def _boot(app: str) -> list:
    return [
        md("## Arranque\n\nEjecuta esta celda y la siguiente. Kernel: **Python (NovaShop)**."),
        code(CELDA_0),
        code(f"spark = get_spark('{app}')\nprint(spark.version, spark.sparkContext.master)"),
    ]


def m01() -> list:
    return [
        md(
            """# M01 — Fundamentos y entorno (teoría de clase)

Este notebook lo recorremos **juntos**. El formador ejecuta; tú ejecutas las mismas celdas aquí.

Luego crearás **tu** lab en `notebooks/alumno/M01-01-sesion-spark.ipynb` (guion: `labs/M01-…`).

## Qué vamos a ver

- Spark es un **motor de cómputo**, no una base de datos.
- Pandas calcula en cada línea; Spark guarda un **plan** hasta una acción.
- `SparkSession` + `local[*]`."""
        ),
        *_boot("novashop-clase-m01"),
        md(
            """## Pandas vs Spark

| | Pandas | PySpark |
|---|--------|---------|
| Dónde viven los datos | RAM del proceso Python | Particiones (aquí: cores del Codespace) |
| Cuándo se calcula | En cada línea | Solo ante una **acción** |
| Índice de filas | Sí | No |

> Spark no “guarda” el DataFrame como un Excel. Guarda un **plan**. Hasta que no lanzas una acción, la cocina está apagada."""
        ),
        md("## Transformación vs acción"),
        code(
            """from pyspark.sql import Row

pedidos = [
    Row(order_id="O1", status="paid", amount=10.0),
    Row(order_id="O2", status="cancelled", amount=20.0),
    Row(order_id="O3", status="paid", amount=5.0),
]
df = spark.createDataFrame(pedidos)
paid = df.filter(df.status == "paid")  # transformación: aún no cuenta
print("objeto filter:", paid)
print("count (acción):", paid.count())
df.show()"""
        ),
        md(
            """En Spark UI (puerto **4040**) aparece el job del `show`/`count`, no el del `filter`.

**Siguiente:** crea `notebooks/alumno/M01-01-sesion-spark.ipynb` y sigue [M01-01](../../labs/M01-fundamentos-entorno/M01-01-sesion-spark-primer-dataframe.md)."""
        ),
    ]


def m02() -> list:
    return [
        md(
            """# M02 — Ingesta y preparación (teoría de clase)

Leemos las fuentes reales de NovaShop. Inferir schema es **exploración**; producir pide schema explícito.

**Después:** labs M02-01 → M02-03 en `notebooks/alumno/`."""
        ),
        *_boot("novashop-clase-m02"),
        md("## CSV: todo entra como texto"),
        code(
            """orders_txt = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("filas", orders_txt.count())
orders_txt.printSchema()
orders_txt.show(3, truncate=False)"""
        ),
        md(
            """## JSON array vs JSONL

`products.json` es **un** documento (array): hace falta `multiLine=True`.  
`events.jsonl` es una línea = un objeto."""
        ),
        code(
            """products = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()"""
        ),
        md("## Inferencia vs schema (el “antes / después”)"),
        code(
            """from pyspark.sql.functions import col, coalesce, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

print("fechas raras (dd/mm/yyyy):")
orders_txt.where(col("OrderDate").contains("/")).select("OrderId", "OrderDate").show()

schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Channel", StringType(), True),
])
orders = (
    spark.read.option("header", True).schema(schema).csv(str(RAW / "orders.csv"))
    .withColumnRenamed("OrderId", "order_id")
    .withColumnRenamed("CustomerId", "customer_id")
    .withColumnRenamed("OrderDate", "order_ts_raw")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("Channel", "channel")
    .withColumn(
        "order_ts",
        coalesce(
            to_timestamp(col("order_ts_raw"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("order_ts_raw"), "dd/MM/yyyy"),
        ),
    )
    .drop("order_ts_raw")
)
orders.printSchema()
print("nulos de fecha", orders.where(col("order_ts").isNull()).count())
print("count sigue siendo", orders.count())"""
        ),
        md(
            """Tipar **no borra** filas. La suciedad de claves se limpia en el lab M02-03.

**Siguiente:** [M02-01](../../labs/M02-ingesta-preparacion/M02-01-ingesta-csv-json.md) → notebook `notebooks/alumno/M02-01-ingesta-csv-json.ipynb`."""
        ),
    ]


def m03() -> list:
    return [
        md(
            """# M03 — Transformación (teoría de clase)

La regla de negocio es una **columna**, no un `for`. Demo con 4 líneas en memoria (no hace falta el staging)."""
        ),
        *_boot("novashop-clase-m03"),
        md("## `withColumn` y GMV"),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, when, lower, least, lit, date_format

lineas = spark.createDataFrame([
    Row(order_id="O1", qty=2, unit_price=10.0, discount=0.10, status="paid", channel="WEB"),
    Row(order_id="O2", qty=1, unit_price=80.0, discount=1.50, status="cancelled", channel="marketplace"),
    Row(order_id="O3", qty=3, unit_price=5.0, discount=0.0, status="paid", channel="app"),
    Row(order_id="O4", qty=1, unit_price=20.0, discount=0.0, status="pending", channel="store"),
])
crudo = lineas.withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
crudo.select("order_id", "discount", "gmv_line").show()"""
        ),
        md("El descuento `1.50` **vuelve el GMV negativo**. En el lab se capa a 1 y se recalcula."),
        code(
            """fact = (
    crudo.withColumn("discount", least(col("discount"), lit(1.0)))
    .withColumn(
        "channel_norm",
        when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel"))).otherwise(lit("other")),
    )
    .withColumn("is_billable", col("status") == "paid")
    .withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
)
fact.select("order_id", "channel", "channel_norm", "discount", "gmv_line", "is_billable").show()"""
        ),
        md(
            """No uses `collect()` / `toPandas()` del fact entero. Si miras, `limit(20).toPandas()`.

**Siguiente:** [M03-01](../../labs/M03-transformacion-datos/M03-01-enriquecimiento.md)."""
        ),
    ]


def m04() -> list:
    return [
        md(
            """# M04 — Joins y KPIs (teoría de clase)

Un KPI mentiroso casi siempre es un **join mal elegido**. Demo mínima con un huérfano."""
        ),
        *_boot("novashop-clase-m04"),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, sum as fsum, countDistinct

clientes = spark.createDataFrame([
    Row(customer_id="C1", country="ES"),
    Row(customer_id="C2", country="FR"),
])
lineas = spark.createDataFrame([
    Row(order_id="O1", customer_id="C1", gmv_line=100.0, is_billable=True),
    Row(order_id="O2", customer_id="C1", gmv_line=50.0, is_billable=True),
    Row(order_id="O3", customer_id="CX9", gmv_line=999.0, is_billable=True),
])
print("inner", lineas.join(clientes, "customer_id", "inner").count())
print("left ", lineas.join(clientes, "customer_id", "left").count())
lineas.join(clientes, "customer_id", "left_anti").show()"""
        ),
        md("Ticket medio = `sum(GMV) / countDistinct(order_id)`, **no** `avg` de la línea."),
        code(
            """sales = lineas.join(clientes, "customer_id", "inner").where(col("is_billable"))
sales.agg(fsum("gmv_line").alias("gmv"), countDistinct("order_id").alias("orders")).show()
sales.groupBy("country").agg(fsum("gmv_line").alias("gmv")).show()"""
        ),
        md("**Siguiente:** [M04-01](../../labs/M04-integracion-agregacion/M04-01-joins.md) sobre el dataset real."),
    ]


def m05() -> list:
    return [
        md(
            """# M05 — Window functions (teoría de clase)

`groupBy` **aplasta** filas. Una window **calcula y conserva** el detalle."""
        ),
        *_boot("novashop-clase-m05"),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window

hist = spark.createDataFrame([
    Row(customer_id="C1", order_id="O1", order_n_ts="2024-01-01", gmv=10.0),
    Row(customer_id="C1", order_id="O2", order_n_ts="2024-02-01", gmv=30.0),
    Row(customer_id="C2", order_id="O3", order_n_ts="2024-01-15", gmv=5.0),
    Row(customer_id="C2", order_id="O4", order_n_ts="2024-03-01", gmv=8.0),
])
w = Window.partitionBy("customer_id").orderBy("order_n_ts")
(
    hist.withColumn("order_n", row_number().over(w))
    .withColumn("gmv_running", fsum("gmv").over(w))
    .orderBy("customer_id", "order_n")
    .show()
)"""
        ),
        md(
            """`partitionBy` de la window ≠ `repartition` físico (M06).

**Siguiente:** [M05-01](../../labs/M05-analisis-avanzado/M05-01-ranking-ventana.md)."""
        ),
    ]


def m06() -> list:
    return [
        md(
            """# M06 — Lazy, plan y cache (teoría de clase)

Sin acción no hay job. `cache()` no materializa hasta un `count`/`show`."""
        ),
        *_boot("novashop-clase-m06"),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col

base = spark.createDataFrame([Row(x=i, canal="web" if i % 2 == 0 else "app") for i in range(20)])
planned = base.where(col("x") > 3).where(col("canal") == "web").select("x")
print("sin acción:", planned)
print("con count:", planned.count())
planned.explain("formatted")"""
        ),
        code(
            """warm = planned.cache()
print("1º (materializa)", warm.count())
print("2º (debería leer cache)", warm.count())
warm.unpersist()"""
        ),
        md(
            """Mira Spark UI (4040): el primer `count` llena Storage; el segundo no relee el plan desde cero.

**Siguiente:** [M06-01](../../labs/M06-optimizacion-ejecucion/M06-01-explain-dag.md)."""
        ),
    ]


def m07() -> list:
    return [
        md(
            """# M07 — Parquet y partición de negocio (teoría de clase)

El pipeline acaba en un directorio que otro proceso puede leer mañana."""
        ),
        *_boot("novashop-clase-m07"),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col

demo = spark.createDataFrame([
    Row(order_id="O1", order_month="2024-01", gmv=10.0),
    Row(order_id="O2", order_month="2024-01", gmv=20.0),
    Row(order_id="O3", order_month="2024-02", gmv=5.0),
])
dest = CURATED / "_demo_sales"
CURATED.mkdir(parents=True, exist_ok=True)
demo.write.mode("overwrite").partitionBy("order_month").parquet(str(dest))
print(sorted(p.name for p in dest.iterdir() if p.is_dir()))
enero = spark.read.parquet(str(dest)).where(col("order_month") == "2024-01")
enero.explain("formatted")
print("enero", enero.count(), "total", spark.read.parquet(str(dest)).count())"""
        ),
        md(
            """`repartition` (M06) baraja **memoria**. `partitionBy` en el `write` organiza **disco**.

**Siguiente:** [M07-01](../../labs/M07-persistencia-datos/M07-01-parquet-layout.md) sobre el fact real."""
        ),
    ]


if __name__ == "__main__":
    build_all()
