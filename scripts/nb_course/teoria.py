"""Teoría M01–M07: explicación y demo intercaladas (tú ejecutas aquí)."""
from __future__ import annotations

from .common import boot_cells, code, md, teoria_head


def m01() -> list:
    return [
        md(
            teoria_head(
                "M01 — Fundamentos y entorno",
                """Spark es un **motor de cómputo**, no una base de datos. Pandas calcula en cada línea; Spark guarda un **plan** hasta una acción.

Después de este notebook creas el lab en `notebooks/trabajo/`. El guion está al lado: `02-lab-sesion-spark.ipynb`.""",
                "../M00-entorno-notebooks/02-lab-primer-notebook.ipynb",
                "02-lab-sesion-spark.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m01"),
        md(
            """## Pandas vs Spark

| | Pandas | PySpark |
|---|--------|---------|
| Dónde viven los datos | RAM del proceso Python | Particiones (aquí: cores del Codespace) |
| Cuándo se calcula | En cada línea | Solo ante una **acción** |
| Índice de filas | Sí | No |

Spark no “guarda” el DataFrame como un Excel. Guarda un **plan**. Hasta que no lanzas una acción, la cocina está apagada."""
        ),
        md(
            """## Transformación frente a acción

`filter` alarga el plan. `count` y `show` lo ejecutan. Ejecuta y fíjate: el `print` del objeto no es una tabla."""
        ),
        code(
            """from pyspark.sql import Row

pedidos = [
    Row(order_id="O1", status="paid", amount=10.0),
    Row(order_id="O2", status="cancelled", amount=20.0),
    Row(order_id="O3", status="paid", amount=5.0),
]
df = spark.createDataFrame(pedidos)
paid = df.filter(df.status == "paid")
print("objeto filter (aún no ha contado):", paid)
print("count (acción):", paid.count())
df.show()"""
        ),
        md(
            """En Spark UI (puerto **4040**) aparece el job del `show`/`count`, no el del `filter`.

**Siguiente:** abre el [lab](02-lab-sesion-spark.ipynb) y **crea tu** notebook."""
        ),
    ]


def m02() -> list:
    return [
        md(
            teoria_head(
                "M02 — Ingesta y preparación",
                """Leemos las fuentes reales de NovaShop. Inferir schema es **exploración**; producir pide schema explícito.

Labs después: ingesta → tipos → limpieza.""",
                "../M01-fundamentos-entorno/02-lab-sesion-spark.ipynb",
                "02-lab-ingesta-csv-json.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m02"),
        md("## CSV: todo entra como texto\n\nSin schema, Spark trata **todas** las columnas como string. Eso es lo que quieres ver ahora."),
        code(
            """orders_txt = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("filas", orders_txt.count())
orders_txt.printSchema()
orders_txt.show(3, truncate=False)"""
        ),
        md(
            """## JSON array vs JSONL

`products.json` es **un** documento (array): hace falta `multiLine=True`.  
`events.jsonl` es una línea = un objeto. Sin `multiLine` el array se parte en `_corrupt_record`."""
        ),
        code(
            """products = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()"""
        ),
        md("## Inferencia vs schema\n\nTipar **no borra** filas. Hay fechas `dd/mm/yyyy`: un solo formato las deja nulas."),
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
        md("**Siguiente:** [lab de ingesta](02-lab-ingesta-csv-json.ipynb) — creas tu notebook y cargas las cuatro fuentes."),
    ]


def m03() -> list:
    return [
        md(
            teoria_head(
                "M03 — Transformación",
                """La regla de negocio es una **columna**, no un `for`. Demo con 4 líneas en memoria (no hace falta el staging).""",
                "../M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb",
                "02-lab-enriquecimiento.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m03"),
        md("## `withColumn` y GMV\n\nSi el descuento crudo es `1.50`, el GMV **sale negativo**. No es un bug de Spark."),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, when, lower, least, lit

lineas = spark.createDataFrame([
    Row(order_id="O1", qty=2, unit_price=10.0, discount=0.10, status="paid", channel="WEB"),
    Row(order_id="O2", qty=1, unit_price=80.0, discount=1.50, status="cancelled", channel="marketplace"),
    Row(order_id="O3", qty=3, unit_price=5.0, discount=0.0, status="paid", channel="app"),
    Row(order_id="O4", qty=1, unit_price=20.0, discount=0.0, status="pending", channel="store"),
])
crudo = lineas.withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
crudo.select("order_id", "discount", "gmv_line").show()"""
        ),
        md("En el lab capas el descuento a 1 y **recalculas** el GMV."),
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
        md("No uses `collect()` / `toPandas()` del fact entero. Si miras, `limit(20).toPandas()`.\n\n**Siguiente:** [lab de enriquecimiento](02-lab-enriquecimiento.ipynb)."),
    ]


def m04() -> list:
    return [
        md(
            teoria_head(
                "M04 — Joins y KPIs",
                """Un KPI mentiroso casi siempre es un **join mal elegido**. Demo mínima con un huérfano.""",
                "../M03-transformacion-datos/03-lab-reglas-negocio.ipynb",
                "02-lab-joins.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m04"),
        md("## Inner, left y anti\n\nEl inner **tira** al huérfano. El left lo deja. `left_anti` lo lista."),
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
        md("**Siguiente:** [lab de joins](02-lab-joins.ipynb) sobre el dataset real."),
    ]


def m05() -> list:
    return [
        md(
            teoria_head(
                "M05 — Window functions",
                """`groupBy` **aplasta** filas. Una window **calcula y conserva** el detalle.""",
                "../M04-integracion-agregacion/04-lab-segmentacion.ipynb",
                "02-lab-ranking-ventana.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m05"),
        md("## `row_number` y suma acumulada\n\n`partitionBy` de la window ≠ `repartition` físico (eso es M06)."),
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
        md("**Siguiente:** [lab de ranking](02-lab-ranking-ventana.ipynb)."),
    ]


def m06() -> list:
    return [
        md(
            teoria_head(
                "M06 — Lazy, plan y cache",
                """Sin acción no hay job. `cache()` no materializa hasta un `count`/`show`.""",
                "../M05-analisis-avanzado/03-lab-acumulados.ipynb",
                "02-lab-explain-dag.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m06"),
        md("## El plan no es un job\n\nImprimir el DataFrame no dispara nada. `count` sí."),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col

base = spark.createDataFrame([Row(x=i, canal="web" if i % 2 == 0 else "app") for i in range(20)])
planned = base.where(col("x") > 3).where(col("canal") == "web").select("x")
print("sin acción:", planned)
print("con count:", planned.count())
planned.explain("formatted")"""
        ),
        md("## Cache perezoso\n\nLa primera acción llena Storage; la segunda debería leer memoria."),
        code(
            """warm = planned.cache()
print("1º (materializa)", warm.count())
print("2º (debería leer cache)", warm.count())
warm.unpersist()"""
        ),
        md(
            """Mira Spark UI (4040): el primer `count` llena Storage.

**Siguiente:** [lab de explain](02-lab-explain-dag.ipynb)."""
        ),
    ]


def m07() -> list:
    return [
        md(
            teoria_head(
                "M07 — Parquet y partición de negocio",
                """El pipeline acaba en un directorio que otro proceso puede leer mañana. `repartition` baraja **memoria**. `partitionBy` en el `write` organiza **disco**.""",
                "../M06-optimizacion-ejecucion/03-lab-cache-particionado.ipynb",
                "02-lab-parquet-layout.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m07"),
        md("## Escribes carpetas, no un Excel"),
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
        md("**Siguiente:** [lab de parquet](02-lab-parquet-layout.ipynb) sobre el fact real."),
    ]
