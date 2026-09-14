"""Labs M01–M03: guion paso a paso (tú creas el notebook)."""
from __future__ import annotations

from .common import CELDA_0, comprueba, errores, lab_abre, md, paso, reto, siguiente


def m01_01() -> list:
    return [
        md(
            lab_abre(
                "M01-01",
                "Sesión Spark y primer DataFrame",
                "M01-01-sesion-spark.ipynb",
                "Dejar una SparkSession viva y materializar 5 pedidos en memoria. Distingues un `filter` de un `count`. Aún no lees `data/raw/`.",
                "01-teoria.ipynb",
                "../M02-ingesta-preparacion/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Arranque (siempre el primero)",
                "Celda 0: localizo el repo y las rutas. Sin esto el resto no arranca.",
                CELDA_0,
                "`RAW` existe True. `ROOT` es la carpeta del curso.",
                "El notebook vive en `trabajo/`; las rutas se resuelven desde el repo, no desde `cwd`.",
                "Copia la celda entera. No escribas `Path('data/raw')`.",
            ),
        *paso(
                "2",
                "Comprueba Java y PySpark",
                "Antes de crear la sesión miro versiones. Sin JRE 17 Spark no arranca.",
                """import pyspark, shutil, subprocess

print("pyspark", pyspark.__version__)
print(subprocess.check_output(["java", "-version"], text=True, stderr=subprocess.STDOUT).splitlines()[0])
print("java:", shutil.which("java"))""",
                "PySpark `3.5.x` y una línea `openjdk version \"17…\"` (o Microsoft JDK 17).",
                "Detectas el fallo de entorno *antes* de pelearte con un DataFrame.",
                "Si Java no es 17: Codespace limpio o `java -version` en local. No improvises otro JDK.",
            ),
        *paso(
                "3",
                "Crea (o reusa) la sesión",
                "Pido una SparkSession local[*]. getOrCreate evita un segundo contexto en el puerto 4040.",
                """spark = get_spark("novashop-m01")
spark""",
                "Ves un objeto SparkSession. Vuelve a ejecutar la misma celda: es **la misma** sesión, no otra.",
                "`get_spark` ya pone master `local[*]`, UI 4040 y TZ UTC.",
                "Puerto ocupado: `spark.stop()` y otra vez `get_spark()`.",
            ),
        *paso(
                "4",
                "Cinco pedidos en memoria",
                "createDataFrame es la forma más pequeña de ver schema + tabla sin ficheros.",
                """from pyspark.sql import Row

pedidos = [
    Row(order_id="O90001", customer_id="C0001", status="paid", amount=49.90),
    Row(order_id="O90002", customer_id="C0002", status="paid", amount=12.50),
    Row(order_id="O90003", customer_id="C0003", status="cancelled", amount=80.00),
    Row(order_id="O90004", customer_id="C0001", status="paid", amount=23.10),
    Row(order_id="O90005", customer_id="C0004", status="pending", amount=5.00),
]
df = spark.createDataFrame(pedidos)
df.printSchema()
df.show()""",
                "Schema: `order_id`/`customer_id`/`status` string, `amount` double. Tabla de 5 filas (O90001…O90005).",
                "Materializas algo visible. `printSchema` y `show` **sí** son acciones.",
            ),
        *paso(
                "5",
                "Filter no cuenta; count sí",
                "filter solo alarga el plan. count obliga a ejecutarlo. Quiero 3 paid.",
                """paid = df.filter(df.status == "paid")
print("después del filter, Spark aún no ha contado nada")
print("paid count =", paid.count())""",
                "`paid count = 3`.",
                "Si crees que `filter` ya filtró y “no ves nada”, te falta una acción.",
                extra="Opcional, en otra celda (con su Markdown): `paid.explain(\"formatted\")`. No hace falta entender cada línea.",
            ),
        md(
            comprueba(
                """- `spark.version` es 3.5.x.
- Sobre las 5 filas, `status == "paid"` da **3**.
- Tu notebook tiene Markdown **antes** de cada código.
- **Run All** funciona de arriba abajo."""
            )
        ),
        *reto(
                "Canal en dos columnas",
                "En *tu* notebook: Markdown que explique el `withColumn` + código que añada `channel` con todos `\"web\"` y muestre solo `order_id` y `channel`. Ejecuta. Deben ser 5 filas y no debe salir `amount`.",
                """```python
from pyspark.sql.functions import lit

df.withColumn("channel", lit("web")).select("order_id", "channel").show()
```""",
            ),
        md(
            errores(
                [
                    ("`Java gateway process exited`", "No hay JDK 17", "Codespace limpio; `java -version` = 17"),
                    ("Puerto 4040 ocupado", "Segunda SparkSession", "`spark.stop()` y `get_spark()`"),
                    ("`filter` y “no veo nada”", "No lanzaste acción", "Encadena `.show()` o `.count()`"),
                ]
            )
        ),
        md(siguiente("../M02-ingesta-preparacion/01-teoria.ipynb", "M02 — teoría")),
    ]


def m02_01() -> list:
    return [
        md(
            lab_abre(
                "M02-01",
                "Ingesta CSV y JSON",
                "M02-01-ingesta-csv-json.ipynb",
                "Cargar customers, orders, products y events y comprobar los volúmenes canónicos. Casi sin transformar.",
                "01-teoria.ipynb",
                "03-lab-schema-tipos.ipynb",
            )
        ),
        *paso(
                "1",
                "Arranque y sesión",
                "Celda 0 + sesión. Compruebo que RAW existe antes de leer.",
                CELDA_0
                + "\n\nspark = get_spark(\"novashop-m02\")\nprint(RAW.exists())",
                "`True` y una sesión `local[*]`.",
                "Todas las lecturas de este curso son rutas locales del repo (`RAW`, no un string suelto).",
            ),
        *paso(
                "2",
                "CSV de clientes y pedidos",
                "Leo CSV con header. Sin schema: todo string. Cuento y miro 3 filas de orders.",
                """customers = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
orders = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("customers", customers.count(), "orders", orders.count())
orders.printSchema()
orders.show(3, truncate=False)""",
                "`customers 250` · `orders 800`. Schema de `orders` con `OrderId`, `CustomerId`, `OrderDate`, `Status`, `Channel` (todo `string`).",
                "Sin schema ves el fichero crudo. Si cuentas 801, has contado la cabecera.",
                "`option(\"header\", True)` en los dos.",
            ),
        *paso(
                "3",
                "JSON array y JSONL",
                "products.json es un array: multiLine=True. events.jsonl es una línea = un objeto.",
                """products = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()
events.printSchema()""",
                "`products 60` · `events 2500`. En productos aparecen `productId` y `listPrice` (camelCase).",
                "El API es el mismo (`.json`); cambia el fichero.",
                "Si products = 362 o ves `_corrupt_record`: falta `multiLine=True`.",
            ),
        md(
            comprueba(
                """Cuentas las cuatro fuentes otra vez (Run All). Debes tener **250 / 800 / 60 / 2500**.
Cada lectura tiene su celda Markdown encima."""
            )
        ),
        *reto(
                "Líneas de pedido",
                "Lee `order_items.csv` con header, cuenta y muestra 3 filas. Markdown + código + ejecuta.",
                """```python
items = spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
print(items.count())  # 2046
items.show(3)
```""",
            ),
        md(
            errores(
                [
                    ("PATH not found", "Saltaste la Celda 0", "Pega el arranque y usa `RAW`"),
                    ("orders = 801", "Contaste la cabecera", "`option(\"header\", True)`"),
                    ("products 362 / `_corrupt_record`", "JSON array sin multiLine", "`option(\"multiLine\", True)`"),
                ]
            )
        ),
        md(siguiente("03-lab-schema-tipos.ipynb", "M02-02 schema y tipos")),
    ]


def m02_02() -> list:
    return [
        md(
            lab_abre(
                "M02-02",
                "Schema y tipos",
                "M02-02-schema-tipos.ipynb",
                "Leer pedidos, líneas y eventos con schema (o cast), nombres en snake_case e importes/fechas casteados.",
                "02-lab-ingesta-csv-json.ipynb",
                "04-lab-calidad-limpieza.ipynb",
            )
        ),
        *paso(
                "1",
                "Arranque",
                "Celda 0 y sesión. Este lab parte de raw, no de lo que tenías en memoria ayer.",
                CELDA_0 + "\n\nspark = get_spark(\"novashop-m02\")",
                "Sesión lista. `RAW` True.",
                "Cada notebook es autónomo: no asumas variables de otro fichero.",
            ),
        *paso(
                "2",
                "Schema de pedidos y snake_case",
                "El fichero trae camelCase. El pipeline interno habla snake_case. Declaro el schema y renombro.",
                """from pyspark.sql.types import StructType, StructField, StringType

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
orders.printSchema()
print(orders.count())""",
                "Cinco columnas ya renombradas. `order_ts_raw` sigue string. Count **800**.",
                "Tipar no borra filas. Las fechas raras se arreglan en el siguiente paso.",
            ),
        *paso(
                "3",
                "Timestamp con dos formatos",
                "Hay ISO y dd/MM/yyyy. Un solo to_timestamp deja nulos. coalesce de dos formatos las recupera.",
                """from pyspark.sql.functions import col, coalesce, to_timestamp

orders = orders.withColumn(
    "order_ts",
    coalesce(
        to_timestamp(col("order_ts_raw"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_ts_raw"), "dd/MM/yyyy"),
    ),
).drop("order_ts_raw")
print("nulos de fecha", orders.where(col("order_ts").isNull()).count())
orders.printSchema()""",
                "`0` nulos en `order_ts`. Tipo `timestamp`.",
                "Si solo usas ISO, las 3 filas sucias mueren como nulo.",
            ),
        *paso(
                "4",
                "Líneas: enteros y decimales",
                "En el CSV unit_price es texto. DecimalType es el tipo de dinero del curso. Reasigno items = items.withColumn(...).",
                """from pyspark.sql.types import IntegerType, DecimalType

items = (
    spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    .withColumn("qty", col("qty").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
)
items.printSchema()
items.select("unit_price").limit(3).show()""",
                "`qty` integer, `unit_price`/`discount` decimal. `show` ya no pone comillas.",
                "Si no reasignas, `unit_price` sigue string en el objeto viejo.",
            ),
        *paso(
                "5",
                "Eventos con schema",
                "JSONL infiere bien casi siempre; el schema evita que ts se quede string el día que llegue un fichero raro.",
                """from pyspark.sql.types import TimestampType

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
print(events.count())""",
                "2500 filas; `ts` en `timestamp`.",
                "Declarar el contrato es más barato que depurar un inferido distinto mañana.",
            ),
        md(
            comprueba(
                """`printSchema()` de `orders` (tras el paso 3) e `items` (paso 4):
`orders.order_ts` timestamp; `items.unit_price` `decimal(10,2)`; `items.qty` int."""
            )
        ),
        *reto(
                "Catálogo en snake_case y decimal",
                "Lee `products.json` (multiLine), renombra `productId` → `product_id`, `listPrice` → `list_price` y castea `list_price` a `DecimalType(10,2)`. Tres `list_price` nulos es correcto (se limpian en el siguiente lab).",
                """```python
products = (
    spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    .withColumnRenamed("productId", "product_id")
    .withColumnRenamed("listPrice", "list_price")
    .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
)
products.printSchema()
```""",
            ),
        md(
            errores(
                [
                    ("Casi todas las fechas nulas", "Un solo to_timestamp ISO", "Añade `dd/MM/yyyy` en el coalesce"),
                    ("unit_price sigue string", "No reasignaste", "`items = items.withColumn(...)`"),
                    ("cannot resolve OrderId", "Renombraste y filtraste el nombre viejo", "Usa `order_id` a partir de aquí"),
                ]
            )
        ),
        md(siguiente("04-lab-calidad-limpieza.ipynb", "M02-03 calidad")),
    ]


def m02_03() -> list:
    reglas = """| Tabla | Regla |
|-------|--------|
| `customers` | `country` vacío o nulo → `"UNK"`. **No** borres filas. |
| `products` | Tira filas con `list_price` nulo. |
| `orders` | Tira `customer_id` nulo o `""`. **No** tiras huérfanos `CX*`. |
| `order_items` | Tira `product_id` vacío o `qty <= 0`. |
| `events` | Tira `customer_id` nulo. |"""
    return [
        md(
            lab_abre(
                "M02-03",
                "Calidad y limpieza",
                "M02-03-calidad-limpieza.ipynb",
                "Aplicar las reglas intra-tabla y escribir el staging en Parquet. Si perdiste el notebook anterior, rehaz la lectura tipada de M02-02 al empezar.",
                "03-lab-schema-tipos.ipynb",
                "../M03-transformacion-datos/01-teoria.ipynb",
            )
        ),
        md(f"## Contrato (cúmplelo tal cual)\n\n{reglas}\n"),
        *paso(
                "1",
                "Arranque y lecturas tipadas",
                "Repito la lectura de M02-02 (orders con coalesce, items casteados, products, customers, events). No invento otro schema.",
                CELDA_0
                + """

from pyspark.sql.functions import col, coalesce, to_timestamp, when, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

spark = get_spark("novashop-m02")

orders = (
    spark.read.option("header", True).csv(str(RAW / "orders.csv"))
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
customers = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
products = (
    spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    .withColumnRenamed("productId", "product_id")
    .withColumnRenamed("listPrice", "list_price")
    .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
)
items = (
    spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    .withColumn("qty", col("qty").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
)
events = spark.read.schema(
    StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("product_id", StringType(), True),
    ])
).json(str(RAW / "events.jsonl"))
print(orders.count(), customers.count(), products.count(), items.count(), events.count())""",
                "800 250 60 2046 2500 (aún sucios).",
                "El lab de limpieza parte de tipos ya puestos. Si saltas esto, los filtros no coinciden.",
            ),
        *paso(
                "2",
                "Clientes y productos",
                "País vacío es recuperable (UNK). Producto sin precio no se vende: se tira.",
                """customers_clean = customers.withColumn(
    "country",
    when(col("country").isNull() | (trim(col("country")) == ""), "UNK").otherwise(col("country")),
)
products_clean = products.where(col("list_price").isNotNull())
print("customers", customers_clean.count(), "unk", customers_clean.where(col("country") == "UNK").count())
print("products", products_clean.count())""",
                "customers **250** (5 `UNK`) · products **57**.",
                "Spark CSV convierte vacíos en null: hay que tratar null y `\"\"`.",
            ),
        *paso(
                "3",
                "Pedidos, líneas y eventos",
                "Claves vacías rompen joins. Los huérfanos CX* y P999 se quedan: M04 los visibiliza.",
                """orders_clean = orders.where(trim(col("customer_id")) != "")
items_clean = items.where((trim(col("product_id")) != "") & (col("qty") > 0))
events_clean = events.where(col("customer_id").isNotNull())
print("orders", orders_clean.count())
print("items", items_clean.count())
print("events", events_clean.count())""",
                "orders **788** · items **2010** · events **2420**.",
                "Si filtras también los CX* no te saldrá 788.",
            ),
        *paso(
                "4",
                "Escribe staging (Parquet)",
                "Parquet guarda el schema. El siguiente módulo no vuelve a inferir CSV.",
                """STAGING.mkdir(parents=True, exist_ok=True)
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
print("releer orders", spark.read.parquet(str(STAGING / "orders_clean")).count())""",
                "Cinco carpetas bajo `data/staging/`. Releer `orders_clean` = 788 y `order_ts` timestamp.",
                "Si escribes CSV “para verlo”, pierdes tipos.",
            ),
        md(
            comprueba(
                """En `orders_clean` e `items_clean`, nulos de `order_id` / `customer_id` / `product_id` → **0**.
Pedidos **788**. Líneas **2010**."""
            )
        ),
        *reto(
                "Filas que tiraste",
                "Imprime `antes - después` de cada regla (pedidos, líneas, eventos, productos). Markdown que interprete cada diferencia.",
                """```text
orders     800 - 788 = 12   (customer_id vacío)
items     2046 - 2010 = 36  (21 sin producto ∪ 15 qty 0)
events    2500 - 2420 = 80
products    60 -  57 = 3
```""",
            ),
        md(
            errores(
                [
                    ("788 no sale", "Filtraste también los CX*", "Esos 8 se quedan; solo quitas customer_id vacío"),
                    ("items ≠ 2010", "Filtros a medias", "Una sola where: producto no vacío **y** qty > 0"),
                    ("Staging ilegible", "Escribiste CSV", "Parquet; para espiar: `spark.read.parquet(...).show()`"),
                ]
            )
        ),
        md(siguiente("../M03-transformacion-datos/01-teoria.ipynb", "M03 — teoría")),
    ]


def m03_01() -> list:
    return [
        md(
            lab_abre(
                "M03-01",
                "Enriquecimiento",
                "M03-01-enriquecimiento.ipynb",
                "Añadir `gmv_line` y `order_month` al cruce líneas ⋈ pedidos y detectar GMV negativo (descuento sucio).",
                "01-teoria.ipynb",
                "03-lab-reglas-negocio.ipynb",
            )
        ),
        *paso(
                "1",
                "Arranque y staging",
                "Leo Parquet de M02-03. El schema ya viaja; no re-inferimos.",
                CELDA_0
                + """

spark = get_spark("novashop-m03")
orders = spark.read.parquet(str(STAGING / "orders_clean"))
items = spark.read.parquet(str(STAGING / "order_items_clean"))
print(orders.count(), items.count())""",
                "`788 2010`.",
                "Si falla el path, no has escrito staging. Vuelve a M02-03.",
            ),
        *paso(
                "2",
                "Inner para tener fecha",
                "order_month vive en la cabecera. Inner: las líneas de los 12 pedidos sin cliente no entran.",
                """from pyspark.sql.functions import col

lines = items.join(orders, "order_id", "inner")
print(lines.count())""",
                "**1980** filas (2010 − 30 líneas de pedidos descartados).",
                "Si haces left desde items te quedas en 2010.",
            ),
        *paso(
                "3",
                "GMV y mes",
                "La fórmula es una columna. Si discount es 1.50, el GMV sale negativo: suciedad que tapas en el siguiente lab.",
                """from pyspark.sql.functions import date_format

lines = (
    lines.withColumn(
        "gmv_line",
        col("qty") * col("unit_price") * (1 - col("discount")),
    ).withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
)
lines.select("order_id", "qty", "unit_price", "discount", "gmv_line", "order_month").show(5)
print("gmv nulos", lines.where(col("gmv_line").isNull()).count())
print("gmv < 0", lines.where(col("gmv_line") < 0).count())""",
                "`gmv nulos 0` · `gmv < 0` **13**. `order_month` tipo string `2024-01` … `2024-12`.",
                "No “arregles” el negativo aquí. Quieres verlo.",
            ),
        md(
            comprueba(
                """Cero nulos de `gmv_line` en las 1980 filas; 12 meses de 2024; **13** GMV negativos.
Deja `lines` en el notebook: lo usas en M03-02 (o rehaz estos 3 pasos)."""
            )
        ),
        *reto(
                "Pedido de alto valor",
                "Crea `is_high_value` = `gmv_line >= 500` y cuenta los true (GMV aún sin capar).",
                """```python
lines.withColumn("is_high_value", col("gmv_line") >= 500).where("is_high_value").count()
# 506
```""",
            ),
        md(
            errores(
                [
                    ("2010 tras el join", "Hiciste left", "Inner contra orders_clean → 1980"),
                    ("gmv_line string raro", "No casteaste unit_price en M02", "Relee el staging"),
                    ("order_month nulo", "order_ts no parseó", "Vuelve a M02-02 (coalesce de dos formatos)"),
                ]
            )
        ),
        md(siguiente("03-lab-reglas-negocio.ipynb", "M03-02 reglas")),
    ]


def m03_02() -> list:
    return [
        md(
            lab_abre(
                "M03-02",
                "Reglas de negocio",
                "M03-02-reglas-negocio.ipynb",
                "Capar el descuento, normalizar el canal, marcar lo cobrable y persistir `data/staging/fact_lines`.",
                "02-lab-enriquecimiento.ipynb",
                "../M04-integracion-agregacion/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Reconstruye `lines` (1980)",
                "Este notebook es autónomo: repito lectura + join + gmv_line + order_month.",
                CELDA_0
                + """

from pyspark.sql.functions import col, date_format

spark = get_spark("novashop-m03")
orders = spark.read.parquet(str(STAGING / "orders_clean"))
items = spark.read.parquet(str(STAGING / "order_items_clean"))
lines = (
    items.join(orders, "order_id", "inner")
    .withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
    .withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
)
print(lines.count(), lines.where(col("gmv_line") < 0).count())""",
                "1980 filas y 13 GMV negativos (punto de partida).",
                "Si empiezas “en el aire”, no sabes si el capado funcionó.",
            ),
        *paso(
                "2",
                "Tres reglas y recalcular GMV",
                "least(..., 1) evita GMV negativo. marketplace/WEB/App no sirven para un groupBy. Recalculo gmv_line AL FINAL.",
                """from pyspark.sql.functions import when, lower, least, lit

fact = (
    lines.withColumn("discount", least(col("discount"), lit(1.0)))
    .withColumn(
        "channel_norm",
        when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel")))
        .otherwise(lit("other")),
    )
    .withColumn("is_billable", col("status") == "paid")
    .withColumn(
        "gmv_line",
        col("qty") * col("unit_price") * (1 - col("discount")),
    )
)
print("filas", fact.count())
print("gmv < 0", fact.where(col("gmv_line") < 0).count())""",
                "1980 filas; `gmv_line < 0` pasa a **0**.",
                "Si capas el discount *después* de calcular GMV y no recalculas, siguen los 13 negativos.",
            ),
        *paso(
                "3",
                "Valida el dominio",
                "Un set cerrado se comprueba con groupBy, no a ojo.",
                """fact.groupBy("channel_norm").count().orderBy("channel_norm").show()
print("discount > 1", fact.where(col("discount") > 1).count())
print("billable", fact.where(col("is_billable")).count())""",
                "Canales solo `app`, `other`, `store`, `web`. `discount > 1` → **0**. Líneas cobrables **1127**.",
                "El fact guarda las 1980; el flag decide en M04. No filtres `is_billable` al escribir.",
            ),
        *paso(
                "4",
                "Escribe fact_lines",
                "M04 parte de este fact. CSV perdería tipos y el boolean.",
                """dest = STAGING / "fact_lines"
fact.write.mode("overwrite").parquet(str(dest))
print(spark.read.parquet(str(dest)).count())""",
                "**1980** al releer.",
                "M04 parte de este fact. CSV perdería tipos y el boolean.",
                extra="Opcional: `fact.explain(\"formatted\")` y señala el join. Es el puente a M06.",
            ),
        md(
            comprueba(
                """`discount <= 1` en todas las filas; `channel_norm` ⊆ {web, app, store, other}; count 1980.
Tres checks en verde en *tu* notebook (cada uno con Markdown)."""
            )
        ),
        *reto(
                "El plan incluye el join",
                "Lanza `fact.explain(\"formatted\")` y señala (en Markdown) la línea del join / Exchange.",
                "En el plan físico aparece un BroadcastHashJoin o SortMergeJoin con `order_id`. Si no lo ves, estás explicando `lines` *antes* del join.",
            ),
        md(
            errores(
                [
                    ("Siguen 13 GMV negativos", "No recalculaste gmv_line", "Recalcula al final de la cadena"),
                    ("channel_norm tiene WEB", "Faltó lower", "`lower(col(\"channel\"))` antes del isin"),
                    ("1127 no sale", "Filtraste is_billable al escribir", "El fact guarda 1980"),
                ]
            )
        ),
        md(siguiente("../M04-integracion-agregacion/01-teoria.ipynb", "M04 — teoría")),
    ]
