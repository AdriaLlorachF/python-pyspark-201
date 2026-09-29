"""Labs M04–M07: guion paso a paso (tú creas el notebook)."""
from __future__ import annotations

from .common import CELDA_0, comprueba, errores, lab_abre, md, paso, prueba, reto, siguiente


def m04_01() -> list:
    return [
        md(
            lab_abre(
                "M04-01",
                "Joins",
                "M04-01-joins.ipynb",
                "Medir cuántas líneas y pedidos se pierden al hacer inner contra clientes, y listar los huérfanos.",
                "01-teoria.ipynb",
                "03-lab-kpis.ipynb",
            )
        ),
        *paso(
                "1",
                "Carga fact y clientes",
                "El fact ya trae customer_id de la cabecera. Leo Parquet, no CSV.",
                CELDA_0
                + """

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
print(fact.count(), customers.count())""",
                "`1980 250`.",
                "Si falta fact_lines, cierra M03-02 primero.",
            ),
        *paso(
                "2",
                "Inner frente a left",
                "La diferencia ES el síntoma de las claves huérfanas. Cuento los dos.",
                """inner = fact.join(customers, "customer_id", "inner")
left = fact.join(customers, "customer_id", "left")
print("inner", inner.count(), "left", left.count())""",
                "inner **1956** · left **1980**.",
                "Si el inner sale mayor que 1980, el join de catálogo te ha duplicado (no lo hagas aquí).",
            ),
        *paso(
                "3",
                "Anti-join de huérfanos",
                "left_anti = está en el fact y no en clientes. Mejor que un where a ciegas.",
                """orphans = fact.join(customers, "customer_id", "left_anti")
orphans.select("order_id", "customer_id").distinct().orderBy("order_id").show()
print("líneas", orphans.count(), "pedidos", orphans.select("order_id").distinct().count())""",
                "**24** líneas · **8** pedidos · `customer_id` tipo `CX*`.",
                "Si no ves CX*, los filtraste en M02-03: regenera staging.",
                extra="Opcional: left a `products_clean` por `product_id`. Las líneas `P999` aparecen con `name` nulo: mismo patrón.",
            ),
        md(
            comprueba(
                "`fact.count() - inner.count()` → **24** líneas (8 pedidos). Anótalo en Markdown."
            )
        ),
        *reto(
                "Mismo patrón a grano pedido",
                "Inner/left de `orders_clean` ⋈ `customers_clean`. Markdown que compare con el grano línea.",
                """```python
orders = spark.read.parquet(str(STAGING / "orders_clean"))
print("orders", orders.count())
print("inner", orders.join(customers, "customer_id", "inner").count())  # 780
print("left ", orders.join(customers, "customer_id", "left").count())   # 788
```""",
            ),
        md(
            errores(
                [
                    ("Inner > 1980", "Productos duplicados en otro join", "`dropDuplicates` en product_id"),
                    ("No veo CX*", "Los filtraste en M02-03", "Regenera staging: solo quitas customer_id vacío"),
                    ("customer_id ambiguo", "Join mal nombrado", "Usa `join(..., \"customer_id\")`"),
                ]
            )
        ),
        md(siguiente("03-lab-kpis.ipynb", "M04-02 KPIs")),
    ]


def m04_02() -> list:
    return [
        md(
            lab_abre(
                "M04-02",
                "KPIs",
                "M04-02-kpis.ipynb",
                "Calcular GMV cobrable, nº de pedidos cobrables, ticket medio y tasa de cancelación sobre el universo **con cliente real**.",
                "02-lab-joins.ipynb",
                "04-lab-segmentacion.ipynb",
            )
        ),
        *paso(
                "1",
                "Universo de venta",
                "KPI de dinero ≠ KPI de operativa. Inner a clientes y solo is_billable para el dinero.",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
)
print(sales.count())""",
                "**1122** líneas cobrables con cliente (1127 − 5 paid huérfanas).",
                "Si usas left, atribuyes GMV a CX*.",
            ),
        *paso(
                "2",
                "Cuatro métricas globales",
                "El ticket medio se calcula a grano pedido: sum(GMV) / countDistinct(order_id), no avg de línea.",
                """from pyspark.sql.functions import sum as fsum, countDistinct, round as fround

kpis = sales.agg(
    fround(fsum("gmv_line"), 2).alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
kpis = kpis.withColumn("aov", fround(col("gmv") / col("orders"), 2))
kpis.show()""",
                "GMV ≈ **400157.73** · pedidos cobrables **469** · AOV ≈ **853**.",
                "Si casteaste a double, el céntimo puede moverse: redondea a 2 decimales.",
            ),
        *paso(
                "3",
                "Tasa de cancelación",
                "El denominador es pedidos (no líneas). Sobre orders_clean inner clientes (780).",
                """from pyspark.sql.functions import avg

orders = spark.read.parquet(str(STAGING / "orders_clean"))
ord_ok = orders.join(customers, "customer_id", "inner")
cancel = ord_ok.agg(
    avg((col("status") == "cancelled").cast("double")).alias("cancel_rate")
)
cancel.show()
print("pedidos con cliente", ord_ok.count())""",
                "≈ **0.22**. Pedidos con cliente **780**.",
                "Si mides sobre `sales` (solo paid), la tasa sale 0.",
            ),
        *paso(
                "4",
                "KPI por canal",
                "channel_norm (no channel) evita partir web/WEB. Ordeno por GMV.",
                """(
    sales.groupBy("channel_norm")
    .agg(
        fround(fsum("gmv_line"), 2).alias("gmv"),
        countDistinct("order_id").alias("orders"),
    )
    .orderBy(col("gmv").desc())
    .show()
)""",
                "Cuatro filas (`app`, `other`, `store`, `web`). `web` o `app` en cabeza.",
                "Este groupBy es el cuadro de mando.",
            ),
        md(
            comprueba(
                """Reproduce `gmv / countDistinct(order_id)` solo con is_billable e inner.
Un número ~850, no ~350 (eso sería media de línea). Escríbelo en Markdown."""
            )
        ),
        *reto(
                "GMV por mes y país",
                "`groupBy(\"order_month\", \"country\")` con la misma regla cobrable. `UNK` aparece si no rellenaste país.",
                """```python
(
    sales.groupBy("order_month", "country")
    .agg(fround(fsum("gmv_line"), 2).alias("gmv"))
    .orderBy("order_month", "country")
    .show(20)
)
```""",
            ),
        md(
            errores(
                [
                    ("GMV ~ 2×", "Join al catálogo duplicado", "`dropDuplicates([\"product_id\"])`"),
                    ("AOV ridículamente bajo", "`avg(\"gmv_line\")`", "`sum / countDistinct(order_id)`"),
                    ("Cancel rate 0", "Mediste sobre sales (solo paid)", "Usa orders_clean"),
                ]
            )
        ),
        md(siguiente("04-lab-segmentacion.ipynb", "M04-03 segmentación")),
    ]


def m04_03() -> list:
    return [
        md(
            lab_abre(
                "M04-03",
                "Segmentación",
                "M04-03-segmentacion.ipynb",
                "Clasificar clientes con venta cobrable en low / mid / high según GMV y contar cada banda.",
                "03-lab-kpis.ipynb",
                "../M05-analisis-avanzado/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "GMV por cliente",
                "La segmentación es una agregación DESPUÉS de fijar el grano. Parto de sales cobrable.",
                CELDA_0
                + """

from pyspark.sql.functions import col, sum as fsum, countDistinct, when, lit

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = fact.join(customers, "customer_id", "inner").where(col("is_billable"))
customer_gmv = sales.groupBy("customer_id", "country", "segment").agg(
    fsum("gmv_line").alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
customer_gmv.orderBy(col("gmv").desc()).show(5)
print(customer_gmv.count())""",
                "~**211** clientes con al menos un paid.",
                "Si agregas *todos* los clientes con left, inflas con GMV nulo.",
            ),
        *paso(
                "2",
                "Bandas de negocio",
                "Umbrales explícitos: <1000 low, <3000 mid, resto high. Encadena when bien (no solapes).",
                """banded = customer_gmv.withColumn(
    "value_band",
    when(col("gmv") < 1000, lit("low"))
    .when(col("gmv") < 3000, lit("mid"))
    .otherwise(lit("high")),
)
banded.groupBy("value_band").count().orderBy("value_band").show()""",
                "`high` ≈ 40 · `low` ≈ 56 · `mid` ≈ 115. Suma = count de customer_gmv.",
                "Los quintiles (`ntile`) van en la mejora, no aquí.",
            ),
        *paso(
                "3",
                "Guarda para M05",
                "M05 rankea sobre este grano sin recalcular el GMV.",
                """banded.write.mode("overwrite").parquet(str(STAGING / "customer_gmv"))
print(spark.read.parquet(str(STAGING / "customer_gmv")).count())""",
                "Carpeta `data/staging/customer_gmv` y el mismo count (~211).",
                "overwrite para poder repetir el lab.",
            ),
        md(
            comprueba(
                "`low + mid + high` debe igualar `customer_gmv.count()`. Una sola cifra, sin clientes en dos bandas."
            )
        ),
        *reto(
                "Quintiles",
                "Usa `ntile(5)` sobre `gmv` (ventana global `orderBy(gmv)`) y cuenta cada quintil. Sin partitionBy: ranking de la compañía.",
                """```python
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

w = Window.orderBy(col("gmv"))
customer_gmv.withColumn("q", ntile(5).over(w)).groupBy("q").count().orderBy("q").show()
```""",
            ),
        md(
            errores(
                [
                    ("250 clientes en las bandas", "Left con GMV nulo", "Parte de sales cobrable"),
                    ("Un cliente en two bands", "Whens solapados", "`< 1000` luego `< 3000` luego otherwise"),
                ]
            )
        ),
        md(siguiente("../M05-analisis-avanzado/01-teoria.ipynb", "M05 — teoría")),
    ]


def m05_01() -> list:
    return [
        md(
            lab_abre(
                "M05-01",
                "Ranking por ventana",
                "M05-01-ranking-ventana.ipynb",
                """La misma ventana de la teoría, a tamaño NovaShop: top 10 clientes por GMV y, **dentro de cada cliente**, sus 3 productos que más dinero dejan.

No hace falta memorizar la API. En cada paso: ejecuta → mira `rn` → **cambia un número o quita un `partitionBy`** y vuelve a ejecutar. Si solo pegas, no has visto la ventana.""",
                "01-teoria.ipynb",
                "03-lab-acumulados.ipynb",
            )
        ),
        *paso(
                "1",
                "Top 10 de la compañía",
                """Parte de `customer_gmv` (M04-03: una fila por cliente con venta cobrable). Sin `partitionBy`, el ranking es **de toda la empresa**: un solo `rn=1`.

`row_number` pone 1 al GMV más alto (`orderBy desc`), 2 al siguiente, etc. El `where rn <= 10` es el top.""",
                CELDA_0
                + """

from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window

spark = get_spark("novashop-m05")
# Una fila = un cliente (sale de M04-03). Si PATH falla: rehaz ese lab o run_pipeline no basta
# (customer_gmv lo escribes tú en M04-03).
cust = spark.read.parquet(str(STAGING / "customer_gmv"))
print("clientes con GMV cobrable", cust.count())

# Sin partitionBy: un único ranking para toda la tabla
w_global = Window.orderBy(col("gmv").desc())
top10 = (
    cust.withColumn("rn", row_number().over(w_global))  # 1 = el que más factura
    .where(col("rn") <= 10)
)
top10.orderBy("rn").show()""",
                "10 filas, `rn` de 1 a 10, GMV hacia abajo. El nº 1 ronda **6000 €**.",
                "Si no tienes `customer_gmv`, no es M05: vuelve a M04-03 (groupBy cliente).",
                if_fail="PATH not found → el Parquet vive en `data/staging/customer_gmv` (lo escribes en el lab de segmentación).",
            ),
        *prueba(
                "Cambia el corte del top",
                "En la celda de arriba, cambia `<= 10` por `<= 3` y vuelve a ejecutar. Luego prueba `<= 1`.",
                """print("filas top3", top10.where(col("rn") <= 3).count())  # 3
# ¿El customer_id del rn=1 sigue siendo el mismo que con top 10?
top10.where(col("rn") == 1).select("customer_id", "gmv").show()""",
                "`<= 3` da 3 filas. El nº 1 **no cambia** (solo recortas). Si cambia, reordenaste mal.",
            ),
        *paso(
                "2",
                "Top 3 productos **por** cliente",
                """Ahora el ranking se **reinicia** en cada persona. Eso es `partitionBy("customer_id")`.

Antes hay que **juntar líneas del mismo producto**: si rankeas el fact a palo seco, la misma SKU sale muchas veces (una por línea). Por eso `groupBy(customer_id, product_id)` y luego la ventana.""",
                """fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))

# Dinero cobrable de cada par cliente+producto (ya no es grano línea)
product_gmv = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "product_id")
    .agg(fsum("gmv_line").alias("gmv"))
)

# El rn vuelve a 1 en CADA customer_id
w_prod = Window.partitionBy("customer_id").orderBy(col("gmv").desc())
top3 = (
    product_gmv.withColumn("rn", row_number().over(w_prod))
    .where(col("rn") <= 3)
)

# Mira solo al cliente que era nº 1 de la compañía
top_id = top10.select("customer_id").first()["customer_id"]
print("cliente nº 1 de la compañía:", top_id)
top3.where(col("customer_id") == top_id).orderBy("rn").show()
print("filas top3 (todos los clientes)", top3.count())""",
                "Como mucho 3 filas por cliente; `rn` 1–3. `filas top3` ≤ 211 × 3. El nº 1 de *ese* cliente es un producto, no el ranking global.",
                "Si ves 30 filas del mismo cliente, rankeaste líneas: faltó el groupBy producto.",
            ),
        *prueba(
                "Quita el partitionBy del top 3",
                "Crea `w_mal = Window.orderBy(col(\"gmv\").desc())` (sin partitionBy), calcula `rn` y filtra `rn <= 3`. Compáralo con `top3`.",
                """w_mal = Window.orderBy(col("gmv").desc())  # ranking de TODA la empresa
mal = product_gmv.withColumn("rn", row_number().over(w_mal)).where(col("rn") <= 3)
print("sin partitionBy, filas", mal.count())  # 3 en total, no 3 por cliente
mal.show()
print("con partitionBy, filas", top3.count())""",
                "Sin `partitionBy`: **3 filas en toda la tabla**. Con él: cientos (3 por cliente). Anota los dos counts en Markdown.",
            ),
        md(
            comprueba(
                """Elige un `customer_id` con varios productos y mira sus `rn`.
Empiezan en **1** (no continúan el 1–10 de la compañía). Markdown: id + tres filas.

También: count sin `partitionBy` vs con él (prueba de arriba)."""
            )
        ),
        *reto(
                "rank vs row_number",
                "Sobre `cust`, añade columnas `row_number`, `rank` y `dense_rank` con el mismo `w_global`. Si hay empate de GMV se ve el salto. Markdown: qué salta y qué no.",
                """```python
from pyspark.sql.functions import rank, dense_rank

cmp_ = (
    cust.withColumn("rn", row_number().over(w_global))
    .withColumn("rk", rank().over(w_global))
    .withColumn("dr", dense_rank().over(w_global))
    .orderBy(col("gmv").desc())
)
cmp_.select("customer_id", "gmv", "rn", "rk", "dr").show(15)
```""",
            ),
        md(
            errores(
                [
                    ("Un solo rn=1 en todo el fact", "Olvidaste partitionBy", "Añádelo para “por cliente”"),
                    ("Top 3 con 30 filas del mismo cliente", "Rankeaste líneas", "groupBy cliente+producto antes"),
                    ("Window sin orderBy", "Ranking indefinido", "Siempre ordena la métrica"),
                    ("No está customer_gmv", "Saltaste M04-03", "Ese lab escribe el Parquet"),
                ]
            )
        ),
        md(siguiente("03-lab-acumulados.ipynb", "M05-02 acumulados")),
    ]


def m05_02() -> list:
    return [
        md(
            lab_abre(
                "M05-02",
                "Acumulados por entidad",
                "M05-02-acumulados.ipynb",
                """Numerar los pedidos de cada cliente (`order_n`) y el GMV cobrable **acumulado** en el tiempo (`gmv_running`).

Misma ventana que la teoría: `partitionBy(cliente)` + `orderBy(fecha)`. Si `gmv_running` baja dentro de un cliente, el orden está mal — no lo copies: **compruébalo**.""",
                "02-lab-ranking-ventana.ipynb",
                "../M06-optimizacion-ejecucion/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Primero: grano pedido (no línea)",
                """Un pedido con 3 productos no es 3 visitas. Si rankeas o acumulas el fact a palo seco, `order_n` cuenta **líneas**.

Por eso agrupas a `order_id`: fecha del pedido = `min(order_ts)`, dinero = `sum(gmv_line)`.""",
                CELDA_0
                + """

from pyspark.sql.functions import col, min as fmin, sum as fsum, row_number

spark = get_spark("novashop-m05")
orders_gmv = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "order_id")
    .agg(
        fmin("order_ts").alias("order_ts"),  # un instante por ticket
        fsum("gmv_line").alias("gmv"),       # dinero de todas las líneas del ticket
    )
)
print("pedidos cobrables con cliente", orders_gmv.count())""",
                "**469** (el mismo count que el KPI de M04-02). Si salen **1122**, no agregaste a `order_id`: estás en grano línea.",
                "469 tickets ≠ 1122 líneas. El acumulado “por visita” vive en el ticket.",
            ),
        *prueba(
                "¿Qué pasa si no agrupas?",
                "Cuenta el fact cobrable+inner **sin** el `groupBy` de `order_id`. Compáralo con 469.",
                """lineas = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
)
print("líneas", lineas.count(), "pedidos distintos", lineas.select("order_id").distinct().count())""",
                "`líneas` **1122**, `pedidos distintos` **469**. Si usas 1122 como “nº de pedido”, estás inflando visitas.",
            ),
        *paso(
                "2",
                "Número de pedido y acumulado",
                """Una sola window para las dos columnas: el vecindario es el cliente; el eje es el tiempo.

`row_number` → 1.er, 2.º, 3.er ticket de **esa** persona.
`sum(gmv).over(w)` → dinero desde el primer ticket **hasta este** (incluido).""",
                """from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_ts")
hist = (
    orders_gmv.withColumn("order_n", row_number().over(w))
    .withColumn("gmv_running", fsum("gmv").over(w))
)
hist.orderBy("customer_id", "order_n").show(12)""",
                "`order_n` 1, 2, 3… **por cliente**. `gmv_running` no decrece dentro del mismo `customer_id`.",
                "Si baja, el `orderBy` de la window no es `order_ts` (o está descendente).",
            ),
        *prueba(
                "Un cliente concreto",
                "Elige un `customer_id` que en el `show` tenga `order_n` ≥ 2. Filtra solo ese id y mira si la fila 2 tiene `gmv_running` ≥ fila 1. Anota id y las dos filas en Markdown.",
                """# Cambia el id por uno que hayas visto con varios pedidos
cid = hist.where(col("order_n") >= 2).select("customer_id").first()["customer_id"]
print("cliente", cid)
hist.where(col("customer_id") == cid).orderBy("order_n").show()""",
                "Al menos dos filas. `gmv_running` de `order_n=2` ≥ el de `order_n=1`. Si no, el orden de la ventana está al revés.",
            ),
        *paso(
                "3",
                "Primera compra vs repetición",
                """`order_n == 1` es la definición de “nuevo” en este curso: primer ticket cobrable de ese cliente. El resto son repeticiones.""",
                """hist.groupBy((col("order_n") == 1).alias("is_first")).count().show()""",
                "~211 primeras compras (`true`: un cliente con paid) y el resto `false` (repeticiones). 211 + repeticiones = 469.",
                "Sin `partitionBy`, `order_n=1` sería **una sola fila en toda la empresa**.",
            ),
        *prueba(
                "Ventana de toda la empresa",
                "Repite el paso 2 con `w_emp = Window.orderBy(\"order_ts\")` (sin partitionBy). Cuenta cuántos `order_n == 1` hay.",
                """w_emp = Window.orderBy("order_ts")  # un solo ranking temporal global
hist_emp = orders_gmv.withColumn("order_n", row_number().over(w_emp))
print("order_n=1 sin partitionBy", hist_emp.where(col("order_n") == 1).count())  # 1
print("order_n=1 con partitionBy", hist.where(col("order_n") == 1).count())     # ~211""",
                "Sin `partitionBy`: **1**. Con él: ~**211**. Esa diferencia *es* la ventana.",
            ),
        md(
            comprueba(
                """Un cliente con `order_n` ≥ 2: `gmv_running` fila 2 ≥ fila 1. Markdown con el id.

Counts: 469 pedidos; ~211 primeros; `order_n=1` global (sin partitionBy) = 1."""
            )
        ),
        *reto(
                "Pedidos hasta superar 1000 €",
                "Quédate, por cliente, con la **primera** fila donde `gmv_running >= 1000` (o ninguna si no llega). Markdown: ¿`order_n` 1 o hace falta el 2.º ticket?",
                """```python
w2 = Window.partitionBy("customer_id").orderBy("order_ts")
crossed = hist.where(col("gmv_running") >= 1000)
first_cross = crossed.withColumn("rn", row_number().over(w2)).where(col("rn") == 1)
first_cross.select("customer_id", "order_n", "gmv_running").show()
print("clientes que cruzan 1000", first_cross.count())
```""",
            ),
        md(
            errores(
                [
                    ("gmv_running igual en todas las filas", "Window sin orderBy", "partitionBy + orderBy(order_ts)"),
                    ("1122 “pedidos”", "No agregaste a order_id", "Paso 1"),
                    ("Acumulado a nivel empresa", "Falta partitionBy", "Añádelo"),
                    ("order_n=1 solo una vez", "Ventana global", "partitionBy(customer_id)"),
                ]
            )
        ),
        md(siguiente("../M06-optimizacion-ejecucion/01-teoria.ipynb", "M06 — teoría")),
    ]

def m06_01() -> list:
    return [
        md(
            lab_abre(
                "M06-01",
                "Explain y DAG",
                "M06-01-explain-dag.ipynb",
                """Separar **tres cosas** que en la UI se parecen:

1. **Plan** — la receta (`explain` la imprime; **no** lee el Parquet).
2. **Job** — una ejecución de verdad (`count`, `show`, `write`).
3. **DAG** — el dibujo de ese job en Spark UI. Solo existe **después** de una acción.

Al terminar: un `print` del DataFrame no mueve Jobs; un `count` sí; y en el `explain` sabes señalar el *Scan* y el *Filter*.""",
                "01-teoria.ipynb",
                "03-lab-cache-particionado.ipynb",
            )
        ),
        md(
            """## Léelo antes de copiar código (si no, el lab no se entiende)

Spark es **vago**: `where` / `select` / `read` **encadenan** trabajo, no lo hacen. Hasta que no lanzas una **acción**, no hay recorrido de ficheros ni número de filas.

| Escribes | Nombre | ¿Lee `fact_lines`? | ¿Sale un número / tabla? |
|----------|--------|--------------------|---------------------------|
| `df.where(...)` | transformación | no | no |
| `print(df)` | enseñar el **objeto** | no | el tipo y las columnas, no el count |
| `df.explain(...)` | imprimir el **plan** | no | un muro de texto (la receta) |
| `df.count()` / `df.show()` | **acción** | sí | un entero / filas |

`explain` **no** es una acción. No llena cache, no crea job, no sube el Job Id. Sirve para leer el mapa **antes** (o después) de viajar.

### Qué significa cada palabro del plan

No memorices Catalyst. En el texto de `explain("formatted")` busca **estas** palabras:

| Si ves… | Quiere decir… |
|---------|----------------|
| *FileScan parquet* / *Scan parquet* | “voy a abrir este directorio Parquet” (ahí sale la ruta `fact_lines`) |
| *PushedFilters* | “estos `where` se los mando al scan” (no leo todo y filtro después del todo) |
| *Filter* | un `where` que aún se aplica como paso |
| *Project* | un `select` / columnas que se quedan |
| *HashAggregate* | un `groupBy` / `count` / `sum` |
| *InMemoryTableScan* | “esto lo leo del **cache**”, no del Parquet (eso es el lab siguiente) |

### Spark UI (útil, no obligatorio)

Pestaña **Ports** del Codespace → puerto **4040** (si está muerto, otra sesión ocupó el puerto: `spark.stop()` y `get_spark` otra vez; a veces cae en 4041).

- **Jobs**: cada `count`/`show` añade una fila. Un `print(df)` o un `explain` **no**.
- **Storage**: este lab no cachea; debe seguir vacío.
- El **DAG** es el dibujo que se abre al hacer clic en un job. Es el de **esa** acción, no del `explain`.

Si no te abre la UI, da igual: este lab se demuestra con `print` vs `count` vs `explain` en el notebook.

Necesitas `data/staging/fact_lines` (M03-02 o `python3 scripts/run_pipeline.py`)."""
        ),
        *paso(
                "1",
                "Encadenar no ejecuta",
                """Celda 0 + sesión + un DataFrame con **tres** `where` y un `select`.

`print(planned)` enseña el objeto (`DataFrame[order_id: …]`). Eso **no** es el número de filas. Spark aún no ha abierto el Parquet (salvo, a veces, para leer el *schema*; eso no cuenta como “ya filtró”).

Si tienes UI: anota el Job Id más alto **antes** de ejecutar, y otra vez **después**. No debe subir.""",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m06")

# read + 3 where + select = plan más largo. Todavía NO hay job.
planned = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .where(col("is_billable"))                          # cobrable
    .where(col("gmv_line") > 0)                         # GMV positivo
    .where(col("channel_norm").isin("web", "app"))      # solo esos canales
    .select("order_id", "customer_id", "gmv_line", "order_month")
)

# Objeto, no resultado. Tiene que salir algo tipo DataFrame[order_id: string, ...]
print("¿qué es?", type(planned).__name__)
print(planned)""",
                "`DataFrame[...]` con esas cuatro columnas. **Ningún** entero tipo 800. El Job Id de la UI no sube (si la tienes).",
                "Si `print(planned)` ya te diera 1127, estarías ejecutando un `count` sin darte cuenta.",
                if_fail="PATH / AnalysisException de `fact_lines` → no está el Parquet de M03. Corre el pipeline o cierra M03-02.",
            ),
        *prueba(
                "print no cuenta; count sí",
                "En *otra* celda, imprime otra vez `planned` y **después** `planned.count()`. Mira qué línea da el entero.",
                """print("otra vez el objeto:", planned)
n = planned.count()
print("ahora sí, filas =", n)""",
                "La primera línea sigue siendo el DataFrame. La segunda es un **entero** (varios cientos: cobrable + GMV>0 + web/app). Si tienes UI, **aquí** aparece un job nuevo y un DAG.",
            ),
        *paso(
                "2",
                "Una acción = un job (y entonces hay DAG)",
                """`count()` recorre las particiones, aplica los tres filtros y devuelve un número.

Eso es el viaje: Spark UI → **Jobs** → el job nuevo → clic → **DAG**. El DAG es el dibujo de *este* count, no de los `where` del paso 1.

Si cada celda te crea un job, tienes un `.show()` de debug por medio: coméntalo mientras mides.""",
                """# Acción: ahora sí lee fact_lines y filtra
n = planned.count()
print("líneas cobrables web/app con GMV > 0 =", n)
print("(vuelve a dar el mismo número: el plan no ha cambiado)")""",
                "Un entero **> 0**, del orden de varios cientos. Un job nuevo en la UI. El DAG tiene al menos un stage.",
                "Sin acción no hay DAG que mirar. El `explain` del paso 3 es el mapa en texto, no ese dibujo.",
            ),
        *paso(
                "3",
                "`explain` imprime el mapa (no el resultado)",
                """`explain("formatted")` **planifica** (resuelve nombres, optimiza) y **imprime**. No cuenta filas.

En la salida, **no** leas de arriba abajo como un libro. Busca con Ctrl+F:

1. `fact_lines` o `FileScan` / `Scan parquet` → de dónde salen las filas.
2. `is_billable` o `gmv_line` o `channel_norm` o `PushedFilters` / `Filter` → tus `where`.
3. `Project` → el `select` de cuatro columnas.

Copia a Markdown **dos** líneas: una del scan y una del filtro. Con eso basta.

`explain` no debe crear job nuevo.""",
                """# Mapa, no viaje. Job Id no debería subir.
planned.explain("formatted")""",
                "Texto largo con Scan/FileScan y la ruta `fact_lines`. Los predicados de los `where` aparecen (en *Filter* o en *PushedFilters*). No sale el entero del count.",
                "Si no ves `is_billable`, estás explain-ando **otro** DataFrame (uno sin ese `where`).",
            ),
        *prueba(
                "Un where extra no lanza job",
                "Encadena **otro** `where` **sin** `count`. Imprime el objeto y, si quieres, un `explain`. Luego mira Jobs.",
                """# Transformación de más: el plan crece, el job no
mas_estrecho = planned.where(col("gmv_line") > 50)
print("sigue siendo un objeto:", mas_estrecho)
# mas_estrecho.explain("formatted")  # opcional: un Filter/PushedFilters más
print("si tienes UI: el Job Id más alto no cambia respecto al count del paso 2")""",
                "Otro `DataFrame[...]`. Cero jobs nuevos. El count de `mas_estrecho` (si lo lanzas) será **menor** que el de `planned`; eso ya sería una acción nueva.",
            ),
        md(
            comprueba(
                """Markdown en *tu* notebook con:

- una frase: `print(planned)` no lee datos; `count()` sí; `explain` imprime el plan.
- las **dos** líneas del explain (scan + filtro).
- si usaste UI: Job Id antes del primer count vs después (solo sube con el count)."""
            )
        ),
        *reto(
                "formatted vs extended",
                """`explain("formatted")` es el físico, legible. `explain(True)` (o `explain("extended")`) suelta **cuatro** capas: parsed → analyzed → optimized → physical. Más ruido.

Ejecuta los dos. En el físico / formatted, ¿ves `PushedFilters` junto al FileScan? Eso es “el `where` se empujó al scan”. Anótalo.

Si el filtro **no** aparece, lo pusiste *después* de un `select` que ya tiró la columna.""",
                """print("===== formatted (el de clase) =====")
planned.explain("formatted")
print("===== extended (cuatro capas; busca Physical Plan y PushedFilters) =====")
planned.explain(True)""",
            ),
        md(
            errores(
                [
                    ("UI vacía / 404", "Puerto 4040 no reenviado o sesión en 4041", "Ports → 4040; o `spark.stop()` + `get_spark()`"),
                    ("Cada celda crea un job", "Tienes un `show`/`count` de debug", "Coméntalo mientras mides"),
                    ("Dos sesiones", "`SparkSession()` a mano", "Solo `get_spark()`"),
                    ("explain peta AnalysisException", "Columna mal escrita", "El plan también resuelve nombres: corrige el typo"),
                    ("No está fact_lines", "Saltaste M03", "`run_pipeline.py` o lab M03-02"),
                ]
            )
        ),
        md(siguiente("03-lab-cache-particionado.ipynb", "M06-02 cache")),
    ]


def m06_02() -> list:
    return [
        md(
            lab_abre(
                "M06-02",
                "Cache y particionado",
                "M06-02-cache-particionado.ipynb",
                """Dos ideas **distintas** (no las mezcles):

1. **Cache** — “guarda este resultado en memoria para no repetir el plan”. Se llena con una **acción**, no al escribir `.cache()`. La prueba **no** es que el `count` salga 9016 dos veces (eso es normal). La prueba es Storage **0 → 0 → 1 → 1 → 0**.
2. **`repartition`** — baraja filas en **memoria** a N trozos. No crea carpetas en disco (eso es `write.partitionBy` en M07) ni es el `partitionBy` de las ventanas (M05).""",
                "02-lab-explain-dag.ipynb",
                "../M07-persistencia-datos/01-teoria.ipynb",
            )
        ),
        md(
            """## Léelo antes (si no, “no demuestra nada”)

### Palabras que se parecen y no son lo mismo

| Lo que escribes | Dónde vive | Para qué |
|-----------------|------------|----------|
| `Window.partitionBy("customer_id")` (M05) | receta de la ventana | recortar el **vecindario** (por cliente) |
| `df.repartition(12, col("order_month"))` | **memoria**, ahora | barajar a 12 trozos (shuffle) |
| `write.partitionBy("order_month")` (M07) | **disco**, carpetas | `order_month=2024-01/` |
| `df.cache()` | memoria, **después** de una acción | no repetir un plan caro |

### Cómo se demuestra el cache (no mires el 9016)

El número de filas **tiene** que ser igual con cache y sin él. Cache no cambia el resultado; cambia **de dónde** sale la segunda lectura.

| Momento | Storage (pestaña o `n_en_storage()`) | Qué significa |
|---------|--------------------------------------|---------------|
| DataFrame creado | **0** | no hay nada guardado |
| Acabas de escribir `.cache()` | **0** | solo **marcó**; aún no calculó |
| 1.er `count` | **1** | esa acción **llenó** memoria |
| 2.º `count` | **1** | reutiliza; no soltó |
| `unpersist()` | **0** | suelta (si no, el Codespace se llena) |

Los **segundos** del cronómetro en local a veces no se inmutan. Si el reloj miente, Storage no.

Spark UI: Ports **4040** → pestaña **Storage**. Jobs: el 1.er count hace Scan+Filter; el 2.º debería verse más corto / *InMemory*.

Necesitas `fact_lines` (M03). No subas de 8 copias: OOM.""",
        ),
        *paso(
                "1",
                "El fact de siempre + qué es una partición",
                """Una **partición** (aquí) es un **trozo de trabajo en memoria**, no una carpeta. `getNumPartitions()` dice en cuántos trozos está **ahora** este DataFrame.

Leemos las 1980 líneas. Aún no hay cache.""",
                CELDA_0
                + """

from pyspark.sql.functions import col, lit
from time import perf_counter

spark = get_spark("novashop-m06")
spark.catalog.clearCache()  # por si una celda anterior dejó basura

def n_en_storage():
    # Cuántos RDD hay en memoria (= pestaña Storage). 0 vacío, ≥1 hay cache lleno.
    return int(spark.sparkContext._jsc.getPersistentRDDs().size())

base = spark.read.parquet(str(STAGING / "fact_lines"))
print("filas fact_lines", base.count())                 # 1980
print("particiones ahora", base.rdd.getNumPartitions())  # > 1 en local[*]
print("Storage (tiene que ser 0)", n_en_storage())""",
                "`filas fact_lines 1980`. Particiones **> 1**. Storage **0**.",
                "Si sale PATH error, no es este lab: falta M03-02 / pipeline.",
                if_fail="1980 no sale → regenera staging. Storage ≠ 0 → `spark.catalog.clearCache()` y reejecuta.",
            ),
        *paso(
                "2",
                "Por qué apilamos 8 copias (lupa, no producción)",
                """1980 filas (como las 20 de la teoría) son tan pocas que el 1.er y el 2.º `count` duran igual y parece que el cache “no hace nada”.

`unionByName` **apila** la misma tabla 8 veces: 1980 × 8 = **15840** filas. `_copy` marca de qué copia sale cada fila. No es un patrón de pipeline; es para que Storage tenga algo que mostrar y el job dure un poco.

`range(7)` + la base con `_copy = -1` → 8 copias en total.""",
                """xl = base.withColumn("_copy", lit(-1))
for i in range(7):
    xl = xl.unionByName(base.withColumn("_copy", lit(i)))

print("filas apiladas", xl.count())                       # 15840
print("particiones tras el union", xl.rdd.getNumPartitions())
print("Storage sigue", n_en_storage())                    # 0: aún no hay cache""",
                "**15840**. Storage **0**. Varias particiones.",
                "Si cuentas 1980, el bucle no se ejecutó (reusas `base` en vez de `xl`).",
            ),
        *paso(
                "3",
                "Dos counts **fríos** (sin cache): el plan se repite",
                """Sin cache, cada `count` vuelve a leer Parquet + unions + filtro cobrable.

`timed_count` mide reloj **y** imprime filas. Los dos tiempos son del **mismo orden**. El número **9016** las dos veces no demuestra cache: demuestra que el filtro es el mismo (1127 cobrables × 8).

Storage sigue en 0: no hemos marcado nada.""",
                """def timed_count(df, label):
    t0 = perf_counter()
    n = df.count()
    print(label, "n =", n, "s =", round(perf_counter() - t0, 3),
          "Storage =", n_en_storage())
    return n

# Filtro cobrable AQUÍ (no dentro de timed_count: si no, no se entiende qué cacheas)
billable = xl.where(col("is_billable"))

timed_count(billable, "1er count frío")
timed_count(billable, "2º count frío")""",
                "Las dos veces **n = 9016**. Storage **0** y **0**. Tiempos parecidos (si no, mira Jobs: son dos jobs del mismo estilo, Scan otra vez).",
                "Frío = cada acción rehace el plan. Aún no hay atajo.",
            ),
        *paso(
                "4",
                "`.cache()` **solo marca** — Storage sigue vacío",
                """`cache()` quiere decir: “la **próxima** acción, guarda el resultado”. Hasta que no haya `count`/`show`, Storage = 0.

Ejecuta **esta** celda y **párate**. Mira UI → Storage (vacío) y el print. No lances el count todavía.""",
                """warm = billable.cache()
print("acabo de escribir cache(); Storage =", n_en_storage())  # 0
print("storageLevel (la intención, no los datos):", warm.storageLevel)
print("PARA AQUÍ. Storage tiene que seguir vacío.")""",
                "Storage **0**. `storageLevel` ya dice MEMORY (eso es la marca, no el llenado).",
                "Si aquí ya ves 1, el kernel tenía un cache viejo: `clearCache()` y desde el paso 1.",
            ),
        *paso(
                "5",
                "El 1.er count llena; el 2.º reutiliza",
                """Ahora sí: la primera acción **materializa**. La segunda debería leer memoria.

El 9016 **otra vez** es correcto: cache no cambia el resultado. Lo que cambia es Storage 0→**1** y, en `explain`, *InMemoryTableScan* / *InMemoryRelation*.

No hagas `unpersist` en esta celda: si no, al terminar Storage vuelve a 0 y “no se ve nada”.""",
                """timed_count(warm, "1º count (LLENA el cache)")
print("   → Storage tiene que ser 1. Mira también UI → Storage.")

timed_count(warm, "2º count (lee cache, mismo n)")
print("   → Storage sigue 1 (no lo soltó)")

print("--- Plan del 2º count: busca InMemoryTableScan / InMemoryRelation ---")
warm.explain("formatted")""",
                "1º: n=9016, Storage **1**. 2º: n=9016, Storage **1**. En el explain aparece *InMemory*. El reloj del 2º no empeora (a veces es igual de corto: fíate de Storage).",
                "Dos 9016 no son la demo. Un Storage que pasa a 1 sí.",
            ),
        *paso(
                "6",
                "`unpersist` suelta (otra celda, siempre)",
                """Si no sueltas, el Codespace se queda con el fact×8 en memoria. `unpersist()` vacía Storage.

Jupyter a veces pinta `DataFrame[...]` porque `unpersist` **devuelve** el DataFrame: ignóralo, o deja el `print` al final.""",
                """warm.unpersist()
print("después de unpersist, Storage =", n_en_storage())  # 0
print("listo")""",
                "Storage **0**. UI → Storage vacío otra vez.",
                "Si vuelves a `warm.count()` ahora, **recalcula** (frío) y, como ya no está cacheado, Storage sigue 0.",
            ),
        *prueba(
                "Cache sin acción = Storage vacío",
                "Sin pegar el paso 5: `otra = billable.cache()` y `print(n_en_storage())`. Luego un `count` y otra vez el print. `unpersist` al terminar.",
                """otra = billable.cache()
print("solo cache(), Storage =", n_en_storage())  # 0
_ = otra.count()
print("tras count, Storage =", n_en_storage())    # ≥ 1
otra.unpersist()
print("tras unpersist, Storage =", n_en_storage())  # 0""",
                "**0**, luego **≥ 1**, luego **0**. Escríbelo en Markdown. Eso es todo el lab de cache.",
            ),
        md(
            """## Parte B — `repartition` (esto **no** es cache)

Ya sabes llenar y soltar memoria. Ahora: **cómo está troceado** el DataFrame en RAM.

`repartition(12, col("order_month"))` **baraja** (shuffle) hacia 12 particiones, una intención por mes. Sirve de cara a **escribir** en M07. No crea las carpetas todavía.

Si omites el `12` y pones solo la columna, Spark 3.5 usa **200** particiones por defecto: inútil aquí."""
        ),
        *paso(
                "7",
                "12 particiones por mes (shuffle en memoria)",
                """Vuelve a cachear `billable` un momento (el paso 6 lo soltó) o reparte sobre `billable` directo.

`getNumPartitions() == 12`. El `groupBy("order_month")` debe listar **12** meses de 2024.

Esto **no** es `Window.partitionBy`. Esto **no** es `write.partitionBy`.""",
                """# El cache del paso 5 ya se soltó; no hace falta para contar particiones
by_month = billable.repartition(12, col("order_month"))
print("particiones", by_month.rdd.getNumPartitions())  # 12

(
    by_month.groupBy("order_month")
    .count()
    .orderBy("order_month")
    .show()
)""",
                "`particiones 12`. Doce filas de meses `2024-01` … `2024-12`.",
                "Si ves 200, llamaste `repartition(col(\"order_month\"))` **sin** el 12.",
            ),
        md(
            comprueba(
                """Markdown con la tabla Storage de tu ejecución: 0 (tras cache sin count), 1 (tras 1.er count), 1 (tras 2.º), 0 (unpersist).

Una frase: el 9016 igual las dos veces **no** es la prueba.

`getNumPartitions()` tras el paso 7 = **12**."""
            )
        ),
        *reto(
                "coalesce(1) vs repartition(1)",
                """Los dos dejan 1 partición, pero no igual de caro:

- `repartition(1)` **siempre** shufflea (`Exchange` en el plan).
- `coalesce(1)` **reduce** trozos sin un shuffle amplio.

Útil para un único fichero de entrega; mal hábito si lo pones en mitad del pipeline (M07). Ejecuta los dos `explain` y copia la línea donde uno dice *Exchange* y el otro no.""",
                """print("===== coalesce(1) (sin shuffle amplio) =====")
billable.coalesce(1).explain("formatted")
print("===== repartition(1) (shuffle: busca Exchange) =====")
billable.repartition(1).explain("formatted")
print("particiones coalesce", billable.coalesce(1).rdd.getNumPartitions())
print("particiones repartition", billable.repartition(1).rdd.getNumPartitions())""",
            ),
        md(
            errores(
                [
                    ("Los dos count salen 9016 y “no demuestra nada”", "El número tiene que ser igual", "Mira Storage 0→1→1→0, no el 9016"),
                    ("Storage vacío tras cache()", "No hubo acción, o unpersist en la misma celda", "count en celda aparte; unpersist después"),
                    ("Storage = 1 al escribir cache()", "Cache viejo en el kernel", "`spark.catalog.clearCache()`"),
                    ("OOM / el kernel muere", "Demasiadas copias + cache", "Quédate en 8; `unpersist`"),
                    ("1980 o 200 particiones en el paso 7", "`repartition` sin `12`", "`repartition(12, col(\"order_month\"))`"),
                    ("UI en 4041 / vacía", "Otra SparkSession ocupó 4040", "`spark.stop()`; Ports 4040"),
                ]
            )
        ),
        md(siguiente("../M07-persistencia-datos/01-teoria.ipynb", "M07 — teoría")),
    ]


def m07_01() -> list:
    return [
        md(
            lab_abre(
                "M07-01",
                "Parquet y layout analítico",
                "M07-01-parquet-layout.ipynb",
                "Publicar `data/curated/sales_analytics` en Parquet particionado por mes y demostrar que un filtro de mes no lee el año entero.",
                "01-teoria.ipynb",
                "../../README.md",
            )
        ),
        *paso(
                "1",
                "Dataset curated",
                "Left al catálogo conserva P999. Inner a clientes quita CX*. Solo paid. dropDuplicates en product_id.",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m07")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
products = (
    spark.read.parquet(str(STAGING / "products_clean"))
    .dropDuplicates(["product_id"])
)
sales = (
    fact.join(customers, "customer_id", "inner")
    .join(products, "product_id", "left")
    .where(col("is_billable"))
    .select(
        "order_id", "order_ts", "order_month",
        "customer_id", "country", "segment",
        "product_id", "category",
        "qty", "unit_price", "discount", "gmv_line", "channel_norm",
    )
)
print(sales.count())""",
                "**1122** filas (mismo universo que M04-02).",
                "Si sale 2244, el catálogo no era único.",
            ),
        *paso(
                "2",
                "Escribe Parquet por mes",
                "overwrite deja el curated idempotente. Doce particiones = doce meses de 2024.",
                """dest = CURATED / "sales_analytics"
CURATED.mkdir(parents=True, exist_ok=True)
(
    sales.write.mode("overwrite")
    .partitionBy("order_month")
    .parquet(str(dest))
)
print(sorted(p.name for p in dest.iterdir() if p.is_dir()))""",
                "Carpetas `order_month=2024-01` … `order_month=2024-12` (más `_SUCCESS`).",
                "Esto es layout de disco, no el repartition de M06.",
            ),
        *paso(
                "3",
                "Prune al leer un mes",
                "El plan debe listar solo marzo (o PartitionFilters: order_month=2024-03).",
                """marzo = spark.read.parquet(str(dest)).where(col("order_month") == "2024-03")
marzo.explain("formatted")
print("marzo", marzo.count(), "total", spark.read.parquet(str(dest)).count())""",
                "Total **1122**. `marzo` es un subconjunto. El formatted menciona `2024-03`.",
                "Copia en Markdown la línea del PartitionFilters.",
            ),
        *paso(
                "4",
                "CSV vs Parquet (schema, no solo tamaño)",
                "coalesce(1) solo existe aquí para comparar *un* CSV, no como patrón. Releo los dos schemas.",
                """import os

csv_dir = CURATED / "_csv_compare"
sales.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_dir))

def du(path):
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

print("parquet", du(dest), "csv", du(csv_dir))
spark.read.parquet(str(dest)).printSchema()
spark.read.option("header", True).csv(str(csv_dir)).printSchema()""",
                "Parquet mantiene `decimal`/`timestamp`. El CSV vuelve a string. El tamaño: Parquet suele ganar; en este volumen a veces es parecido.",
                "Curated en CSV “para el analista” pierde tipos.",
            ),
        md(
            comprueba(
                """Vuelve a ejecutar el `write.mode(\"overwrite\")` y cuenta.
Sigue **1122**. No se duplica. Anótalo."""
            )
        ),
        *reto(
                "Dos claves de partición",
                "Copia `sales_analytics_geo` con `partitionBy(\"order_month\", \"country\")` y lee marzo ∧ ES. No particiones por customer_id.",
                """```python
geo = CURATED / "sales_analytics_geo"
sales.write.mode("overwrite").partitionBy("order_month", "country").parquet(str(geo))
(
    spark.read.parquet(str(geo))
    .where((col("order_month") == "2024-03") & (col("country") == "ES"))
    .explain("formatted")
)
```""",
            ),
        md(
            errores(
                [
                    ("Miles de part-000xx", "repartition(200) residual", "repartition(12, order_month) antes del write"),
                    ("Count 2244", "append o join duplicado", "overwrite + dropDuplicates de productos"),
                    ("order_month no está al leer", "API antigua", "spark.read.parquet de 3.5 sí la incluye"),
                ]
            )
        ),
        md(siguiente("../../README.md", "índice del curso")),
    ]
