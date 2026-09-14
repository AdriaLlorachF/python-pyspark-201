#!/usr/bin/env python3
"""Genera notebooks/_qa/*.ipynb (referencia ejecutable)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from nbutil import CELDA_0, code, md, write_notebook  # noqa: E402

OUT = ROOT / "notebooks" / "_qa"


def build_all() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    specs = [
        ("00-entorno.ipynb", entorno()),
        ("M01-01-sesion-spark.ipynb", m01_01()),
        ("M02-01-ingesta-csv-json.ipynb", m02_01()),
        ("M02-02-schema-tipos.ipynb", m02_02()),
        ("M02-03-calidad-limpieza.ipynb", m02_03()),
        ("M03-01-enriquecimiento.ipynb", m03_01()),
        ("M03-02-reglas-negocio.ipynb", m03_02()),
        ("M04-01-joins.ipynb", m04_01()),
        ("M04-02-kpis.ipynb", m04_02()),
        ("M04-03-segmentacion.ipynb", m04_03()),
        ("M05-01-ranking-ventana.ipynb", m05_01()),
        ("M05-02-acumulados.ipynb", m05_02()),
        ("M06-01-explain-dag.ipynb", m06_01()),
        ("M06-02-cache-particionado.ipynb", m06_02()),
        ("M07-01-parquet-layout.ipynb", m07_01()),
        ("99-pipeline-completo.ipynb", pipeline()),
    ]
    for name, cells in specs:
        dest = OUT / name
        write_notebook(dest, cells)
        print("wrote", dest.relative_to(ROOT))


def _head(title: str, lab: str) -> list:
    return [
        md(f"# {title}\n\nReferencia de validación. El alumno trabaja en `notebooks/alumno/{lab}`."),
        md("## Celda 0 — localizar el repo"),
        code(CELDA_0),
    ]


def entorno() -> list:
    return [
        md("# 00 — Entorno Codespace"),
        code(CELDA_0),
        code(
            """import shutil, subprocess, sys, pyspark
print("python", sys.executable, sys.version.split()[0])
print("pyspark", pyspark.__version__)
print(subprocess.check_output(["java", "-version"], text=True, stderr=subprocess.STDOUT).splitlines()[0])
print("java", shutil.which("java"))
assert RAW.is_dir(), "falta data/raw — python3 scripts/generate_novashop.py"
assert (RAW / "orders.csv").is_file()
print("raw ok")"""
        ),
        code(
            """spark = get_spark("novashop-entorno")
print(spark.version, spark.sparkContext.master)
assert spark.sparkContext.master.startswith("local")
spark.stop()
print("sesión ok")"""
        ),
    ]


def m01_01() -> list:
    return _head("M01-01 — Sesión Spark y primer DataFrame", "M01-01-sesion-spark.ipynb") + [
        md("## 1 — Runtime"),
        code(
            """import shutil, subprocess, pyspark
print("pyspark", pyspark.__version__)
print(subprocess.check_output(["java", "-version"], text=True, stderr=subprocess.STDOUT).splitlines()[0])
print("java:", shutil.which("java"))
assert pyspark.__version__.startswith("3.5")"""
        ),
        md("## 2 — Sesión"),
        code(
            """spark = get_spark("novashop-m01")
print(spark)
assert spark.sparkContext.master.startswith("local")"""
        ),
        md("## 3 — Pedidos en memoria"),
        code(
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
df.show()
assert df.count() == 5"""
        ),
        md("## 4 — filter vs count"),
        code(
            """paid = df.filter(df.status == "paid")
print("después del filter, Spark aún no ha contado nada")
print("paid count =", paid.count())
assert paid.count() == 3"""
        ),
        md("## Comprueba + reto"),
        code(
            """from pyspark.sql.functions import lit
print("spark.version", spark.version)
assert paid.count() == 3
df.withColumn("channel", lit("web")).select("order_id", "channel").show()
print("M01-01 OK")"""
        ),
    ]


def m02_01() -> list:
    return _head("M02-01 — Ingesta CSV/JSON", "M02-01-ingesta-csv-json.ipynb") + [
        code("spark = get_spark('novashop-m02')\nassert RAW.exists()"),
        md("## 2 — CSV"),
        code(
            """customers = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
orders = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("customers", customers.count(), "orders", orders.count())
orders.printSchema()
orders.show(3, truncate=False)
assert customers.count() == 250 and orders.count() == 800"""
        ),
        md("## 3 — JSON / JSONL"),
        code(
            """products = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()
events.printSchema()
assert products.count() == 60 and events.count() == 2500"""
        ),
        md("## Reto — order_items"),
        code(
            """items = spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
print(items.count())
items.show(3)
assert items.count() == 2046
print("M02-01 OK")"""
        ),
    ]


def m02_02() -> list:
    return _head("M02-02 — Schema y tipos", "M02-02-schema-tipos.ipynb") + [
        code("spark = get_spark('novashop-m02')"),
        md("## 1–2 — Pedidos + timestamp"),
        code(
            """from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, coalesce, to_timestamp
orders_raw_schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Channel", StringType(), True),
])
orders = (
    spark.read.option("header", True).schema(orders_raw_schema).csv(str(RAW / "orders.csv"))
    .withColumnRenamed("OrderId", "order_id")
    .withColumnRenamed("CustomerId", "customer_id")
    .withColumnRenamed("OrderDate", "order_ts_raw")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("Channel", "channel")
)
orders = orders.withColumn(
    "order_ts",
    coalesce(
        to_timestamp(col("order_ts_raw"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_ts_raw"), "dd/MM/yyyy"),
    ),
).drop("order_ts_raw")
nulos = orders.where(col("order_ts").isNull()).count()
print("nulos order_ts", nulos)
orders.printSchema()
assert nulos == 0
assert dict(orders.dtypes)["order_ts"] == "timestamp" """
        ),
        md("## 3 — Líneas decimal"),
        code(
            """from pyspark.sql.types import IntegerType, DecimalType
items = (
    spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    .withColumn("qty", col("qty").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
)
items.printSchema()
items.select("unit_price").limit(3).show()
assert dict(items.dtypes)["qty"] == "int"
assert dict(items.dtypes)["unit_price"].startswith("decimal")"""
        ),
        md("## 4 — Eventos + reto productos"),
        code(
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
assert events.count() == 2500
products = (
    spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    .withColumnRenamed("productId", "product_id")
    .withColumnRenamed("listPrice", "list_price")
    .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
)
products.printSchema()
print("M02-02 OK")"""
        ),
    ]


def m02_03() -> list:
    return _head("M02-03 — Calidad y limpieza", "M02-03-calidad-limpieza.ipynb") + [
        code(
            """spark = get_spark("novashop-m02")
from pyspark.sql.functions import col, coalesce, to_timestamp, when, trim
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType,
)
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
items = (
    spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    .withColumn("qty", col("qty").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
)
customers = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
products = (
    spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    .withColumnRenamed("productId", "product_id")
    .withColumnRenamed("listPrice", "list_price")
    .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
)
events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("product_id", StringType(), True),
])
events = spark.read.schema(events_schema).json(str(RAW / "events.jsonl"))"""
        ),
        md("## 1–2 — Reglas"),
        code(
            """customers_clean = customers.withColumn(
    "country", when(col("country").isNull() | (trim(col("country")) == ""), "UNK").otherwise(col("country")),
)
products_clean = products.where(col("list_price").isNotNull())
orders_clean = orders.where(trim(col("customer_id")) != "")
items_clean = items.where((trim(col("product_id")) != "") & (col("qty") > 0))
events_clean = events.where(col("customer_id").isNotNull())
print("customers", customers_clean.count(), "unk", customers_clean.where(col("country") == "UNK").count())
print("products", products_clean.count())
print("orders", orders_clean.count(), "items", items_clean.count(), "events", events_clean.count())
assert customers_clean.count() == 250
assert customers_clean.where(col("country") == "UNK").count() == 5
assert products_clean.count() == 57
assert orders_clean.count() == 788
assert items_clean.count() == 2010
assert events_clean.count() == 2420"""
        ),
        md("## 3 — Staging"),
        code(
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
    print(name, spark.read.parquet(str(dest)).count())
assert spark.read.parquet(str(STAGING / "orders_clean")).count() == 788
print("M02-03 OK")"""
        ),
    ]


def m03_01() -> list:
    return _head("M03-01 — Enriquecimiento", "M03-01-enriquecimiento.ipynb") + [
        code(
            """from pyspark.sql.functions import col, date_format
spark = get_spark("novashop-m03")
orders = spark.read.parquet(str(STAGING / "orders_clean"))
items = spark.read.parquet(str(STAGING / "order_items_clean"))
print(orders.count(), items.count())
assert orders.count() == 788 and items.count() == 2010
lines = items.join(orders, "order_id", "inner")
print("inner", lines.count())
assert lines.count() == 1980
lines = lines.withColumn(
    "gmv_line", col("qty") * col("unit_price") * (1 - col("discount"))
).withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
print("nulos", lines.where(col("gmv_line").isNull()).count())
print("negativos", lines.where(col("gmv_line") < 0).count())
assert lines.where(col("gmv_line").isNull()).count() == 0
assert lines.where(col("gmv_line") < 0).count() == 13
print("high_value", lines.where(col("gmv_line") >= 500).count())
assert lines.where(col("gmv_line") >= 500).count() == 506
lines.write.mode("overwrite").parquet(str(STAGING / "lines_enriched"))
print("M03-01 OK")"""
        ),
    ]


def m03_02() -> list:
    return _head("M03-02 — Reglas de negocio", "M03-02-reglas-negocio.ipynb") + [
        code(
            """from pyspark.sql.functions import col, when, lower, least, lit
spark = get_spark("novashop-m03")
lines = spark.read.parquet(str(STAGING / "lines_enriched"))
fact = (
    lines.withColumn("discount", least(col("discount"), lit(1.0)))
    .withColumn(
        "channel_norm",
        when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel"))).otherwise(lit("other")),
    )
    .withColumn("is_billable", col("status") == "paid")
    .withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
)
fact.groupBy("channel_norm").count().orderBy("channel_norm").show()
assert fact.count() == 1980
assert fact.where(col("gmv_line") < 0).count() == 0
assert fact.where(col("discount") > 1).count() == 0
assert fact.where(col("is_billable")).count() == 1127
chans = {r.channel_norm for r in fact.select("channel_norm").distinct().collect()}
assert chans <= {"web", "app", "store", "other"}
fact.write.mode("overwrite").parquet(str(STAGING / "fact_lines"))
assert spark.read.parquet(str(STAGING / "fact_lines")).count() == 1980
print("M03-02 OK")"""
        ),
    ]


def m04_01() -> list:
    return _head("M04-01 — Joins", "M04-01-joins.ipynb") + [
        code(
            """spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
print(fact.count(), customers.count())
inner = fact.join(customers, "customer_id", "inner")
left = fact.join(customers, "customer_id", "left")
orphans = fact.join(customers, "customer_id", "left_anti")
print("inner", inner.count(), "left", left.count())
orphans.select("order_id", "customer_id").distinct().orderBy("order_id").show()
print("líneas", orphans.count(), "pedidos", orphans.select("order_id").distinct().count())
assert fact.count() == 1980 and customers.count() == 250
assert inner.count() == 1956 and left.count() == 1980
assert orphans.count() == 24
assert orphans.select("order_id").distinct().count() == 8
orders = spark.read.parquet(str(STAGING / "orders_clean"))
assert orders.join(customers, "customer_id", "inner").count() == 780
assert orders.join(customers, "customer_id", "left").count() == 788
print("M04-01 OK")"""
        ),
    ]


def m04_02() -> list:
    return _head("M04-02 — KPIs", "M04-02-kpis.ipynb") + [
        code(
            """from pyspark.sql.functions import col, sum as fsum, countDistinct, round as fround, avg
spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = fact.join(customers, "customer_id", "inner").where(col("is_billable"))
print("sales", sales.count())
assert sales.count() == 1122
kpis = sales.agg(fround(fsum("gmv_line"), 2).alias("gmv"), countDistinct("order_id").alias("orders"))
kpis = kpis.withColumn("aov", fround(col("gmv") / col("orders"), 2))
kpis.show()
row = kpis.collect()[0]
assert row["orders"] == 469
assert 399000 < float(row["gmv"]) < 402000
orders = spark.read.parquet(str(STAGING / "orders_clean"))
ord_ok = orders.join(customers, "customer_id", "inner")
cancel = ord_ok.agg(avg((col("status") == "cancelled").cast("double")).alias("cancel_rate"))
cancel.show()
(
    sales.groupBy("channel_norm")
    .agg(fround(fsum("gmv_line"), 2).alias("gmv"), countDistinct("order_id").alias("orders"))
    .orderBy(col("gmv").desc())
    .show()
)
print("M04-02 OK")"""
        ),
    ]


def m04_03() -> list:
    return _head("M04-03 — Segmentación", "M04-03-segmentacion.ipynb") + [
        code(
            """from pyspark.sql.functions import col, sum as fsum, countDistinct, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile
spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = fact.join(customers, "customer_id", "inner").where(col("is_billable"))
customer_gmv = sales.groupBy("customer_id", "country", "segment").agg(
    fsum("gmv_line").alias("gmv"), countDistinct("order_id").alias("orders")
)
print(customer_gmv.count())
assert customer_gmv.count() == 211
banded = customer_gmv.withColumn(
    "value_band",
    when(col("gmv") < 1000, lit("low")).when(col("gmv") < 3000, lit("mid")).otherwise(lit("high")),
)
banded.groupBy("value_band").count().orderBy("value_band").show()
assert banded.count() == 211
banded.write.mode("overwrite").parquet(str(STAGING / "customer_gmv"))
w = Window.orderBy(col("gmv"))
customer_gmv.withColumn("q", ntile(5).over(w)).groupBy("q").count().orderBy("q").show()
print("M04-03 OK")"""
        ),
    ]


def m05_01() -> list:
    return _head("M05-01 — Ranking", "M05-01-ranking-ventana.ipynb") + [
        code(
            """from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window
spark = get_spark("novashop-m05")
cust = spark.read.parquet(str(STAGING / "customer_gmv"))
w_global = Window.orderBy(col("gmv").desc())
top10 = cust.withColumn("rn", row_number().over(w_global)).where(col("rn") <= 10)
top10.orderBy("rn").show()
assert top10.count() == 10
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
product_gmv = (
    fact.join(customers, "customer_id", "inner").where(col("is_billable"))
    .groupBy("customer_id", "product_id").agg(fsum("gmv_line").alias("gmv"))
)
w_prod = Window.partitionBy("customer_id").orderBy(col("gmv").desc())
top3 = product_gmv.withColumn("rn", row_number().over(w_prod)).where(col("rn") <= 3)
print("filas top3", top3.count())
assert top3.count() <= 211 * 3
print("M05-01 OK")"""
        ),
    ]


def m05_02() -> list:
    return _head("M05-02 — Acumulados", "M05-02-acumulados.ipynb") + [
        code(
            """from pyspark.sql.functions import col, min as fmin, sum as fsum, row_number
from pyspark.sql.window import Window
spark = get_spark("novashop-m05")
sales = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
)
orders_gmv = sales.groupBy("customer_id", "order_id").agg(
    fmin("order_ts").alias("order_ts"), fsum("gmv_line").alias("gmv")
)
print(orders_gmv.count())
assert orders_gmv.count() == 469
w = Window.partitionBy("customer_id").orderBy("order_ts")
hist = orders_gmv.withColumn("order_n", row_number().over(w)).withColumn("gmv_running", fsum("gmv").over(w))
hist.orderBy("customer_id", "order_n").show(12)
hist.groupBy((col("order_n") == 1).alias("is_first")).count().show()
assert hist.where(col("order_n") == 1).count() == 211
print("M05-02 OK")"""
        ),
    ]


def m06_01() -> list:
    return _head("M06-01 — Explain y DAG", "M06-01-explain-dag.ipynb") + [
        code(
            """from pyspark.sql.functions import col
spark = get_spark("novashop-m06")
planned = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .where(col("is_billable"))
    .where(col("gmv_line") > 0)
    .where(col("channel_norm").isin("web", "app"))
    .select("order_id", "customer_id", "gmv_line", "order_month")
)
print(planned)
n = planned.count()
print("count", n)
assert n > 0
planned.explain("formatted")
print("M06-01 OK")"""
        ),
    ]


def m06_02() -> list:
    return _head("M06-02 — Cache y particionado", "M06-02-cache-particionado.ipynb") + [
        code(
            """from pyspark.sql.functions import col, lit
spark = get_spark("novashop-m06")
base = spark.read.parquet(str(STAGING / "fact_lines"))
xl = base.withColumn("_copy", lit(-1))
for i in range(7):
    xl = xl.unionByName(base.withColumn("_copy", lit(i)))
print("particiones iniciales", xl.rdd.getNumPartitions())
assert xl.count() == 1980 * 8
import time
def timed_count(df, label):
    t0 = time.perf_counter()
    n = df.where(col("is_billable")).count()
    print(label, n, f"{time.perf_counter() - t0:.2f}s")
    return n
assert timed_count(xl, "1er count frío") == 9016
warm = xl.where(col("is_billable")).cache()
assert timed_count(warm, "calentamiento") == 9016
assert timed_count(warm, "caliente") == 9016
by_month = warm.repartition(12, col("order_month"))
print("particiones", by_month.rdd.getNumPartitions())
assert by_month.rdd.getNumPartitions() == 12
by_month.groupBy("order_month").count().orderBy("order_month").show()
warm.unpersist()
print("M06-02 OK")"""
        ),
    ]


def m07_01() -> list:
    return _head("M07-01 — Parquet curated", "M07-01-parquet-layout.ipynb") + [
        code(
            """from pyspark.sql.functions import col
spark = get_spark("novashop-m07")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
products = spark.read.parquet(str(STAGING / "products_clean")).dropDuplicates(["product_id"])
sales = (
    fact.join(customers, "customer_id", "inner")
    .join(products, "product_id", "left")
    .where(col("is_billable"))
    .select(
        "order_id", "order_ts", "order_month", "customer_id", "country", "segment",
        "product_id", "category", "qty", "unit_price", "discount", "gmv_line", "channel_norm",
    )
)
print(sales.count())
assert sales.count() == 1122
dest = CURATED / "sales_analytics"
CURATED.mkdir(parents=True, exist_ok=True)
sales.write.mode("overwrite").partitionBy("order_month").parquet(str(dest))
months = sorted(p.name for p in dest.iterdir() if p.is_dir() and p.name.startswith("order_month="))
print(months)
assert len(months) == 12
marzo = spark.read.parquet(str(dest)).where(col("order_month") == "2024-03")
marzo.explain("formatted")
print("marzo", marzo.count(), "total", spark.read.parquet(str(dest)).count())
assert spark.read.parquet(str(dest)).count() == 1122
print("M07-01 OK")"""
        ),
    ]


def pipeline() -> list:
    return [
        md("# 99 — Pipeline completo\n\nEquivale a `python3 scripts/run_pipeline.py`."),
        code(CELDA_0),
        code(
            """import runpy
script = ROOT / "scripts" / "run_pipeline.py"
ns = runpy.run_path(str(script), run_name="not_main")
# main se ejecuta solo si __name__ == '__main__'; llamamos a mano
raise_code = ns["main"]()
assert raise_code == 0
print("99 OK")"""
        ),
    ]


if __name__ == "__main__":
    build_all()
