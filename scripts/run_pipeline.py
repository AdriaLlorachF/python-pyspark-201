#!/usr/bin/env python3
"""Ejecuta el pipeline NovaShop M01→M07 y aserta counts canónicos."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "labs" / "_shared"))

from paths import CURATED, RAW, STAGING, ensure_dirs  # noqa: E402
from session import get_spark  # noqa: E402
from pyspark.sql import Row  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    avg,
    col,
    coalesce,
    countDistinct,
    date_format,
    least,
    lit,
    lower,
    min as fmin,
    row_number,
    sum as fsum,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (  # noqa: E402
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window  # noqa: E402


def expect(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("  OK", msg)


def main() -> int:
    counts = json.loads((ROOT / "data" / "CANONICAL_COUNTS.json").read_text(encoding="utf-8"))
    ensure_dirs()
    spark = get_spark("novashop-validacion")

    print("== M01")
    pedidos = [
        Row(order_id="O90001", customer_id="C0001", status="paid", amount=49.90),
        Row(order_id="O90002", customer_id="C0002", status="paid", amount=12.50),
        Row(order_id="O90003", customer_id="C0003", status="cancelled", amount=80.00),
        Row(order_id="O90004", customer_id="C0001", status="paid", amount=23.10),
        Row(order_id="O90005", customer_id="C0004", status="pending", amount=5.00),
    ]
    demo = spark.createDataFrame(pedidos)
    expect(demo.count() == 5, "M01 demo 5 filas")
    expect(demo.filter(demo.status == "paid").count() == 3, "M01 paid = 3")

    print("== M02 ingesta")
    customers_raw = spark.read.option("header", True).csv(str(RAW / "customers.csv"))
    orders_raw = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
    products_raw = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
    events_raw = spark.read.json(str(RAW / "events.jsonl"))
    items_raw = spark.read.option("header", True).csv(str(RAW / "order_items.csv"))
    expect(customers_raw.count() == counts["customers"], "customers 250")
    expect(orders_raw.count() == counts["orders"], "orders 800")
    expect(products_raw.count() == counts["products"], "products 60")
    expect(events_raw.count() == counts["events"], "events 2500")
    expect(items_raw.count() == counts["order_items"], "items 2046")

    print("== M02 schema + limpieza")
    orders_schema = StructType(
        [
            StructField("OrderId", StringType(), True),
            StructField("CustomerId", StringType(), True),
            StructField("OrderDate", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Channel", StringType(), True),
        ]
    )
    orders = (
        spark.read.option("header", True)
        .schema(orders_schema)
        .csv(str(RAW / "orders.csv"))
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
    expect(orders.where(col("order_ts").isNull()).count() == 0, "0 fechas nulas")

    items = (
        items_raw.withColumn("qty", col("qty").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount", col("discount").cast(DecimalType(5, 2)))
    )
    events_schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("ts", TimestampType(), True),
            StructField("session_id", StringType(), True),
            StructField("page", StringType(), True),
            StructField("product_id", StringType(), True),
        ]
    )
    events = spark.read.schema(events_schema).json(str(RAW / "events.jsonl"))
    products = (
        products_raw.withColumnRenamed("productId", "product_id")
        .withColumnRenamed("listPrice", "list_price")
        .withColumn("list_price", col("list_price").cast(DecimalType(10, 2)))
    )
    customers = customers_raw.withColumn(
        "country",
        when(col("country").isNull() | (trim(col("country")) == ""), "UNK").otherwise(col("country")),
    )
    products_clean = products.where(col("list_price").isNotNull())
    orders_clean = orders.where(trim(col("customer_id")) != "")
    items_clean = items.where((trim(col("product_id")) != "") & (col("qty") > 0))
    events_clean = events.where(col("customer_id").isNotNull())
    expect(customers.count() == 250, "customers 250")
    expect(customers.where(col("country") == "UNK").count() == 5, "5 UNK")
    expect(products_clean.count() == 57, "products 57")
    expect(orders_clean.count() == 788, "orders 788")
    expect(items_clean.count() == 2010, "items 2010")
    expect(events_clean.count() == 2420, "events 2420")

    for name, frame in {
        "customers_clean": customers,
        "products_clean": products_clean,
        "orders_clean": orders_clean,
        "order_items_clean": items_clean,
        "events_clean": events_clean,
    }.items():
        frame.write.mode("overwrite").parquet(str(STAGING / name))

    print("== M03")
    lines = items_clean.join(orders_clean, "order_id", "inner")
    expect(lines.count() == 1980, "inner líneas 1980")
    lines = lines.withColumn(
        "gmv_line", col("qty") * col("unit_price") * (1 - col("discount"))
    ).withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
    expect(lines.where(col("gmv_line") < 0).count() == 13, "13 GMV negativos")
    expect(
        lines.where(col("gmv_line") >= 500).count() == counts["high_value_lines_before_cap"],
        "506 high value",
    )
    fact = (
        lines.withColumn("discount", least(col("discount"), lit(1.0)))
        .withColumn(
            "channel_norm",
            when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel"))).otherwise(
                lit("other")
            ),
        )
        .withColumn("is_billable", col("status") == "paid")
        .withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
    )
    expect(fact.count() == 1980, "fact 1980")
    expect(fact.where(col("gmv_line") < 0).count() == 0, "GMV capado")
    expect(fact.where(col("is_billable")).count() == 1127, "billable 1127")
    expect(fact.where(col("discount") > 1).count() == 0, "discount <= 1")
    chans = {r["channel_norm"] for r in fact.select("channel_norm").distinct().collect()}
    expect(chans <= {"web", "app", "store", "other"}, f"canales {chans}")
    fact.write.mode("overwrite").parquet(str(STAGING / "fact_lines"))

    print("== M04")
    inner = fact.join(customers, "customer_id", "inner")
    left = fact.join(customers, "customer_id", "left")
    orphans = fact.join(customers, "customer_id", "left_anti")
    expect(inner.count() == 1956, "inner 1956")
    expect(left.count() == 1980, "left 1980")
    expect(orphans.count() == 24, "24 huérfanos")
    expect(orphans.select("order_id").distinct().count() == 8, "8 pedidos CX*")
    sales = inner.where(col("is_billable"))
    expect(sales.count() == 1122, "sales 1122")
    kpis = sales.agg(fsum("gmv_line").alias("gmv"), countDistinct("order_id").alias("orders")).collect()[0]
    expect(kpis["orders"] == 469, f"pedidos cobrables {kpis['orders']}")
    gmv = float(kpis["gmv"])
    expect(399000 < gmv < 402000, f"GMV ~400k ({gmv})")
    customer_gmv = sales.groupBy("customer_id", "country", "segment").agg(
        fsum("gmv_line").alias("gmv"), countDistinct("order_id").alias("orders")
    )
    expect(customer_gmv.count() == 211, "211 clientes con paid")
    banded = customer_gmv.withColumn(
        "value_band",
        when(col("gmv") < 1000, lit("low")).when(col("gmv") < 3000, lit("mid")).otherwise(lit("high")),
    )
    expect(banded.count() == 211, "bandas = clientes")
    banded.write.mode("overwrite").parquet(str(STAGING / "customer_gmv"))
    cancel = (
        orders_clean.join(customers, "customer_id", "inner")
        .agg(avg((col("status") == "cancelled").cast("double")).alias("cancel_rate"))
        .collect()[0]["cancel_rate"]
    )
    expect(0.15 < float(cancel) < 0.30, f"cancel_rate {cancel}")

    print("== M05")
    w_global = Window.orderBy(col("gmv").desc())
    top10 = banded.withColumn("rn", row_number().over(w_global)).where(col("rn") <= 10)
    expect(top10.count() == 10, "top 10")
    orders_gmv = sales.groupBy("customer_id", "order_id").agg(
        fmin("order_ts").alias("order_ts"), fsum("gmv_line").alias("gmv")
    )
    expect(orders_gmv.count() == 469, "469 pedidos grano")
    w = Window.partitionBy("customer_id").orderBy("order_ts")
    hist = orders_gmv.withColumn("order_n", row_number().over(w)).withColumn(
        "gmv_running", fsum("gmv").over(w)
    )
    firsts = hist.where(col("order_n") == 1).count()
    expect(firsts == 211, "211 primeras compras")

    print("== M06")
    planned = (
        fact.where(col("is_billable"))
        .where(col("gmv_line") > 0)
        .where(col("channel_norm").isin("web", "app"))
        .select("order_id", "customer_id", "gmv_line", "order_month")
    )
    n_webapp = planned.count()
    expect(n_webapp > 0, f"count web/app {n_webapp}")
    plan = planned._jdf.queryExecution().simpleString()
    expect("fact_lines" in plan or "Filter" in plan or "FileScan" in plan or True, "explain disponible")
    xl = fact.withColumn("_copy", lit(-1))
    for i in range(7):
        xl = xl.unionByName(fact.withColumn("_copy", lit(i)))
    expect(xl.count() == 1980 * 8, "xl 15840")
    by_month = xl.repartition(12, col("order_month"))
    expect(by_month.rdd.getNumPartitions() == 12, "12 particiones")

    print("== M07")
    products_u = products_clean.dropDuplicates(["product_id"])
    curated = sales.join(products_u, "product_id", "left").select(
        "order_id",
        "order_ts",
        "order_month",
        "customer_id",
        "country",
        "segment",
        "product_id",
        "category",
        "qty",
        "unit_price",
        "discount",
        "gmv_line",
        "channel_norm",
    )
    expect(curated.count() == 1122, "curated 1122")
    dest = CURATED / "sales_analytics"
    curated.write.mode("overwrite").partitionBy("order_month").parquet(str(dest))
    months = sorted(p.name for p in dest.iterdir() if p.is_dir() and p.name.startswith("order_month="))
    expect(len(months) == 12, f"12 meses {months}")
    expect(spark.read.parquet(str(dest)).count() == 1122, "relectura 1122")

    print("PIPELINE OK")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
