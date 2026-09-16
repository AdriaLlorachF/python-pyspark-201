"""Sesión Spark reutilizable para los laboratorios NovaShop."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

# Etiqueta del connector (los jars están en labs/_shared/jars/; no uses spark.jars.packages:
# Ivy no resuelve el rango de versiones del POM y el gateway Java se cae).
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"
_JARS_DIR = Path(__file__).resolve().parent / "jars"


def mongo_uri() -> str:
    return os.environ.get("MONGO_URI", "mongodb://mongo:27017")


def mongo_spark_jars() -> str:
    jars = sorted(_JARS_DIR.glob("*.jar"))
    if len(jars) < 4:
        raise FileNotFoundError(
            f"Faltan jars Mongo en {_JARS_DIR}. Terminal: python3 scripts/fetch_mongo_jars.py"
        )
    return ",".join(str(p) for p in jars)


def get_spark(app_name: str = "novashop", packages: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.port", "4040")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if packages:
        builder = builder.config("spark.jars", mongo_spark_jars())
    return builder.getOrCreate()


def prefetch_mongo_connector() -> None:
    spark = get_spark("prefetch-mongo", packages=MONGO_SPARK_PACKAGE)
    spark.stop()
