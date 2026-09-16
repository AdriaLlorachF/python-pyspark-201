"""Sesión Spark reutilizable para los laboratorios NovaShop."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Connector 10.x ↔ Spark 3.5 / Scala 2.12. Se baja en setup.sh para no esperar en clase.
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.2"


def mongo_uri() -> str:
    return os.environ.get("MONGO_URI", "mongodb://mongo:27017")


def get_spark(app_name: str = "novashop", packages: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.port", "4040")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


def prefetch_mongo_connector() -> None:
    spark = get_spark("prefetch-mongo", packages=MONGO_SPARK_PACKAGE)
    spark.stop()
