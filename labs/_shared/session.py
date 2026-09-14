"""Sesión Spark reutilizable para los laboratorios NovaShop."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "novashop") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.port", "4040")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
