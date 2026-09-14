# M01 — Fundamentos y entorno

[← Página anterior](../../README.md) · [Siguiente página →](M01-01-sesion-spark-primer-dataframe.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en el laboratorio.

## Qué aprenderás

- Distinguir Spark (motor de cómputo) de una base de datos y de Pandas.
- Crear una `SparkSession` en modo `local[*]` y reutilizarla.
- Separar **transformación** (plan) de **acción** (ejecución).
- Leer la primera salida de un DataFrame (`printSchema`, `show`, `count`).

## Teoría

Spark no guarda una hoja de cálculo en memoria como Excel o Pandas. Guarda un **plan**: qué lecturas y qué transformaciones harás cuando alguien pida un resultado.

| | Pandas | PySpark |
|---|--------|---------|
| Dónde viven los datos | RAM del proceso Python | Particiones (aquí: cores del Codespace) |
| Cuándo se calcula | En cada línea | Solo ante una **acción** |
| Índice de filas | Sí | No |
| Tope práctico | Lo que quepa en una máquina | Lo que quepa en el clúster (aquí: tu Codespace) |

**Cuándo Pandas basta:** un CSV de unos cientos de MB, exploración rápida, un analista solo.

**Cuándo Spark gana:** varias fuentes, joins grandes, el mismo pipeline mañana en un clúster, o cuando el `toPandas()` ya duele.

La puerta de entrada es **`SparkSession`**. Un proceso, una sesión. `getOrCreate()` reutiliza la que ya existe.

| Concepto | Ejemplos | ¿Ejecuta? |
|----------|----------|-----------|
| Transformación | `select`, `filter`, `withColumn`, `join` | No. Amplía el plan. |
| Acción | `show`, `count`, `collect`, `write` | Sí. Lanza jobs. |

En este curso el master es `local[*]`: Spark usa todos los cores del Codespace. El API es el mismo que en un clúster; cambia el sitio donde corren las tareas, no la forma de pensar.

> [!NOTE]
> Spark no “guarda” el DataFrame como un Excel. Guarda un **plan**. Hasta que no lanzas una acción, la cocina está apagada.

## Demostración guiada

> Recorrido que hace el formador en vivo. Tono descriptivo, sin imperativos.

1. Al abrir el Codespace, el entorno ya tiene Java 17 y PySpark. En la raíz del repo, un notebook en blanco (o [notebooks/sandbox.ipynb](../../notebooks/sandbox.ipynb)) es el sitio de trabajo.
2. Se construye la sesión:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("novashop")
    .getOrCreate()
)
spark.version
```

3. Aquí aparece un DataFrame de tres pedidos NovaShop creados en memoria (todavía no se toca `data/raw/`). `printSchema()` lista tipos; `show()` dispara el primer job.
4. En Spark UI (`http://localhost:4040`, puerto reenviado del Codespace) aparece ese job: un DAG mínimo con una sola etapa.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M01-01 | [Sesión Spark y primer DataFrame](M01-01-sesion-spark-primer-dataframe.md) | Crear `notebooks/alumno/M01-01-sesion-spark.ipynb`, sesión Spark, 5 pedidos y `filter` vs `count`. |

→ Empieza por **[M01-01 — Sesión Spark y primer DataFrame](M01-01-sesion-spark-primer-dataframe.md)**.
