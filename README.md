# Python para tratamiento de datos con PySpark

Trabajas sobre **NovaShop** (pedidos, catálogo y eventos). Todo el curso vive en **notebooks**.

## Cómo va cada módulo

1. Abres la **teoría** (`01-teoria.ipynb`): lees y **ejecutas** las celdas aquí mismo (en clase, juntos).
2. Abres el **lab** (`0N-lab-….ipynb`): es el guion. **Crea tu propio notebook** en `notebooks/trabajo/` y ve creando celdas Markdown (qué y por qué) + código. Ejecutas, compruebas, mejoras.

Empieza por el Lab 0: entorno, fork, Codespace y qué es un notebook.

Índice de ficheros: [notebooks/README.md](notebooks/README.md).

## Temario

### M00 — Entorno y notebooks

Fork, Codespace, qué es un notebook (Markdown vs código) y cómo ejecutarlo.

- Teoría: [01-teoria](notebooks/M00-entorno-notebooks/01-teoria.ipynb)
- Lab: [tu primer notebook](notebooks/M00-entorno-notebooks/02-lab-primer-notebook.ipynb)

### M01 — Fundamentos y entorno

Qué es Spark y cuándo usarlo. Diferencia con Pandas. `SparkSession` y modelo de ejecución (transformación vs acción).

- Teoría: [01-teoria](notebooks/M01-fundamentos-entorno/01-teoria.ipynb)
- Lab: [sesión Spark y primer DataFrame](notebooks/M01-fundamentos-entorno/02-lab-sesion-spark.ipynb)

### M02 — Ingesta y preparación de datos

Lectura CSV y JSON. Quién decide nombres y tipos (Spark adivina vs tú escribes el contrato). Calidad y limpieza.

- Teoría: [01-teoria](notebooks/M02-ingesta-preparacion/01-teoria.ipynb)
- Labs: [ingesta CSV/JSON](notebooks/M02-ingesta-preparacion/02-lab-ingesta-csv-json.ipynb) · [schema y tipos](notebooks/M02-ingesta-preparacion/03-lab-schema-tipos.ipynb) · [calidad y limpieza](notebooks/M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb)

### M03 — Transformación de datos

Operaciones sobre DataFrames (`select`, `withColumn`). Filtros y expresiones. Lógica de negocio en columnas, no en un `for`.

- Teoría: [01-teoria](notebooks/M03-transformacion-datos/01-teoria.ipynb)
- Labs: [enriquecimiento](notebooks/M03-transformacion-datos/02-lab-enriquecimiento.ipynb) · [reglas de negocio](notebooks/M03-transformacion-datos/03-lab-reglas-negocio.ipynb)

### M04 — Integración y agregación

Joins entre datasets. Agrupaciones (`groupBy`). Cálculo de métricas y segmentación.

- Teoría: [01-teoria](notebooks/M04-integracion-agregacion/01-teoria.ipynb)
- Labs: [joins](notebooks/M04-integracion-agregacion/02-lab-joins.ipynb) · [KPIs](notebooks/M04-integracion-agregacion/03-lab-kpis.ipynb) · [segmentación](notebooks/M04-integracion-agregacion/04-lab-segmentacion.ipynb)

### M05 — Análisis avanzado

Window functions. Ranking y acumulados por entidad (cliente / pedido).

- Teoría: [01-teoria](notebooks/M05-analisis-avanzado/01-teoria.ipynb)
- Labs: [ranking](notebooks/M05-analisis-avanzado/02-lab-ranking-ventana.ipynb) · [acumulados](notebooks/M05-analisis-avanzado/03-lab-acumulados.ipynb)

### M06 — Optimización y ejecución

Evaluación lazy. Plan de ejecución (DAG). Cache y particionado.

- Teoría: [01-teoria](notebooks/M06-optimizacion-ejecucion/01-teoria.ipynb)
- Labs: [explain y DAG](notebooks/M06-optimizacion-ejecucion/02-lab-explain-dag.ipynb) · [cache y particionado](notebooks/M06-optimizacion-ejecucion/03-lab-cache-particionado.ipynb)

### M07 — Persistencia de datos

Escritura en Parquet. Organización en disco (`partitionBy`). Dataset listo para analítica.

- Teoría: [01-teoria](notebooks/M07-persistencia-datos/01-teoria.ipynb)
- Lab: [Parquet y layout](notebooks/M07-persistencia-datos/02-lab-parquet-layout.ipynb)

## Entorno

Codespace (Java 17 + PySpark) o local: [infra/README.md](infra/README.md). Dataset: [data/README.md](data/README.md).
