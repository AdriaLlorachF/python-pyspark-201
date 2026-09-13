# M01-01 — Sesión Spark y primer DataFrame

[← Página anterior](README.md) · [Siguiente página →](../M02-ingesta-preparacion/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Dejar una `SparkSession` viva y materializar un DataFrame de 5 pedidos NovaShop, distinguiendo un `filter` de un `count`.

### Prerrequisitos

- Codespace arrancado (o Python 3.11 + Java 17 en local; ver [infra/README.md](../../infra/README.md)).
- Trabajas en [notebooks/sandbox.ipynb](../../notebooks/sandbox.ipynb) o en un notebook nuevo. Ejecuta las celdas **desde la raíz del repo**.

### En qué consiste

Carga (sesión + filas en memoria) → transformación (`filter` de pedidos `paid`) → validación (`show` / `count`). Aún **no** lees `data/raw/`.

### 1 — Comprobar el runtime

**Acción:** en una celda, ejecuta:

```python
import pyspark, shutil, subprocess

print("pyspark", pyspark.__version__)
print(subprocess.check_output(["java", "-version"], text=True, stderr=subprocess.STDOUT).splitlines()[0])
print("java:", shutil.which("java"))
```

**Por qué:** sin JRE no arranca el JVM de Spark. Si esto falla, regenera el Codespace; no improvises otro Java.

**Resultado esperado:** PySpark `3.5.x` y una línea `openjdk version "17…"` (o equivalente Microsoft JDK 17).

### 2 — Crear (o reusar) la sesión

**Acción:**

```python
import sys
from pathlib import Path

sys.path.append(str(Path("labs/_shared").resolve()))
from session import get_spark

spark = get_spark("novashop-m01")
spark
```

**Por qué:** `getOrCreate()` evita un segundo contexto que pelea por el puerto 4040.

**Resultado esperado:** un objeto `SparkSession` con master `local[*]`. Si vuelves a ejecutar la celda, es **la misma** sesión.

### 3 — Pedidos sintéticos en memoria

**Acción:**

```python
from pyspark.sql import Row

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
```

**Por qué:** `createDataFrame` es la forma más pequeña de ver schema + acción sin ficheros.

**Resultado esperado:** schema con `order_id`/`customer_id`/`status` string y `amount` double; tabla de 5 filas.

```text
+--------+-----------+---------+------+
|order_id|customer_id|   status|amount|
+--------+-----------+---------+------+
|  O90001|      C0001|     paid|  49.9|
|  O90002|      C0002|     paid|  12.5|
|  O90003|      C0003|cancelled|  80.0|
|  O90004|      C0001|     paid|  23.1|
|  O90005|      C0004|  pending|   5.0|
+--------+-----------+---------+------+
```

### 4 — Transformación frente a acción

**Acción:**

```python
paid = df.filter(df.status == "paid")
print("después del filter, Spark aún no ha contado nada")
print("paid count =", paid.count())
```

**Por qué:** `filter` solo alarga el plan. `count` obliga a ejecutarlo.

**Resultado esperado:** `paid count = 3`.

> [!TIP]
> Encadena `paid.explain("formatted")` si quieres ver el plan **antes** de M06. No hace falta entender cada línea todavía.

## Comprueba tu entendimiento

**Versión y cobros**
Imprime `spark.version` y el `count` de `status == "paid"` sobre el DataFrame de 5 filas.
→ PySpark 3.5.x y **3**.

## Reto

### 1 — Canal en dos columnas

Añade `channel` con `withColumn` (todos `"web"`) y muestra solo `order_id` y `channel`.

<details>
<summary>Ver solución</summary>

```python
from pyspark.sql.functions import lit

df.withColumn("channel", lit("web")).select("order_id", "channel").show()
```

Cinco filas; no aparece `amount`.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| `Java gateway process exited` | No hay JDK 17 en el PATH | Codespace limpio; `java -version` debe ser 17 |
| Puerto 4040 ocupado / dos UIs | Segunda `SparkSession()` sin `getOrCreate` | `spark.stop()` y vuelve a `get_spark()` |
| Creo que `filter` ya filtró y “no veo nada” | `filter` no es acción | Encadena `.show()` o `.count()` |
