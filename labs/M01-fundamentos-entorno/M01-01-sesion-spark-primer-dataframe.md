# M01-01 — Sesión Spark y primer DataFrame

[← Página anterior](README.md) · [Siguiente página →](../M02-ingesta-preparacion/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).


## Tu notebook

El alumno **crea su propio notebook** aquí. En clase usamos `notebooks/clase/`; no copies `notebooks/_qa/`.

| | Valor fijo |
|--|--|
| Carpeta | [`notebooks/alumno/`](../../notebooks/README.md) |
| Nombre | `M01-01-sesion-spark.ipynb` |
| Cómo crearlo | Explorador → carpeta `notebooks/alumno` → clic derecho → **New File…** → pega el nombre de arriba (con `.ipynb`) → Enter |
| Kernel | **Python (NovaShop)** · paleta `Notebook: Select Notebook Kernel` si no aparece |
| Organización y Celda 0 | [notebooks/README.md](../../notebooks/README.md) |

**Celda 0** (primera celda, idéntica en todos los labs). Ejecútala antes de cualquier otra:

```python
import sys
from pathlib import Path

_here = Path.cwd().resolve()
ROOT = next(
    p
    for p in [_here, *_here.parents]
    if (p / "labs" / "_shared" / "session.py").is_file()
)
sys.path.insert(0, str(ROOT / "labs" / "_shared"))

from paths import RAW, STAGING, CURATED
from session import get_spark

print("ROOT   ", ROOT)
print("RAW    ", RAW, "existe:", RAW.is_dir())
```

Después, **una celda nueva por cada paso** (`### 1`, `### 2`…). Usa `RAW`, `STAGING` y `CURATED` (no `Path("data/raw")`).


### Objetivo

Dejar una `SparkSession` viva y materializar un DataFrame de 5 pedidos NovaShop, distinguiendo un `filter` de un `count`.

### Prerrequisitos

- Codespace arrancado (o Python 3.11 + Java 17; ver [infra/README.md](../../infra/README.md)).
- Notebook `notebooks/alumno/M01-01-sesion-spark.ipynb` creado y **Celda 0** ejecutada.

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
