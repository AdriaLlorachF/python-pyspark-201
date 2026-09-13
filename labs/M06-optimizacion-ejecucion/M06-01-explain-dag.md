# M06-01 — Explain y DAG

[← Página anterior](README.md) · [Siguiente página →](M06-02-cache-particionado.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Demostrar que cinco transformaciones no lanzan job, y señalar scan + filtro en el plan formateado.

### Prerrequisitos

- `data/staging/fact_lines`.
- Spark UI en el puerto **4040** del Codespace (pestaña Ports).

### En qué consiste

Carga (solo construir el plan) → acción → validación en UI y `explain`.

### 1 — Plan sin ejecutar

**Acción:**

```python
import sys
from pathlib import Path
from pyspark.sql.functions import col

sys.path.append(str(Path("labs/_shared").resolve()))
from session import get_spark

spark = get_spark("novashop-m06")
# Anota el último Job Id que ves ahora en Spark UI (puede ser 0 o el de labs anteriores).

planned = (
    spark.read.parquet("data/staging/fact_lines")
    .where(col("is_billable"))
    .where(col("gmv_line") > 0)
    .where(col("channel_norm").isin("web", "app"))
    .select("order_id", "customer_id", "gmv_line", "order_month")
)
print(planned)  # no es una acción
```

**Por qué:** imprimir el objeto DataFrame no dispara jobs.

**Resultado esperado:** el Job Id **más alto** de Spark UI no cambia al ejecutar esta celda.

### 2 — Una acción, un DAG

**Acción:**

```python
print(planned.count())
```

**Por qué:** `count` obliga a recorrer las particiones.

**Resultado esperado:** un job nuevo. En la pestaña Jobs, el DAG muestra al menos un stage. El count es el de líneas cobrables web/app con GMV > 0 (varios cientos).

### 3 — Leer el plan

**Acción:**

```python
planned.explain("formatted")
```

**Por qué:** el plan es el mapa. Buscas *FileScan parquet* (o `Scan`) y *Filter*.

**Resultado esperado:** aparece el path `fact_lines` y predicados `is_billable` / `gmv_line` / `channel_norm`. No hace falta traducir cada operador Catalyst.

## Comprueba tu entendimiento

**Lazy de verdad**
Vuelve a encadenar un `.where(...)` extra **sin** `count` y mira Jobs.
→ No hay job nuevo.

## Reto

### 1 — `explain(True)` vs `formatted`

Compara `explain(True)` (plan lógico + físico) con `explain("formatted")`. ¿Dónde se ve el filtro empujado al scan?

<details>
<summary>Ver solución</summary>

En el físico / formatted, el `PushedFilters` o el `Filter` junto al `FileScan` indica *predicate pushdown*. Si el filtro no aparece, lo aplicaste *después* de un `select` que ya tiró la columna.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| UI vacía / 404 | Puerto 4040 no reenviado | Pestaña Ports del Codespace → 4040 |
| Cada celda crea un job | Tienes un `.show()` de debug | Comenta los `show` mientras mides |
| Dos sesiones | `SparkSession()` extra | Solo `get_spark()` |
