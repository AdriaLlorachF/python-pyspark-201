# M03-02 — Reglas de negocio encadenadas

[← Página anterior](M03-01-enriquecimiento.md) · [Siguiente página →](../M04-integracion-agregacion/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Capar el descuento, normalizar el canal, marcar lo cobrable y persistir `data/staging/fact_lines`.

### Prerrequisitos

- DataFrame `lines` de M03-01 (1980 filas con `gmv_line` y `order_month`).

### En qué consiste

Carga del `lines` enriquecido → tres reglas encadenadas → reescritura de GMV → validación → Parquet.

### 1 — Tres reglas

**Acción:**

```python
from pyspark.sql.functions import col, when, lower, least, lit

fact = (
    lines.withColumn("discount", least(col("discount"), lit(1.0)))
    .withColumn(
        "channel_norm",
        when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel")))
        .otherwise(lit("other")),
    )
    .withColumn("is_billable", col("status") == "paid")
    .withColumn(
        "gmv_line",
        col("qty") * col("unit_price") * (1 - col("discount")),
    )
)
```

**Por qué:**
- `least(..., 1)` evita GMV negativo.
- `marketplace` / `WEB` / `App` no sirven para un `groupBy` limpio.
- GMV de negocio se reporta sobre `paid`; el resto se queda en el fact con flag.

**Resultado esperado:** mismas 1980 filas; `gmv_line < 0` pasa a **0**.

### 2 — Validar dominio

**Acción:**

```python
fact.groupBy("channel_norm").count().orderBy("channel_norm").show()
fact.where(col("discount") > 1).count()
fact.where(col("is_billable")).count()
```

**Por qué:** un set cerrado se comprueba con un `groupBy`, no a ojo.

**Resultado esperado:** canales solo `app`, `other`, `store`, `web`. `discount > 1` → **0**. Líneas cobrables **1127**.

### 3 — Escribir fact_lines

**Acción:**

```python
dest = Path("data/staging/fact_lines")
fact.write.mode("overwrite").parquet(str(dest))
spark.read.parquet(str(dest)).count()
```

**Por qué:** M04 parte de este fact. Si escribes CSV, pierdes tipos y el flag booleano.

**Resultado esperado:** **1980** al releer.

> [!TIP]
> `fact.explain(True)` ya muestra los filtros y el join. No hace falta optimizar: es el puente a M06.

## Comprueba tu entendimiento

**Contrato del fact**
`discount <= 1` en todas las filas; `channel_norm` ⊆ {`web`,`app`,`store`,`other`}; count 1980.
→ Tres checks en verde.

## Reto

### 1 — El plan incluye el join

Lanza `fact.explain("formatted")` y señala la línea del `Exchange` / join `order_id`.

<details>
<summary>Ver solución</summary>

En el plan físico aparece un `BroadcastHashJoin` o `SortMergeJoin` con `order_id`. Si no lo ves, estás explicando `lines` *antes* del join.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| Siguen 13 GMV negativos | Capaste `discount` *después* de calcular GMV y no recalculaste | Recalcula `gmv_line` al final de la cadena |
| `channel_norm` tiene `WEB` | Faltó `lower` | `lower(col("channel"))` antes del `isin` |
| 1127 no sale | Filtraste `is_billable` en vez de solo marcarlo | El fact guarda las 1980; el flag decide en M04 |
