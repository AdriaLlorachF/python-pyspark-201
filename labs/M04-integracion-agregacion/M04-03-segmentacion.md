# M04-03 — Segmentación

[← Página anterior](M04-02-kpis.md) · [Siguiente página →](../M05-analisis-avanzado/README.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Clasificar clientes con venta cobrable en `low` / `mid` / `high` según GMV y contar cada banda.

### Prerrequisitos

- Universo `sales` de M04-02 (fact inner clientes, `is_billable`).

### En qué consiste

Carga → agregación a grano cliente → bandas → validación.

### 1 — GMV por cliente

**Acción:**

```python
from pyspark.sql.functions import col, sum as fsum, countDistinct, when, lit

customer_gmv = sales.groupBy("customer_id", "country", "segment").agg(
    fsum("gmv_line").alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
customer_gmv.orderBy(col("gmv").desc()).show(5)
print(customer_gmv.count())
```

**Por qué:** la segmentación es una agregación **después** de fijar el grano.

**Resultado esperado:** ~**211** clientes con al menos un paid (el resto de los 250 no compró cobrable o es huérfano).

### 2 — Bandas de negocio

**Acción:**

```python
banded = customer_gmv.withColumn(
    "value_band",
    when(col("gmv") < 1000, lit("low"))
    .when(col("gmv") < 3000, lit("mid"))
    .otherwise(lit("high")),
)
banded.groupBy("value_band").count().orderBy("value_band").show()
```

**Por qué:** umbrales explícitos se explican a negocio. Los quintiles (`ntile`) se dejan para el reto.

**Resultado esperado (orientativo):** `high` ≈ 40 · `low` ≈ 56 · `mid` ≈ 115.

### 3 — Guardar para M05

**Acción:**

```python
banded.write.mode("overwrite").parquet(str(Path("data/staging/customer_gmv")))
```

**Por qué:** M05 rankea sobre este grano sin recalcular el GMV.

**Resultado esperado:** carpeta `data/staging/customer_gmv`.

## Comprueba tu entendimiento

**Suma de bandas**
`low + mid + high` debe igualar `customer_gmv.count()`.
→ Una sola cifra, sin clientes en dos bandas.

## Reto

### 1 — Quintiles

Usa `ntile(5)` sobre `gmv` (ventana global `orderBy(gmv)`) y cuenta cada quintil.

<details>
<summary>Ver solución</summary>

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

w = Window.orderBy(col("gmv"))
customer_gmv.withColumn("q", ntile(5).over(w)).groupBy("q").count().orderBy("q").show()
```

Sin `partitionBy`: un solo ranking de la compañía. Eso es correcto aquí.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| 250 clientes en las bandas | Agregaste *todos* los clientes con left y GMV nulo | Parte de `sales` cobrable, o filtra `gmv > 0` |
| Un cliente en two bands | Encadenaste `when` mal (umbrales solapados) | `< 1000` luego `< 3000` luego `otherwise` |
