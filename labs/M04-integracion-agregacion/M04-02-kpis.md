# M04-02 — KPIs

[← Página anterior](M04-01-joins.md) · [Siguiente página →](M04-03-segmentacion.md)

> Práctica del módulo. La teoría y la demo están en el [README del módulo](README.md).

### Objetivo

Calcular GMV cobrable, nº de pedidos cobrables, ticket medio y tasa de cancelación sobre el universo **con cliente real**.

### Prerrequisitos

- Entiendes inner vs left (M04-01).
- Usamos `inner` a clientes para no atribuir GMV a `CX*`.

### En qué consiste

Carga → universo `sales` (fact inner clientes, solo `is_billable` para dinero) → agregaciones → validación.

### 1 — Universo de venta

**Acción:**

```python
from pyspark.sql.functions import col

sales = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
)
print(sales.count())
```

**Por qué:** KPI de dinero ≠ KPI de operativa. Lo cancelado se mira aparte.

**Resultado esperado:** **1122** líneas cobrables con cliente (1127 − 5 líneas paid huérfanas).

### 2 — Cuatro métricas globales

**Acción:**

```python
from pyspark.sql.functions import sum as fsum, countDistinct, round as fround

kpis = sales.agg(
    fround(fsum("gmv_line"), 2).alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
kpis = kpis.withColumn("aov", fround(col("gmv") / col("orders"), 2))
kpis.show()
```

**Por qué:** el ticket medio se calcula a grano pedido, no como media de líneas.

**Resultado esperado:** GMV ≈ **400 157.73** · pedidos cobrables **469** · AOV ≈ **853**.

> [!NOTE]
> Si casteaste a `double`, el céntimo puede moverse. Redondea a 2 decimales y compara el orden de magnitud, no el bit.

### 3 — Tasa de cancelación

**Acción:** sobre `orders_clean` **inner** clientes (788 − 8 = 780 pedidos):

```python
from pyspark.sql.functions import avg

orders = spark.read.parquet(str(STG / "orders_clean"))
ord_ok = orders.join(customers, "customer_id", "inner")
cancel = ord_ok.agg(
    avg((col("status") == "cancelled").cast("double")).alias("cancel_rate")
)
cancel.show()
```

**Por qué:** el denominador es pedidos (no líneas). Si usas el fact, inflas los pedidos con más líneas.

**Resultado esperado:** ≈ **0.22** (171 cancelados / 788 en el staging completo; sobre 780 reales el ratio es el mismo orden).

### 4 — KPI por canal

**Acción:**

```python
(
    sales.groupBy("channel_norm")
    .agg(
        fround(fsum("gmv_line"), 2).alias("gmv"),
        countDistinct("order_id").alias("orders"),
    )
    .orderBy(col("gmv").desc())
    .show()
)
```

**Por qué:** el `groupBy` de negocio es el cuadro de mando. `channel_norm` (no `channel`) evita partir `web`/`WEB`.

**Resultado esperado:** cuatro filas (`app`, `other`, `store`, `web`), `web` o `app` en cabeza.

## Comprueba tu entendimiento

**Ticket medio**
Reproduce `gmv / countDistinct(order_id)` solo con `is_billable` e inner a clientes.
→ Un número ~850, no ~350 (eso sería media de línea).

## Reto

### 1 — GMV por mes y país

`groupBy("order_month", "country")` con la misma regla cobrable.

<details>
<summary>Ver solución</summary>

```python
(
    sales.groupBy("order_month", "country")
    .agg(fround(fsum("gmv_line"), 2).alias("gmv"))
    .orderBy("order_month", "country")
    .show(20)
)
```

`UNK` aparece si no rellenaste país en M02-03.

</details>

## Errores frecuentes

| Síntoma | Causa probable | Cómo arreglarlo |
|---------|----------------|-----------------|
| GMV ~ 2× | Join al catálogo duplicado | `dropDuplicates(["product_id"])` en products |
| AOV ridículamente bajo | `avg("gmv_line")` | `sum / countDistinct(order_id)` |
| Cancel rate 0 | Mediste sobre `sales` (solo paid) | Usa `orders_clean`, no el fact cobrable |
