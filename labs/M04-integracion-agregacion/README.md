# M04 — Integración y agregación

[← Página anterior](../M03-transformacion-datos/M03-02-reglas-negocio.md) · [Siguiente página →](M04-01-joins.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en los laboratorios.

## Qué aprenderás

- Elegir `inner` vs `left` y ver el efecto en los counts.
- Evitar GMV inflado por una cardinalidad 1:N mal leída.
- Calcular KPIs con `groupBy` + `agg`.
- Segmentar clientes por GMV.

## Teoría

Un join **no es un cruce mágico**: declara qué filas sobreviven.

| Tipo | Qué queda | En NovaShop |
|------|-----------|-------------|
| `inner` | Solo claves en **ambos** lados | Pedidos cuyo `customer_id` existe en `customers` |
| `left` | Todas las filas de la izquierda | Pedidos huérfanos `CX*` siguen, con columnas de cliente a nulo |
| `full` / `anti` / `semi` | Menos habituales aquí | `anti` es útil para listar huérfanos |

Cardinalidad: un pedido tiene **N líneas**. Si agregas GMV **después** de unir líneas × clientes estás bien (1:N pedido–línea, N:1 línea–cliente vía pedido). Si unes líneas × productos y el catálogo está duplicado, el GMV se **duplica**.

Granularidad **antes** de agregar:

| Grano | Clave | Métrica típica |
|-------|-------|----------------|
| Línea | `order_id` + producto | `gmv_line` |
| Pedido | `order_id` | GMV del ticket |
| Cliente | `customer_id` | GMV, nº pedidos |
| País / canal / mes | dimensión | KPI de cuadro de mando |

Ticket medio = `sum(GMV) / countDistinct(order_id)` sobre pedidos cobrables. **No** es `avg` del GMV de línea.

> [!WARNING]
> Un KPI mentiroso casi siempre es un join mal elegido, no un `sum` mal escrito. Mira el `count` **antes** y **después** del join.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. Se lee `fact_lines` (1980) y `customers_clean` (250).
2. `inner` vs `left` sobre `customer_id`: el inner baja a **1956** líneas; el left se queda en 1980. La diferencia son **24** líneas de 8 pedidos `CX*`.
3. Un `groupBy("country").agg(sum("gmv_line"))` solo sobre `is_billable` produce el primer KPI usable.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M04-01 | [Joins](M04-01-joins.md) | Inner/left y huérfanos. |
| M04-02 | [KPIs](M04-02-kpis.md) | GMV, pedidos, ticket medio, cancelación. |
| M04-03 | [Segmentación](M04-03-segmentacion.md) | Bandas de cliente por GMV cobrable. |

→ Empieza por **[M04-01 — Joins](M04-01-joins.md)**.
