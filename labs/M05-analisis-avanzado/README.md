# M05 — Análisis avanzado

[← Página anterior](../M04-integracion-agregacion/M04-03-segmentacion.md) · [Siguiente página →](M05-01-ranking-ventana.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en los laboratorios.

## Qué aprenderás

- Distinguir `groupBy` (aplasta) de una **window** (calcula y conserva filas).
- Particionar lógicamente por entidad (`customer_id`) y ordenar por fecha o GMV.
- Sacar rankings (`row_number`, `rank`) y acumulados (`sum` en ventana).

## Teoría

`groupBy("customer_id")` deja **una fila por cliente**. Una window deja **todas las filas** y añade columnas de contexto: “este es tu 3.er pedido”, “llevas 1 200 € acumulados”.

```text
Window.partitionBy("customer_id").orderBy(col("order_ts"))
```

| Pieza | Qué hace | No confundir con |
|-------|----------|------------------|
| `partitionBy` | Recalcula el ranking **dentro** de cada cliente | `repartition` (físico, M06) |
| `orderBy` de la window | Orden del ranking / del acumulado | `orderBy` del DataFrame (solo presentación) |
| `row_number` | 1, 2, 3 sin empates | `rank` (1, 2, 2, 4) / `dense_rank` (1, 2, 2, 3) |
| `sum("gmv").over(w)` | Acumulado según el orden | `sum` de `groupBy` |

> [!NOTE]
> Partición **lógica** (window) ≠ partición **física** (`df.rdd.getNumPartitions()`, `repartition`). La primera es una pregunta de negocio; la segunda, de ejecución.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. Sobre `customer_gmv` se abre una window global `orderBy(gmv desc)` y `row_number` marca el top 5 de NovaShop.
2. Sobre el fact cobrable, `partitionBy("customer_id").orderBy("order_ts")` + `row_number` numera los pedidos de cada cliente.
3. El mismo marco con `sum("gmv_line").over(w)` construye la curva acumulada. Las filas **no desaparecen**.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M05-01 | [Ranking por ventana](M05-01-ranking-ventana.md) | Top clientes y top productos por cliente. |
| M05-02 | [Acumulados por entidad](M05-02-acumulados.md) | GMV acumulado y número de pedido. |

→ Empieza por **[M05-01 — Ranking por ventana](M05-01-ranking-ventana.md)**.
