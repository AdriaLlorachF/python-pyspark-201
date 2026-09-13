# M07 — Persistencia de datos

[← Página anterior](../M06-optimizacion-ejecucion/M06-02-cache-particionado.md) · [Siguiente página →](M07-01-parquet-layout.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en el laboratorio.

## Qué aprenderás

- Escribir Parquet con schema estable y `mode("overwrite")`.
- Particionar por una columna de **negocio** (`order_month`).
- Contrastar CSV vs Parquet (tamaño y relectura).
- Dejar un layout `data/curated/sales_analytics` listo para analítica.

## Teoría

El pipeline no acaba en un `show`. Acaba en un directorio que **otro** proceso puede leer mañana.

| Formato | Ventaja | Coste |
|---------|---------|--------|
| CSV | Legible | Sin tipos, pesado, no hay prune |
| Parquet | Columnar, schema, compresión, predicados | Binario (se mira con Spark, no con `cat`) |

`partitionBy("order_month")` crea carpetas `order_month=2024-03/`. Una lectura con `where(order_month = "2024-03")` **no abre** los demás meses (*partition pruning*).

| Práctica | Sí | No |
|----------|----|----|
| Un job de escritura al final | `overwrite` del dataset curated | `append` a ciegas cada vez que pruebas |
| Partición útil | Mes, país (cardinalidad baja) | `order_id` (miles de carpetas vacías) |
| Entrega puntual | `coalesce(1)` para un único fichero a un analista | `coalesce(1)` en el pipeline diario |

> [!WARNING]
> `repartition` (M06) baraja **en memoria**. `partitionBy` en el `write` organiza **en disco**. Se complementan; no son lo mismo.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. El fact cobrable inner clientes se escribe en `data/curated/sales_analytics` con `partitionBy("order_month")`.
2. En el explorador aparecen doce carpetas `order_month=2024-xx`.
3. Se reabre el dataset filtrando un mes: `explain` muestra que el scan lista **una** partición.
4. La misma tabla en CSV ocupa más y al leer hay que volver a castear.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M07-01 | [Parquet y layout analítico](M07-01-parquet-layout.md) | Exportar el dataset curated y comprobar prune. |

→ Empieza por **[M07-01 — Parquet y layout analítico](M07-01-parquet-layout.md)**.
