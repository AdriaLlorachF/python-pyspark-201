# M03 — Transformación de datos

[← Página anterior](../M02-ingesta-preparacion/M02-03-calidad-limpieza.md) · [Siguiente página →](M03-01-enriquecimiento.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en los laboratorios.

## Qué aprenderás

- Usar `select`, `withColumn` y `drop` sin bajar los datos a Python.
- Expresar reglas con `col`, `when` / `otherwise`.
- Encadenar transformaciones y dejar un fact de líneas de negocio.

## Teoría

Una transformación **añade o cambia columnas** o **filtra filas**. El DataFrame de entrada no se muta: obtienes otro plan.

| API | Uso típico |
|-----|------------|
| `select` | Proyectar / reordenar / alias |
| `withColumn` | Alta o reemplazo de una columna |
| `drop` | Quitar columnas técnicas |
| `filter` / `where` | Predicado de filas |
| `when` / `otherwise` | `if/else` vectorizado |

La lógica de negocio de NovaShop vive en columnas:

| Columna | Regla |
|---------|--------|
| `gmv_line` | `qty * unit_price * (1 - discount)` |
| `order_month` | `date_format(order_ts, "yyyy-MM")` |
| `channel_norm` | `web`/`app`/`store`/`other` (minúsculas; `marketplace` → `other`) |
| `is_billable` | `status == "paid"` |

> [!WARNING]
> No uses `collect()` ni `toPandas()` para “hacer un apply”. Eso trae **todas** las filas al driver. Si necesitas mirar, `limit(20).toPandas()`.

Encadenar vs variables: ambas son válidas. En clase, una variable por *intención* (`lines_gmv`, `lines_billable`) se lee mejor que un único tubo de 15 puntos.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. Se lee `data/staging/order_items_clean` (2010 líneas) ya tipado.
2. Con `withColumn` aparece `gmv_line`. Un `where(gmv_line > 0)` deja fuera descuentos absurdos o precios nulos.
3. `select("order_id", "qty", "unit_price", "discount", "gmv_line")` + `show(5)` enseña que el GMV es una columna más, no un bucle.

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M03-01 | [Enriquecimiento](M03-01-enriquecimiento.md) | Calendario y GMV de línea. |
| M03-02 | [Reglas de negocio encadenadas](M03-02-reglas-negocio.md) | Canal, descuento capado, cobrable → `fact_lines`. |

→ Empieza por **[M03-01 — Enriquecimiento](M03-01-enriquecimiento.md)**.
