# M06 — Optimización y ejecución

[← Página anterior](../M05-analisis-avanzado/M05-02-acumulados.md) · [Siguiente página →](M06-01-explain-dag.md)

> [!NOTE]
> **Cómo funciona este módulo.** Primero la **teoría**, luego la **demostración guiada** del
> formador, y después **practicas tú** en los laboratorios.

## Qué aprenderás

- Ver la evaluación **lazy** en Spark UI: sin acción, no hay job.
- Leer un plan (`explain`) a alto nivel (scan → filter → aggregate).
- Decidir cuándo `cache` ayuda y cuándo estorba.
- Distinguir `repartition` físico de `partitionBy` al escribir (M07).

## Teoría

Spark no ejecuta al escribir `filter` o `join`. Construye un **DAG**. Una acción (`count`, `show`, `write`) lo materializa en **jobs → stages → tasks**.

| Idea | Qué observar |
|------|----------------|
| Lazy | Encadenas 5 transformaciones: Spark UI **no** suma jobs |
| Plan lógico | Qué *quieres* hacer |
| Plan físico | Cómo lo va a hacer (join broadcast vs sort-merge, scans) |
| `cache` / `persist` | Guarda el resultado **después** de la primera acción |
| `repartition(n)` / `repartition("col")` | Shuffle: cambia el número o la clave de partición **en memoria** |

`cache` útil: el mismo DataFrame se usa en **varios** KPI. `cache` inútil: una sola pasada, o cachear el CSV crudo que solo lees una vez.

> [!WARNING]
> `df.cache()` **no** materializa. Hasta que no lanzas una acción, Storage en Spark UI sigue vacío.

En `local[*]` los tiempos son pequeños. Importa el **método** (plan + UI), no el récord de milisegundos.

## Demostración guiada

> Recorrido que hace el formador en vivo.

1. Se encadenan `read` + tres `filter` + un `select` **sin** acción. En `http://localhost:4040` no aparece job nuevo.
2. Un `count()` dispara el DAG. `explain("formatted")` muestra el scan Parquet de `fact_lines` y los filtros.
3. Se repite el `count` dos veces sin cache (dos jobs parecidos) y con `cache()` + `count` de calentamiento (el segundo job lee *InMemoryTableScan*).

## Ahora practica tú

| Lab | Título | Qué harás |
|-----|--------|-----------|
| M06-01 | [Explain y DAG](M06-01-explain-dag.md) | Lazy + lectura del plan. |
| M06-02 | [Cache y particionado](M06-02-cache-particionado.md) | Comparar reuso y `repartition` por mes. |

→ Empieza por **[M06-01 — Explain y DAG](M06-01-explain-dag.md)**.
