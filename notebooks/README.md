# Notebooks — cómo va la clase

Tres sitios, tres usos. **No se mezclan.**

```text
notebooks/
├── clase/           ← AULA: teoría + demo en vivo (el formador ejecuta; tú también)
│   └── M0x-…/01-teoria.ipynb
├── alumno/          ← TÚ creas aquí un .ipynb por laboratorio
└── _qa/             ← NO se usa en clase (batería técnica del repo)
```

## Dinámica (como en un curso de notebooks)

1. **Teoría de clase.** Abrimos juntos `notebooks/clase/M0x-…/01-teoria.ipynb`. El formador proyecta y ejecuta celda a celda. Tú ejecutas las mismas celdas en **ese mismo fichero** (no lo copies).
2. **Laboratorio.** Cierras el de clase. Creas **tu** notebook vacío en `notebooks/alumno/` con el nombre de la tabla. El markdown de `labs/Mxx-NN-….md` es el guion (pasos, counts, retos).
3. **`_qa/` no se abre en clase.** Son notebooks ya resueltos para comprobar que el pipeline del repo no se rompe (`python3 scripts/execute_notebooks.py`). No son el material del alumno ni la pizarra del formador.

## Teoría de clase (abrir estos)

| Módulo | Notebook |
|--------|----------|
| M01 | [clase/M01-fundamentos-entorno/01-teoria.ipynb](clase/M01-fundamentos-entorno/01-teoria.ipynb) |
| M02 | [clase/M02-ingesta-preparacion/01-teoria.ipynb](clase/M02-ingesta-preparacion/01-teoria.ipynb) |
| M03 | [clase/M03-transformacion-datos/01-teoria.ipynb](clase/M03-transformacion-datos/01-teoria.ipynb) |
| M04 | [clase/M04-integracion-agregacion/01-teoria.ipynb](clase/M04-integracion-agregacion/01-teoria.ipynb) |
| M05 | [clase/M05-analisis-avanzado/01-teoria.ipynb](clase/M05-analisis-avanzado/01-teoria.ipynb) |
| M06 | [clase/M06-optimizacion-ejecucion/01-teoria.ipynb](clase/M06-optimizacion-ejecucion/01-teoria.ipynb) |
| M07 | [clase/M07-persistencia-datos/01-teoria.ipynb](clase/M07-persistencia-datos/01-teoria.ipynb) |

Kernel: **Python (NovaShop)**. Primera celda = arranque (localiza el repo).

## Labs: qué crea el alumno

| Laboratorio | Nombre exacto en `notebooks/alumno/` |
|-------------|--------------------------------------|
| M01-01 | `M01-01-sesion-spark.ipynb` |
| M02-01 | `M02-01-ingesta-csv-json.ipynb` |
| M02-02 | `M02-02-schema-tipos.ipynb` |
| M02-03 | `M02-03-calidad-limpieza.ipynb` |
| M03-01 | `M03-01-enriquecimiento.ipynb` |
| M03-02 | `M03-02-reglas-negocio.ipynb` |
| M04-01 | `M04-01-joins.ipynb` |
| M04-02 | `M04-02-kpis.ipynb` |
| M04-03 | `M04-03-segmentacion.ipynb` |
| M05-01 | `M05-01-ranking-ventana.ipynb` |
| M05-02 | `M05-02-acumulados.ipynb` |
| M06-01 | `M06-01-explain-dag.ipynb` |
| M06-02 | `M06-02-cache-particionado.ipynb` |
| M07-01 | `M07-01-parquet-layout.ipynb` |

Cómo crearlo: explorador → `notebooks/alumno` → clic derecho → **New File…** → el nombre de arriba → kernel **Python (NovaShop)** → Celda 0 del lab markdown.

## Celda 0 (labs del alumno)

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
print("STAGING", STAGING)
print("CURATED", CURATED)
```

Usa `RAW` / `STAGING` / `CURATED`. No escribas `Path("data/raw")`.
