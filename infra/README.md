# Entorno del laboratorio

PySpark corre **en el Codespace**, modo `local[*]`. No hay Docker Compose ni clúster.

## Codespace (recomendado)

1. Abre el repo en GitHub → **Code → Codespaces → Create codespace on main** (o Rebuild si ya existía).
2. Espera a que termine `postCreate` (Java 17, PySpark 3.5, kernel **Python (NovaShop)**, `data/raw/`).
3. Spark UI: puerto **4040** (pestaña Ports).
4. Crea tus notebooks en `notebooks/alumno/` — nombres y Celda 0 en [notebooks/README.md](../notebooks/README.md).

Si el kernel pide `ipykernel` o no aparece **Python (NovaShop)**:

```bash
bash .devcontainer/setup.sh
```

Luego paleta (`F1`) → `Notebook: Select Notebook Kernel` → **Python (NovaShop)**.

## Local (alternativa)

- Python 3.11+ y **JDK 17** (`java -version` debe ser 17; Spark 3.5 no arranca bien en Java 25).
- `bash .devcontainer/setup.sh`

## Comprobar

```bash
python3 scripts/run_pipeline.py
```
