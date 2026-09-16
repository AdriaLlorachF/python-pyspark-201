# Entorno

PySpark corre **en el Codespace**, modo `local[*]`. El paso a paso (fork, Codespace, kernel, ejecutar un notebook) está en [M00 — teoría](../notebooks/M00-entorno-notebooks/01-teoria.ipynb).

## Codespace

1. **Fork** del repo → en *tu* fork, **Code → Codespaces → Create codespace on main**.
2. Espera a que termine `postCreate` (Java 17, PySpark 3.5, kernel **Python (NovaShop)**, `data/raw/`).
3. Spark UI: puerto **4040** (pestaña Ports).
4. **Crea tus** notebooks en `notebooks/trabajo/`.

Si no aparece **Python (NovaShop)**:

```bash
bash .devcontainer/setup.sh
```

Luego `F1` → `Notebook: Select Notebook Kernel` → **Python (NovaShop)**.

## Local

Python 3.11+ y **JDK 17** (`java -version` debe ser 17). Luego `bash .devcontainer/setup.sh`.

## Comprobar

```bash
python3 scripts/run_pipeline.py
```
