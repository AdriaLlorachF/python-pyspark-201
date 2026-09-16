# Entorno

PySpark corre **en el Codespace**, modo `local[*]`. El paso a paso (fork, Codespace, kernel, ejecutar un notebook) está en [M00 — teoría](../notebooks/M00-entorno-notebooks/01-teoria.ipynb).

## Codespace

1. **Fork** del repo → en *tu* fork, **Code → Codespaces → Create codespace on main**.
2. Espera a que termine `postCreate` (Java 17, PySpark 3.5, kernel **Python (NovaShop)**, `data/raw/`, y si el compose está bien, seed de Mongo).
3. Spark UI: puerto **4040** (pestaña Ports). Mongo: **27017** (solo el lab extra).
4. **Crea tus** notebooks en `notebooks/trabajo/`.

El Codespace es **dos contenedores**: `app` (Python, Java, Jupyter) y `mongo`. No hay que instalar Mongo a mano ni usar Docker-in-Docker.

Si no aparece **Python (NovaShop)**:

```bash
bash .devcontainer/setup.sh
```

Luego `F1` → `Notebook: Select Notebook Kernel` → **Python (NovaShop)**.

### Ya tenías un Codespace (lab extra Mongo)

Un `git pull` **no** levanta Mongo. Hay que reconstruir el contenedor:

1. `F1` → **Codespaces: Rebuild Container** (o **Dev Containers: Rebuild Container**).
2. Espera al `postCreate`.
3. Comprueba: pestaña **Ports** → **27017**, o en una terminal `python3 scripts/seed_mongo.py` (debe imprimir `reviews = 150`).

Si el rebuild da guerra: borra el Codespace y crea uno nuevo sobre `main`.

## Local

Python 3.11+ y **JDK 17** (`java -version` debe ser 17). Luego `bash .devcontainer/setup.sh`.

El lab extra de Mongo en local pide Docker y, desde la raíz del repo:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d mongo
export MONGO_URI=mongodb://127.0.0.1:27017
python3 scripts/generate_novashop.py
python3 scripts/seed_mongo.py
```

Sin Docker, el curso de ficheros sigue; ese lab no.

## Comprobar

```bash
python3 scripts/run_pipeline.py
```
