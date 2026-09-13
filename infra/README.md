# Entorno del laboratorio

PySpark corre **en el Codespace**, modo `local[*]`. No hay Docker Compose ni clúster.

## Codespace (recomendado)

1. Abre el repo en GitHub → **Code → Codespaces → Create codespace on main**.
2. El contenedor instala Java 17, PySpark 3.5 y regenera `data/raw/` si faltara.
3. Spark UI queda en el puerto **4040** (se reenvía solo).

## Local (alternativa)

- Python 3.11+ y un JRE/JDK 17 (`java -version`).
- `pip install -r requirements.txt`
- `python3 scripts/generate_novashop.py`

## Comprobar

```bash
python3 -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').appName('ping').getOrCreate(); print(s.version); s.stop()"
```
