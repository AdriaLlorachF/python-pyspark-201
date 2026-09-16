#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Python: $(python3 -c 'import sys; print(sys.executable, sys.version.split()[0])')"
python3 -m pip install --user --upgrade pip
python3 -m pip install --user -r "$ROOT/requirements.txt"
python3 -m ipykernel install --user --name novashop --display-name "Python (NovaShop)"
python3 "$ROOT/scripts/generate_novashop.py"
python3 "$ROOT/scripts/seed_mongo.py" || echo "==> seed Mongo omitido (lab extra)"
python3 "$ROOT/scripts/fetch_mongo_jars.py" || echo "==> jars Mongo omitidos (lab extra)"

echo "==> Kernel NovaShop registrado"
python3 -m jupyter kernelspec list || true
echo "==> Java:"
java -version 2>&1 | head -3
python3 -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').appName('setup').getOrCreate(); print('spark', s.version); s.stop()"
echo "==> Connector Mongo (jars locales, sin Ivy):"
PYTHONPATH="$ROOT/labs/_shared${PYTHONPATH:+:$PYTHONPATH}" python3 -c "from session import prefetch_mongo_connector; prefetch_mongo_connector(); print('mongo connector ok')" \
  || echo "==> connector Mongo omitido; el lab extra pide python3 scripts/fetch_mongo_jars.py"
echo "OK setup"
