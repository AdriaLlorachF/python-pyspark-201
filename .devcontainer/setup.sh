#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Python: $(python3 -c 'import sys; print(sys.executable, sys.version.split()[0])')"
python3 -m pip install --user --upgrade pip
python3 -m pip install --user -r "$ROOT/requirements.txt"
python3 -m ipykernel install --user --name novashop --display-name "Python (NovaShop)"
python3 "$ROOT/scripts/generate_novashop.py"

echo "==> Kernel NovaShop registrado"
python3 -m jupyter kernelspec list || true
echo "==> Java:"
java -version 2>&1 | head -3
echo "OK setup"
