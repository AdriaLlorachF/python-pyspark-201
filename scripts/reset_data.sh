#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
rm -rf "$root/data/staging" "$root/data/curated"
python3 "$root/scripts/generate_novashop.py"
echo "OK: raw regenerado; staging y curated vaciados."
