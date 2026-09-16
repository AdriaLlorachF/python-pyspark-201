#!/usr/bin/env python3
"""Siembra novashop.reviews. Si Mongo no está, avisa y sale 0 (el curso de ficheros sigue)."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
REVIEWS = RAW / "reviews.jsonl"
URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DB = "novashop"
COLLECTION = "reviews"


def _client():
    from pymongo import MongoClient

    return MongoClient(URI, serverSelectionTimeoutMS=2000)


def wait_ping(attempts: int = 8) -> bool:
    last = None
    for _ in range(attempts):
        try:
            _client().admin.command("ping")
            return True
        except Exception as exc:  # noqa: BLE001 — cualquier fallo de red/dns aquí es "aún no"
            last = exc
            time.sleep(2)
    print(f"==> Mongo no responde en {URI} ({last}). Lab extra omitido.")
    return False


def seed() -> int:
    if not REVIEWS.is_file():
        print(f"==> Falta {REVIEWS}. Ejecuta scripts/generate_novashop.py")
        return 1
    docs = [json.loads(line) for line in REVIEWS.read_text(encoding="utf-8").splitlines() if line.strip()]
    col = _client()[DB][COLLECTION]
    col.drop()
    if docs:
        col.insert_many(docs)
    n = col.count_documents({})
    print(f"==> Mongo {URI}  {DB}.{COLLECTION} = {n}")
    return 0


def main() -> int:
    if not wait_ping():
        return 0
    return seed()


if __name__ == "__main__":
    sys.exit(main())
