#!/usr/bin/env python3
"""Baja los jars del connector Mongo (versiones fijas). Ivy no resuelve el rango [5.1.1,5.1.99)."""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "labs" / "_shared" / "jars"
MAVEN = "https://repo1.maven.org/maven2"

# Connector 10.4.1 + driver 5.1.4 (el POM pide [5.1.1,5.1.99); Ivy no lo entiende).
FILES = [
    "org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar",
    "org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar",
    "org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar",
    "org/mongodb/bson/5.1.4/bson-5.1.4.jar",
    "org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar",
]


def main() -> int:
    DEST.mkdir(parents=True, exist_ok=True)
    for rel in FILES:
        name = rel.rsplit("/", 1)[-1]
        dest = DEST / name
        if dest.is_file() and dest.stat().st_size > 1000:
            print("ok ", dest.name)
            continue
        url = f"{MAVEN}/{rel}"
        print("get", dest.name)
        urllib.request.urlretrieve(url, dest)
        if dest.stat().st_size < 1000:
            dest.unlink(missing_ok=True)
            print("ERROR: descarga vacía", url, file=sys.stderr)
            return 1
    print("==> jars en", DEST)
    return 0


if __name__ == "__main__":
    sys.exit(main())
