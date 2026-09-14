"""Rutas del repo. Funcionan aunque el notebook esté en notebooks/alumno/."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
STAGING = ROOT / "data" / "staging"
CURATED = ROOT / "data" / "curated"
COUNTS = ROOT / "data" / "CANONICAL_COUNTS.json"


def ensure_dirs() -> None:
    STAGING.mkdir(parents=True, exist_ok=True)
    CURATED.mkdir(parents=True, exist_ok=True)
