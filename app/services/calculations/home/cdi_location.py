"""
Resolves a California ZIP code to a CDI location string.

Uses a bundled CSV (ca_zip_locations.csv, 2593 CA ZIP codes) — no network calls,
no external dependencies. The CSV was generated from the GeoNames public-domain
postal dataset pre-matched to the CDI interactive tool's location dropdown.

Usage:
    from app.services.calculations.home.cdi_location import resolve_location

    resolve_location("95111")   # "SANTA CLARA SAN JOSE - 95111"
    resolve_location("94102")   # "SAN FRANCISCO SAN FRANCISCO"
    resolve_location("90210")   # "LOS ANGELES BEVERLY HILLS"
"""
from __future__ import annotations

import csv
import os
from typing import Optional

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
_CSV_PATH = os.path.join(_DATA_DIR, "ca_zip_locations.csv")

# Lazy-loaded: zip -> cdi_location string
_zip_map: dict[str, str] | None = None


def _load() -> dict[str, str]:
    global _zip_map
    if _zip_map is None:
        _zip_map = {}
        with open(_CSV_PATH, newline="") as f:
            for row in csv.DictReader(f):
                if row["cdi_location"]:
                    _zip_map[row["zip"]] = row["cdi_location"]
    return _zip_map


_zip_info_map: dict[str, dict] | None = None


def _load_full() -> dict[str, dict]:
    global _zip_map, _zip_info_map
    if _zip_info_map is None:
        _zip_map = {}
        _zip_info_map = {}
        with open(_CSV_PATH, newline="") as f:
            for row in csv.DictReader(f):
                if row["cdi_location"]:
                    _zip_map[row["zip"]] = row["cdi_location"]
                    _zip_info_map[row["zip"]] = {
                        "county": row["county"],
                        "city": row["city"],
                        "cdi_location": row["cdi_location"],
                    }
    return _zip_info_map


def resolve_location(zip_code: str) -> Optional[str]:
    """
    Return the CDI location string for a California ZIP code, or None if not found.

    Args:
        zip_code: 5-digit ZIP code string, e.g. "95111"

    Returns:
        CDI location string, e.g. "SANTA CLARA SAN JOSE - 95111"
        None if the ZIP is not a California ZIP code.
    """
    return _load_full().get(zip_code.strip(), {}).get("cdi_location")


def resolve_zip_info(zip_code: str) -> Optional[dict]:
    """
    Return county, city, and CDI location for a California ZIP code.

    Returns:
        {"county": "SANTA CLARA", "city": "SAN JOSE", "cdi_location": "SANTA CLARA SAN JOSE - 95111"}
        None if the ZIP is not found.
    """
    return _load_full().get(zip_code.strip())


def resolve_location_strict(zip_code: str) -> str:
    """Like resolve_location but raises ValueError for unknown ZIP codes."""
    loc = resolve_location(zip_code)
    if loc is None:
        raise ValueError(f"ZIP code {zip_code!r} not found in California ZIP database")
    return loc
