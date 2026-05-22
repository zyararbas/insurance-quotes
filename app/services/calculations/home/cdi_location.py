"""
Resolves a California ZIP code to a CDI location string.

Uses the home_ca_zip_locations MongoDB collection (2593 CA ZIP codes).
"""
from __future__ import annotations

from typing import Optional

from app.services.storage_service import StorageService

# Lazy-loaded: zip -> cdi_location string
_zip_map: dict[str, str] | None = None
_zip_info_map: dict[str, dict] | None = None


def _load_full() -> dict[str, dict]:
    global _zip_map, _zip_info_map
    if _zip_info_map is None:
        _zip_map = {}
        _zip_info_map = {}
        for doc in StorageService().find({}, "home_ca_zip_locations"):
            if doc.get("cdi_location"):
                zip_str = str(doc["zip"]).zfill(5)
                _zip_map[zip_str] = doc["cdi_location"]
                _zip_info_map[zip_str] = {
                    "county": doc["county"],
                    "city": doc["city"],
                    "cdi_location": doc["cdi_location"],
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


_load_full()
