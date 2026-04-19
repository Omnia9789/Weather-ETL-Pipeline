"""
pipeline/extract.py — Extract layer.

Fetches hourly weather data from the Open-Meteo API for every city defined
in config.CITIES.  Raw JSON responses are persisted to data/raw/ for
reproducibility and debugging.
"""

import json
import logging
import os
import time
from datetime import datetime

import requests

from config import (
    CITIES,
    EXPORTS_DIR,
    FORECAST_DAYS,
    HOURLY_VARIABLES,
    OPEN_METEO_URL,
    PAST_DAYS,
    RAW_DIR,
)

logger = logging.getLogger(__name__)


def _build_params(city: dict) -> dict:
    """Build query-string parameters for a single city."""
    return {
        "latitude":       city["latitude"],
        "longitude":      city["longitude"],
        "hourly":         ",".join(HOURLY_VARIABLES),
        "past_days":      PAST_DAYS,
        "forecast_days":  FORECAST_DAYS,
        "timezone":       "UTC",
    }


def fetch_city(city: dict, retries: int = 3, backoff: float = 2.0) -> dict | None:
    """
    Fetch hourly weather data for a single city.

    Returns the parsed JSON payload, or None on permanent failure.
    Implements simple exponential back-off on transient errors.
    """
    params = _build_params(city)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            data["_city"] = city["name"]          # tag the response
            logger.debug("Fetched %s (attempt %d)", city["name"], attempt)
            return data
        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s", attempt, retries, city["name"], exc
            )
            if attempt < retries:
                time.sleep(backoff ** attempt)
    logger.error("All %d attempts failed for %s — skipping.", retries, city["name"])
    return None


def save_raw(data: dict, city_name: str) -> str:
    """Persist raw API response to data/raw/<city>_<date>.json."""
    os.makedirs(RAW_DIR, exist_ok=True)
    date_str   = datetime.utcnow().strftime("%Y%m%d")
    safe_name  = city_name.lower().replace(" ", "_")
    file_path  = os.path.join(RAW_DIR, f"{safe_name}_{date_str}.json")
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    return file_path


def extract_all() -> list[dict]:
    """
    Fetch data for all cities, save raw files, and return a list of
    raw API payloads.
    """
    results = []
    for city in CITIES:
        logger.info("Extracting: %s", city["name"])
        data = fetch_city(city)
        if data:
            path = save_raw(data, city["name"])
            logger.debug("Saved raw → %s", path)
            results.append(data)
    logger.info("Extracted data for %d / %d cities.", len(results), len(CITIES))
    return results
