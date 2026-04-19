"""
pipeline/transform.py — Transform layer.

Converts raw Open-Meteo JSON payloads into a clean, normalised pandas
DataFrame ready for loading into SQLite.

Transformations applied
───────────────────────
1. Flatten the nested hourly JSON into tabular rows
2. Parse timestamps and cast numeric columns
3. Drop rows where temperature or wind speed is null
4. Remove duplicate (city, timestamp) pairs
5. Flag anomalous temperature readings outside physical bounds
6. Save an intermediate cleaned CSV to data/processed/
"""

import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

from config import (
    MAX_VALID_TEMP,
    MIN_VALID_TEMP,
    PROCESSED_DIR,
)

logger = logging.getLogger(__name__)

# Map Open-Meteo field names → canonical column names
COLUMN_MAP = {
    "time":                   "timestamp",
    "temperature_2m":         "temperature_c",
    "relative_humidity_2m":   "humidity_pct",
    "wind_speed_10m":         "wind_speed_kmh",
    "precipitation":          "precipitation_mm",
    "weathercode":            "weather_code",
}


def _flatten_payload(payload: dict) -> pd.DataFrame:
    """Turn a single city's API response into a flat DataFrame."""
    hourly = payload.get("hourly", {})
    df = pd.DataFrame(hourly)
    df["city"] = payload["_city"]
    return df


def _rename_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and cast to correct dtypes."""
    df = df.rename(columns=COLUMN_MAP)

    # Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Numeric casts — coerce any non-parseable values to NaN
    num_cols = ["temperature_c", "humidity_pct", "wind_speed_kmh",
                "precipitation_mm", "weather_code"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that are missing the two most critical measurements."""
    before = len(df)
    df = df.dropna(subset=["temperature_c", "wind_speed_kmh"])
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows with null temperature / wind_speed.", dropped)
    return df


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate (city, timestamp) pairs, keeping the first."""
    before = len(df)
    df = df.drop_duplicates(subset=["city", "timestamp"], keep="first")
    dupes = before - len(df)
    if dupes:
        logger.info("Removed %d duplicate (city, timestamp) rows.", dupes)
    return df


def _flag_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a boolean column 'anomaly_flag' for rows where temperature is
    outside the physical plausibility range defined in config.
    """
    df["anomaly_flag"] = (
        (df["temperature_c"] < MIN_VALID_TEMP) |
        (df["temperature_c"] > MAX_VALID_TEMP)
    )
    n_anomalies = df["anomaly_flag"].sum()
    if n_anomalies:
        logger.warning("Flagged %d anomalous temperature readings.", n_anomalies)
    return df


def _save_processed(df: pd.DataFrame) -> str:
    """Write cleaned data to data/processed/cleaned_<date>.csv."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    date_str  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(PROCESSED_DIR, f"cleaned_{date_str}.csv")
    df.to_csv(file_path, index=False)
    logger.debug("Saved processed → %s", file_path)
    return file_path


def transform(raw_payloads: list[dict]) -> pd.DataFrame:
    """
    Full transform pipeline.

    Parameters
    ----------
    raw_payloads : list of dicts returned by extract.extract_all()

    Returns
    -------
    pd.DataFrame  — clean, normalised weather records
    """
    if not raw_payloads:
        logger.error("No raw data to transform.")
        return pd.DataFrame()

    frames = [_flatten_payload(p) for p in raw_payloads]
    df     = pd.concat(frames, ignore_index=True)

    df = _rename_and_cast(df)
    df = _drop_nulls(df)
    df = _deduplicate(df)
    df = _flag_anomalies(df)

    # Reorder columns for readability
    col_order = [
        "city", "timestamp", "temperature_c", "humidity_pct",
        "wind_speed_kmh", "precipitation_mm", "weather_code", "anomaly_flag",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    _save_processed(df)
    logger.info(
        "Transform complete — %d clean records across %d cities.",
        len(df), df["city"].nunique(),
    )
    return df
