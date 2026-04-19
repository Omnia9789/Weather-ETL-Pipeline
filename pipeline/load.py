"""
pipeline/load.py — Load layer.

Persists the cleaned DataFrame into a SQLite database via SQLAlchemy.
Uses an upsert (INSERT OR REPLACE) strategy so the pipeline is safely
re-runnable without creating duplicates.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text

from config import DB_PATH, DB_URL

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    city             TEXT    NOT NULL,
    timestamp        TEXT    NOT NULL,
    temperature_c    REAL,
    humidity_pct     REAL,
    wind_speed_kmh   REAL,
    precipitation_mm REAL,
    weather_code     INTEGER,
    anomaly_flag     INTEGER DEFAULT 0,
    loaded_at        TEXT    DEFAULT (datetime('now')),
    UNIQUE (city, timestamp)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_city_ts ON weather (city, timestamp);
"""


def _ensure_schema(engine) -> None:
    """Create the weather table and index if they don't yet exist."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text(CREATE_INDEX_SQL))
    logger.debug("Schema verified / created.")


def load(df: pd.DataFrame) -> int:
    """
    Load *df* into the SQLite weather table.

    Duplicate (city, timestamp) pairs are silently ignored thanks to
    INSERT OR IGNORE so re-running the pipeline is always safe.

    Returns
    -------
    int  — number of new rows inserted
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    engine = create_engine(DB_URL)
    _ensure_schema(engine)

    # Convert timestamp to ISO string for SQLite compatibility
    df = df.copy()
    df["timestamp"]    = df["timestamp"].astype(str)
    df["anomaly_flag"] = df["anomaly_flag"].astype(int)

    # Stage into a temp table, then INSERT OR IGNORE into target
    with engine.begin() as conn:
        df.to_sql("_weather_stage", conn, if_exists="replace", index=False)
        result = conn.execute(text("""
            INSERT OR IGNORE INTO weather
                (city, timestamp, temperature_c, humidity_pct,
                 wind_speed_kmh, precipitation_mm, weather_code, anomaly_flag)
            SELECT
                city, timestamp, temperature_c, humidity_pct,
                wind_speed_kmh, precipitation_mm, weather_code, anomaly_flag
            FROM _weather_stage;
        """))
        inserted = result.rowcount
        conn.execute(text("DROP TABLE IF EXISTS _weather_stage;"))

    logger.info("Loaded %d new records into SQLite (skipped duplicates).", inserted)
    return inserted
