"""
analysis/queries.py — Analytical layer.

Runs 6 SQL queries against the loaded SQLite database and exports results
to data/exports/ in both CSV and Parquet formats.

Queries
───────
1. avg_daily_temp          — Average daily temperature per city
2. hottest_coldest_hours   — Hottest and coldest hour per city
3. windiest_city_per_day   — City with the highest wind speed each day
4. monthly_temp_trend      — Month-over-month average temperature per city
5. hourly_temp_rank        — Hourly temperature rank within each city (window fn)
6. high_variance_days      — Days where temp range exceeds the threshold
"""

import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

from config import DB_URL, EXPORTS_DIR, VARIANCE_THRESHOLD

logger = logging.getLogger(__name__)

# ── SQL queries ───────────────────────────────────────────────────────────────

QUERY_AVG_DAILY_TEMP = """
SELECT
    city,
    DATE(timestamp)          AS date,
    ROUND(AVG(temperature_c), 2) AS avg_temp_c,
    ROUND(MIN(temperature_c), 2) AS min_temp_c,
    ROUND(MAX(temperature_c), 2) AS max_temp_c
FROM weather
WHERE anomaly_flag = 0
GROUP BY city, DATE(timestamp)
ORDER BY city, date;
"""

QUERY_HOTTEST_COLDEST_HOURS = """
WITH ranked AS (
    SELECT
        city,
        timestamp,
        temperature_c,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY temperature_c DESC) AS hot_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY temperature_c ASC)  AS cold_rank
    FROM weather
    WHERE anomaly_flag = 0
)
SELECT city, timestamp, temperature_c,
    CASE WHEN hot_rank  = 1 THEN 'hottest'
         WHEN cold_rank = 1 THEN 'coldest'
    END AS label
FROM ranked
WHERE hot_rank = 1 OR cold_rank = 1
ORDER BY city, label;
"""

QUERY_WINDIEST_CITY_PER_DAY = """
WITH daily_wind AS (
    SELECT
        city,
        DATE(timestamp)            AS date,
        ROUND(MAX(wind_speed_kmh), 2) AS max_wind_kmh
    FROM weather
    GROUP BY city, DATE(timestamp)
),
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY date ORDER BY max_wind_kmh DESC) AS rnk
    FROM daily_wind
)
SELECT city, date, max_wind_kmh
FROM ranked
WHERE rnk = 1
ORDER BY date;
"""

QUERY_MONTHLY_TEMP_TREND = """
SELECT
    city,
    STRFTIME('%Y-%m', timestamp)        AS month,
    ROUND(AVG(temperature_c), 2)        AS avg_temp_c,
    ROUND(AVG(temperature_c) - LAG(AVG(temperature_c), 1)
          OVER (PARTITION BY city ORDER BY STRFTIME('%Y-%m', timestamp)), 2
    )                                   AS mom_change_c
FROM weather
WHERE anomaly_flag = 0
GROUP BY city, STRFTIME('%Y-%m', timestamp)
ORDER BY city, month;
"""

QUERY_HOURLY_TEMP_RANK = """
SELECT
    city,
    timestamp,
    temperature_c,
    RANK() OVER (
        PARTITION BY city
        ORDER BY temperature_c DESC
    ) AS temp_rank_in_city
FROM weather
WHERE anomaly_flag = 0
ORDER BY city, temp_rank_in_city;
"""

QUERY_HIGH_VARIANCE_DAYS = f"""
SELECT
    city,
    DATE(timestamp)                              AS date,
    ROUND(MAX(temperature_c) - MIN(temperature_c), 2) AS temp_range_c,
    ROUND(MAX(temperature_c), 2)                 AS max_temp_c,
    ROUND(MIN(temperature_c), 2)                 AS min_temp_c
FROM weather
WHERE anomaly_flag = 0
GROUP BY city, DATE(timestamp)
HAVING temp_range_c > {VARIANCE_THRESHOLD}
ORDER BY temp_range_c DESC;
"""

QUERIES = {
    "avg_daily_temp":        QUERY_AVG_DAILY_TEMP,
    "hottest_coldest_hours": QUERY_HOTTEST_COLDEST_HOURS,
    "windiest_city_per_day": QUERY_WINDIEST_CITY_PER_DAY,
    "monthly_temp_trend":    QUERY_MONTHLY_TEMP_TREND,
    "hourly_temp_rank":      QUERY_HOURLY_TEMP_RANK,
    "high_variance_days":    QUERY_HIGH_VARIANCE_DAYS,
}


# ── Export helpers ────────────────────────────────────────────────────────────

def _export_csv(df: pd.DataFrame, name: str) -> str:
    path = os.path.join(EXPORTS_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    return path


def _export_parquet(df: pd.DataFrame, name: str) -> str:
    path  = os.path.join(EXPORTS_DIR, f"{name}.parquet")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    return path


# ── Main entry point ──────────────────────────────────────────────────────────

def run_analysis() -> dict[str, pd.DataFrame]:
    """
    Execute all 6 queries against the SQLite database.
    Export results to CSV and Parquet.

    Returns
    -------
    dict mapping query name → result DataFrame
    """
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    engine  = create_engine(DB_URL)
    results = {}

    for name, sql in QUERIES.items():
        try:
            df = pd.read_sql(sql, engine)
            results[name] = df

            csv_path     = _export_csv(df, name)
            parquet_path = _export_parquet(df, name)
            logger.info(
                "Query '%s' → %d rows  |  csv: %s  |  parquet: %s",
                name, len(df), os.path.basename(csv_path), os.path.basename(parquet_path),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Query '%s' failed: %s", name, exc)

    logger.info("Exported %d query results to %s", len(results), EXPORTS_DIR)
    return results
