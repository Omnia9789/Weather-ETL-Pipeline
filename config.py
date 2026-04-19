"""
config.py — Central configuration for the Weather ETL Pipeline.
"""

import os

# ── Cities ────────────────────────────────────────────────────────────────────
CITIES = [
    {"name": "Cairo",        "latitude": 30.0626,  "longitude": 31.2497},
    {"name": "London",       "latitude": 51.5085,  "longitude": -0.1257},
    {"name": "New York",     "latitude": 40.7143,  "longitude": -74.006},
    {"name": "Tokyo",        "latitude": 35.6895,  "longitude": 139.6917},
    {"name": "Sydney",       "latitude": -33.8679, "longitude": 151.2073},
]

# ── API ───────────────────────────────────────────────────────────────────────
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Hourly variables to fetch
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "weathercode",
]

# Days of forecast/history to pull (past + forecast)
PAST_DAYS   = 7
FORECAST_DAYS = 1

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
RAW_DIR        = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR  = os.path.join(BASE_DIR, "data", "processed")
EXPORTS_DIR    = os.path.join(BASE_DIR, "data", "exports")
LOG_DIR        = os.path.join(BASE_DIR, "logs")
DB_PATH        = os.path.join(BASE_DIR, "data", "weather.db")
DB_URL         = f"sqlite:///{DB_PATH}"

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_VALID_TEMP =  -90.0   # °C  (world record low: −89.2 °C)
MAX_VALID_TEMP =   60.0   # °C  (world record high: 56.7 °C)
VARIANCE_THRESHOLD = 10.0  # °C  — flag days with temp range above this

# ── Scheduler ────────────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_HOURS = 6   # run every N hours
