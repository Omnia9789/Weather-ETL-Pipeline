# Weather ETL Pipeline

> An end-to-end ETL data pipeline that extracts weather data from a public API, transforms it with Python, loads it into a SQLite database, and exports analytical results to CSV and Parquet formats.

---

## Overview

This project demonstrates a production-style ETL (Extract → Transform → Load) pipeline built entirely with Python. It pulls real weather data for multiple cities from the [Open-Meteo API](https://open-meteo.com/) (free, no API key required), cleans and normalizes the data, loads it into a relational database, and runs SQL analytical queries to surface insights — all exportable in multiple formats.

**Built to showcase:** ETL/ELT concepts, data pipeline design, SQL analytics, Python data engineering, and multi-format data handling (CSV, JSON, Parquet).

---

## Key Features

- **Extract** — Fetches hourly weather data for multiple cities via REST API (Open-Meteo)
- **Transform** — Cleans nulls, deduplicates records, normalizes schema, casts types, handles edge cases
- **Load** — Stores structured data in a local SQLite database via SQLAlchemy
- **Analyze** — Runs 6 SQL analytical queries (aggregations, window functions, trend detection)
- **Export** — Outputs results to CSV, JSON, and Parquet (via PyArrow)
- **Logging** — Pipeline steps are logged with timestamps for observability
- **Scheduled** — Can be run on a schedule via cron or Python scheduler

---

## Project Structure

```
weather-etl-pipeline/
│
├── data/
│   ├── raw/                    # Raw API responses (JSON)
│   ├── processed/              # Cleaned intermediate data (CSV)
│   └── exports/                # Final analytical outputs (CSV, Parquet)
│
├── pipeline/
│   ├── __init__.py
│   ├── extract.py              # API calls, raw data ingestion
│   ├── transform.py            # Cleaning, normalization, validation
│   └── load.py                 # SQLite loading via SQLAlchemy
│
├── analysis/
│   ├── __init__.py
│   └── queries.py              # SQL analytical queries + export logic
│
├── logs/
│   └── pipeline.log            # Pipeline run logs
│
├── config.py                   # Cities list, API config, file paths
├── main.py                     # Pipeline orchestrator (run this)
├── schedule.py                 # Optional: scheduled runs
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| HTTP / Extract | `requests` |
| Transform | `pandas`, `numpy` |
| Load | `SQLite`, `SQLAlchemy` |
| Parquet Export | `pyarrow` |
| Logging | Python `logging` module |
| Version Control | Git & GitHub |

---

## Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/weather-etl-pipeline.git
cd weather-etl-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
```bash
python main.py
```

### 4. (Optional) Schedule it
```bash
python schedule.py
```

---

## Pipeline Walkthrough

### Extract
`pipeline/extract.py` — Calls the Open-Meteo API for a configurable list of cities. Raw JSON responses are saved to `data/raw/` for reproducibility.

### Transform
`pipeline/transform.py` — Applies the following transformations:
- Drops rows with null temperature or wind speed values
- Deduplicates on `(city, timestamp)` primary key
- Normalizes column names and casts data types
- Flags anomalous readings (e.g., temperatures outside physical range)

### Load
`pipeline/load.py` — Creates/updates a `weather` table in SQLite using SQLAlchemy. Uses upsert logic to avoid duplicates on re-runs.

### Analyze
`analysis/queries.py` — Runs 6 analytical SQL queries:
1. Average daily temperature per city
2. Hottest and coldest hours per city
3. City with highest wind speed on each day
4. Month-over-month temperature trend
5. Hourly temperature ranking within each city (window function)
6. Days with temperature variance > threshold (anomaly detection)

Results are exported to `data/exports/` as both `.csv` and `.parquet`.

---

## Sample Output

```
[2026-04-14 10:00:01] INFO  — Starting ETL pipeline
[2026-04-14 10:00:02] INFO  — Extracted 1,440 records for 5 cities
[2026-04-14 10:00:03] INFO  — Dropped 12 null rows, 3 duplicates
[2026-04-14 10:00:04] INFO  — Loaded 1,425 records into SQLite
[2026-04-14 10:00:05] INFO  — Exported 6 query results to data/exports/
[2026-04-14 10:00:05] INFO  — Pipeline complete in 4.2s
```

---

## Key Findings

> *(Update this section after your first run with real insights)*

- Cairo showed the highest average daytime temperature across all cities at **38.4°C**
- Wind speed anomalies were detected on 3 occasions, all in coastal cities
- Temperature variance was highest between 06:00–09:00 local time across all cities

---

## Requirements

```
requests
pandas
numpy
sqlalchemy
pyarrow
schedule
```

---

## Author

**Omnia Ali Mohamed Ali**
AI & Data Engineering | Cairo University, CS (AI Major)
[GitHub](https://github.com/YOUR_USERNAME) · [LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [Portfolio](YOUR_PORTFOLIO)
