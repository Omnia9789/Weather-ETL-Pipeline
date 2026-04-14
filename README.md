Weather ETL Pipeline

An end-to-end ETL data pipeline that extracts weather data from a public API, transforms it with Python, loads it into a SQLite database, and exports analytical results to CSV and Parquet formats.


Overview
This project demonstrates a production-style ETL (Extract → Transform → Load) pipeline built entirely with Python. It pulls real weather data for multiple cities from the Open-Meteo API (free, no API key required), cleans and normalizes the data, loads it into a relational database, and runs SQL analytical queries to surface insights — all exportable in multiple formats.
Built to showcase: ETL/ELT concepts, data pipeline design, SQL analytics, Python data engineering, and multi-format data handling (CSV, JSON, Parquet).

Key Features

Extract — Fetches hourly weather data for multiple cities via REST API (Open-Meteo)
Transform — Cleans nulls, deduplicates records, normalizes schema, casts types, handles edge cases
Load — Stores structured data in a local SQLite database via SQLAlchemy
Analyze — Runs 6 SQL analytical queries (aggregations, window functions, trend detection)
Export — Outputs results to CSV, JSON, and Parquet (via PyArrow)
Logging — Pipeline steps are logged with timestamps for observability
Scheduled — Can be run on a schedule via cron or Python scheduler
