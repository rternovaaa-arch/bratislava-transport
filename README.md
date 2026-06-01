# Bratislava Public Transport Intelligence Platform

A data engineering project that processes real GTFS data from Bratislava's public transport network (DPB) using Python and PySpark.

## Project Overview

This pipeline ingests, validates, transforms and analyses public transport data to generate meaningful KPIs about Bratislava's transit network.

## Architecture

```
data/raw/          ← raw GTFS files (untouched)
         ↓
src/ingestion/     ← load and parse GTFS files
         ↓
src/validation/    ← data quality checks
         ↓
src/processing/    ← PySpark transformations
         ↓
data/processed/    ← cleaned Parquet files
         ↓
src/analytics/     ← KPI calculations
         ↓
data/analytics/    ← final analytical outputs
```

## Data Source

GTFS feed from DPB (Dopravný podnik Bratislava)

| File | Records | Description |
|---|---|---|
| routes.txt | 93 | All bus, tram and trolleybus lines |
| stops.txt | 1,358 | All stops with GPS coordinates |
| trips.txt | 38,310 | All scheduled journeys |
| stop_times.txt | 734,373 | Arrival/departure times per stop |
| calendar.txt | 16 | Service schedules by day |
| calendar_dates.txt | 1,230 | Schedule exceptions |

## Key Findings

**Network composition (regular routes only):**
- 4 tram lines, 12 trolleybus lines, 54 day bus lines, 15 night bus lines
- 1,358 stops across the city
- All stops have valid GPS coordinates within Bratislava bounds
- Special routes excluded: X (diversions), Šk (school buses)

**Route coverage:**
- Tram 4 and Bus 29 serve the most stops (75 each)
- Trolleybus 44 covers 61 unique stops
- Night bus N61 covers 57 stops

**Daily travel patterns:**
- Morning peak: 6:00–7:00 (2,281–2,327 departures)
- Afternoon peak: 14:00–15:00 (2,295–2,302 departures)
- Quietest hours: 0:00–3:00 (144–228 departures)

**Data quality findings:**
- 79 out of 93 routes missing route_long_name (known DPB data issue)
- All critical fields (stop_id, stop_name, GPS coordinates) are complete
- Some routes appear multiple times due to temporary diversions (výluka)
- Route counts verified against official imhd.sk data

## Tech Stack

- **Python 3.12** — core language
- **PySpark 3.5** — distributed data processing
- **Parquet** — columnar storage format for processed data
- **pytest** — unit testing framework
- **Git** — version control

## How to Run

**1. Setup environment:**
```powershell
cd C:\projects\bratislava_mhd
venv\Scripts\activate
.\setup_env.ps1
```

**2. Load and process data:**
```powershell
python src/ingestion/load_gtfs.py
python src/processing/transform_gtfs.py
```

**3. Validate data quality:**
```powershell
python src/validation/validate_gtfs.py
```

**4. Run tests:**
```powershell
python -m pytest tests/ -v
```

## What I Learned

- How to structure a layered data engineering project
- Loading and processing real GTFS public transport data with PySpark
- Saving data in Parquet format for efficient storage and querying
- Writing data validation checks to assess real-world data quality
- Unit testing transformation logic with pytest
- Professional Git workflow witр commit messages
- How to investigate and fix real-world data quality issues
- Verifying analytical results against official data sources (imhd.sk)
- Understanding GTFS format and public transport data structures
- Debugging PySpark on Windows
