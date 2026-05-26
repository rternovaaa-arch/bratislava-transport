\# Bratislava Public Transport Intelligence Platform



A data engineering project that processes real GTFS data from Bratislava's

public transport network (DPB) using Python and PySpark.



\## Project Overview



This pipeline ingests, validates, transforms and analyses public transport

data to generate meaningful KPIs about Bratislava's transit network.



\## Architecture
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

## Data Source



\*\*GTFS feed from DPB (Dopravný podnik Bratislava)\*\*



| File | Records | Description |

|---|---|---|

| routes.txt | 93 | All bus, tram and trolleybus lines |

| stops.txt | 1,358 | All stops with GPS coordinates |

| trips.txt | 38,310 | All scheduled journeys |

| stop\_times.txt | 734,373 | Arrival/departure times per stop |

| calendar.txt | 16 | Service schedules by day |

| calendar\_dates.txt | 1,230 | Schedule exceptions |



\## Key Findings



\*\*Network composition:\*\*

\- 73 bus lines, 16 trolleybus lines, 4 tram lines

\- 1,358 stops across the city

\- All stops have valid GPS coordinates within Bratislava bounds



\*\*Route coverage:\*\*

\- Tram 4 and Bus 29 serve the most stops (75 each)

\- Trolleybus 44 covers 61 unique stops

\- Night bus N61 covers 57 stops



\*\*Daily travel patterns:\*\*

\- Morning peak: 6:00–7:00 (2,281–2,327 departures)

\- Afternoon peak: 14:00–15:00 (2,295–2,302 departures)

\- Quietest hours: 0:00–3:00 (144–228 departures)



\*\*Data quality:\*\*

\- 79 out of 93 routes missing route\_long\_name (known DPB data issue)

\- All critical fields (stop\_id, stop\_name, GPS coordinates) are complete



\## Tech Stack



\- \*\*Python 3.12\*\* — core language

\- \*\*PySpark 3.5\*\* — distributed data processing

\- \*\*Parquet\*\* — columnar storage format for processed data

\- \*\*pytest\*\* — unit testing framework

\- \*\*Git\*\* — version control



\## Project Structure

bratislava\_mhd/

├── data/

│   ├── raw/          # original GTFS files

│   ├── processed/    # cleaned Parquet datasets

│   └── analytics/    # final KPI outputs

├── src/

│   ├── ingestion/    # load\_gtfs.py, explore\_gtfs.py

│   ├── processing/   # transform\_gtfs.py

│   ├── validation/   # validate\_gtfs.py

│   └── analytics/    # (future: visualizations)

├── tests/

│   └── test\_transformations.py

├── requirements.txt

└── setup\_env.ps1

\## How to Run



\*\*1. Setup environment:\*\*

```powershell

cd C:\\projects\\bratislava\_mhd

venv\\Scripts\\activate

.\\setup\_env.ps1

```



\*\*2. Load and process data:\*\*

```powershell

python src/ingestion/load\_gtfs.py

python src/processing/transform\_gtfs.py

```



\*\*3. Validate data quality:\*\*

```powershell

python src/validation/validate\_gtfs.py

```



\*\*4. Run tests:\*\*

```powershell

python -m pytest tests/ -v

```



\## What I Learned



\- How to structure a layered data engineering project

\- Loading and processing real GTFS public transport data with PySpark

\- Saving data in Parquet format for efficient storage and querying

\- Writing data validation checks to assess real-world data quality

\- Unit testing transformation logic with pytest

\- Professional Git workflow with meaningful commit messages

