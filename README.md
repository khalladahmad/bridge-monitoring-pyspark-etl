# Bridge Monitoring with PySpark

This project simulates IoT sensors on bridges and implements a **streaming ETL pipeline** using PySpark Structured Streaming. The pipeline follows the **Bronze → Silver → Gold** medallion architecture to ingest, clean, enrich, and aggregate sensor data (temperature, vibration, tilt) in real time.

## Project Structure
```
bridge-monitoring-pyspark/
│
├── data_generator/
│   └── data_generator.py          # Simulates sensor events
│
├── pipelines/
│   ├── bronze_ingest.py           # Bronze ingestion
│   ├── silver_enrichment.py       # Silver enrichment
│   └── gold_aggregation.py        # Gold aggregation
│
├── notebooks/
│   └── demo.ipynb                 # Queries & visualizations
│
├── metadata/
│   └── bridges.csv                # Bridge metadata
│
├── scripts/
│   └── run_all.bat                # Optional run script
│
└── checkpoints/                   # Spark checkpoints (gitignored)
```

## Requirements

- Python 3.8+
- PySpark 3.x
- Jupyter Notebook (for demo)
- Pandas, Matplotlib, Seaborn

## Setup

```bash
git clone https://github.com/khalladahmad/bridge-monitoring-pyspark-etl.git
cd bridge-monitoring-pyspark
python -m venv venv
# Activate venv
.\venv\Scripts\Activate.ps1   # PowerShell
```
## How to Run

**1) Generate sensor data:**
```
python data_generator\data_generator.py
```

**2) Run ETL pipelines:**
```
python pipelines\bronze_ingest.py
python pipelines\silver_enrichment.py
python pipelines\gold_aggregation.py
```

**3) Open the demo notebook:**
```
jupyter notebook notebooks/demo.ipynb
```
