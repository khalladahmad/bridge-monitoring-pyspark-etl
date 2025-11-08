@echo off
start python data_generator\data_generator.py
timeout /t 5
start python pipelines\bronze_ingest.py
timeout /t 5
start python pipelines\silver_enrichment.py
timeout /t 5
start python pipelines\gold_aggregation.py
