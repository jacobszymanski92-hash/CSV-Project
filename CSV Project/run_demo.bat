@echo off
echo Running CSV to BigQuery ETL Pipeline Demo...
echo ===============================================
.venv\Scripts\python.exe demo.py
echo.
echo Demo completed! 
echo To run the full pipeline, use: .venv\Scripts\python.exe src\etl_pipeline.py
pause