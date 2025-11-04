"""
CSV to BigQuery ETL Pipeline

A comprehensive Python package for extracting data from CSV files,
cleaning and transforming it with Pandas, and loading it into Google BigQuery.
"""

__version__ = "1.0.0"
__author__ = "ETL Pipeline"
__description__ = "CSV to BigQuery ETL Pipeline"

# Import main classes for easier access
from .extractor import CSVExtractor, extract_csv_data
from .transformer import DataCleaner, DataTransformer, clean_and_transform_data
from .bigquery_loader import BigQueryLoader, load_to_bigquery
from .etl_pipeline import ETLPipeline

__all__ = [
    'CSVExtractor',
    'extract_csv_data',
    'DataCleaner',
    'DataTransformer', 
    'clean_and_transform_data',
    'BigQueryLoader',
    'load_to_bigquery',
    'ETLPipeline'
]