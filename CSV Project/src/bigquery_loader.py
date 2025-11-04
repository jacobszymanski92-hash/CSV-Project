"""
BigQuery loader module for CSV to BigQuery ETL pipeline.
Handles connection, authentication, and data loading to Google BigQuery.
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any, List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.auth import default
import json
from pathlib import Path


class BigQueryLoader:
    """
    Handles loading data into Google BigQuery with schema management and error handling.
    """
    
    def __init__(self, 
                 project_id: str,
                 dataset_id: str,
                 credentials_path: Optional[str] = None):
        """
        Initialize the BigQuery loader.
        
        Args:
            project_id (str): Google Cloud Project ID
            dataset_id (str): BigQuery dataset ID
            credentials_path (Optional[str]): Path to service account credentials JSON file
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self.logger = logging.getLogger(__name__)
        self.client = None
        
    def initialize_client(self) -> bool:
        """
        Initialize BigQuery client with authentication.
        
        Returns:
            bool: True if client initialized successfully, False otherwise
        """
        try:
            if self.credentials_path and Path(self.credentials_path).exists():
                # Use service account credentials
                self.client = bigquery.Client.from_service_account_json(
                    self.credentials_path,
                    project=self.project_id
                )
                self.logger.info("BigQuery client initialized with service account credentials")
            else:
                # Use default credentials (ADC)
                self.client = bigquery.Client(project=self.project_id)
                self.logger.info("BigQuery client initialized with default credentials")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
            return False
    
    def create_dataset(self, location: str = "US") -> bool:
        """
        Create BigQuery dataset if it doesn't exist.
        
        Args:
            location (str): Dataset location, defaults to "US"
        
        Returns:
            bool: True if dataset exists or was created successfully, False otherwise
        """
        if not self.client:
            if not self.initialize_client():
                return False
        
        assert self.client is not None  # Type guard for mypy
        
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            
            # Check if dataset exists
            try:
                dataset = self.client.get_dataset(dataset_ref)
                self.logger.info(f"Dataset {self.dataset_id} already exists")
                return True
            except NotFound:
                # Create dataset
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location
                dataset.description = "Dataset for ETL pipeline data"
                
                dataset = self.client.create_dataset(dataset)
                self.logger.info(f"Created dataset {self.dataset_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to create dataset: {e}")
            return False
    
    def create_table_schema(self, df: pd.DataFrame) -> List[bigquery.SchemaField]:
        """
        Create BigQuery schema from pandas DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to generate schema from
        
        Returns:
            List[bigquery.SchemaField]: List of schema fields
        """
        schema = []
        
        for column, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                bq_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                bq_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                bq_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                bq_type = "TIMESTAMP"
            elif isinstance(dtype, pd.CategoricalDtype):
                bq_type = "STRING"
            else:
                bq_type = "STRING"
            
            # Determine if field can be nullable
            mode = "NULLABLE" if df[column].isnull().any() else "NULLABLE"  # Keep nullable for flexibility
            
            schema.append(bigquery.SchemaField(str(column), bq_type, mode=mode))
        
        return schema
    
    def create_table(self, 
                    table_id: str, 
                    schema: List[bigquery.SchemaField],
                    overwrite: bool = False) -> bool:
        """
        Create BigQuery table with specified schema.
        
        Args:
            table_id (str): Table ID
            schema (List[bigquery.SchemaField]): Table schema
            overwrite (bool): Whether to overwrite existing table
        
        Returns:
            bool: True if table was created successfully, False otherwise
        """
        if not self.client:
            if not self.initialize_client():
                return False
        
        assert self.client is not None  # Type guard for mypy
        
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            
            # Check if table exists
            try:
                existing_table = self.client.get_table(table_ref)
                if overwrite:
                    self.client.delete_table(table_ref)
                    self.logger.info(f"Deleted existing table {table_id}")
                else:
                    self.logger.info(f"Table {table_id} already exists")
                    return True
            except NotFound:
                pass
            
            # Create table
            table = bigquery.Table(table_ref, schema=schema)
            table.description = f"Table created by ETL pipeline on {pd.Timestamp.now()}"
            
            table = self.client.create_table(table)
            self.logger.info(f"Created table {table_id} with {len(schema)} columns")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return False
    
    def load_data(self, 
                 df: pd.DataFrame, 
                 table_id: str,
                 write_disposition: str = "WRITE_TRUNCATE",
                 create_table: bool = True) -> bool:
        """
        Load DataFrame to BigQuery table.
        
        Args:
            df (pd.DataFrame): DataFrame to load
            table_id (str): Target table ID
            write_disposition (str): Write disposition ("WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY")
            create_table (bool): Whether to create table if it doesn't exist
        
        Returns:
            bool: True if data was loaded successfully, False otherwise
        """
        if not self.client:
            if not self.initialize_client():
                return False
        
        assert self.client is not None  # Type guard for mypy
        
        if not self.create_dataset():
            return False
        
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            
            # Create table if it doesn't exist and create_table is True
            if create_table:
                try:
                    self.client.get_table(table_ref)
                except NotFound:
                    schema = self.create_table_schema(df)
                    if not self.create_table(table_id, schema):
                        return False
            
            # Prepare DataFrame for BigQuery
            df_clean = self._prepare_dataframe_for_bq(df)
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=False,  # Use explicit schema
                ignore_unknown_values=False,
                max_bad_records=0
            )
            
            self.logger.info(f"Starting data load to {table_id}. Rows: {len(df_clean)}")
            
            # Load data
            job = self.client.load_table_from_dataframe(
                df_clean, 
                table_ref, 
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            # Get final table info
            table = self.client.get_table(table_ref)
            self.logger.info(f"Successfully loaded {table.num_rows} rows to {table_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load data to BigQuery: {e}")
            return False
    
    def _prepare_dataframe_for_bq(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for BigQuery loading by handling data type issues.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            pd.DataFrame: Prepared DataFrame
        """
        df_clean = df.copy()
        
        # Convert datetime columns to proper format
        for column in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[column]):
                # Convert to UTC and handle NaT values
                df_clean[column] = pd.to_datetime(df_clean[column], utc=True, errors='coerce')
            
            # Handle object columns that might contain mixed types
            elif df_clean[column].dtype == 'object':
                df_clean[column] = df_clean[column].astype(str)
                df_clean[column] = df_clean[column].replace('nan', None)
                df_clean[column] = df_clean[column].replace('None', None)
            
            # Handle categorical columns
            elif isinstance(df_clean[column].dtype, pd.CategoricalDtype):
                df_clean[column] = df_clean[column].astype(str)
        
        return df_clean
    
    def query_table(self, 
                   table_id: str, 
                   limit: Optional[int] = None,
                   where_clause: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Query data from BigQuery table.
        
        Args:
            table_id (str): Table ID to query
            limit (Optional[int]): Limit number of rows
            where_clause (Optional[str]): WHERE clause for filtering
        
        Returns:
            Optional[pd.DataFrame]: Query results as DataFrame or None if query fails
        """
        if not self.client:
            if not self.initialize_client():
                return None
        
        assert self.client is not None  # Type guard for mypy
        
        try:
            query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_id}`"
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            self.logger.info(f"Executing query: {query}")
            
            # Execute query
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert to DataFrame
            df = results.to_dataframe()
            self.logger.info(f"Query returned {len(df)} rows")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to query table: {e}")
            return None
    
    def get_table_info(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a BigQuery table.
        
        Args:
            table_id (str): Table ID
        
        Returns:
            Optional[Dict[str, Any]]: Table information or None if table doesn't exist
        """
        if not self.client:
            if not self.initialize_client():
                return None
        
        assert self.client is not None  # Type guard for mypy
        
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            info = {
                'project_id': table.project,
                'dataset_id': table.dataset_id,
                'table_id': table.table_id,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': [{'name': field.name, 'type': field.field_type, 'mode': field.mode} 
                          for field in table.schema],
                'description': table.description
            }
            
            return info
            
        except NotFound:
            self.logger.error(f"Table {table_id} not found")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            return None


def load_to_bigquery(df: pd.DataFrame, 
                    project_id: str,
                    dataset_id: str,
                    table_id: str,
                    credentials_path: Optional[str] = None,
                    **kwargs) -> bool:
    """
    Convenience function to load DataFrame to BigQuery.
    
    Args:
        df (pd.DataFrame): DataFrame to load
        project_id (str): Google Cloud Project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        credentials_path (Optional[str]): Path to service account credentials
        **kwargs: Additional arguments passed to load_data method
    
    Returns:
        bool: True if data was loaded successfully, False otherwise
    """
    loader = BigQueryLoader(project_id, dataset_id, credentials_path)
    return loader.load_data(df, table_id, **kwargs)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # This would typically be called with transformed data
    print("BigQuery loader module ready for use")
    print("Note: Requires Google Cloud credentials and project setup")