"""
Main ETL pipeline orchestrator for CSV to BigQuery data loading.
Coordinates extraction, transformation, and loading processes.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

# Add src directory to path
sys.path.append(str(Path(__file__).parent))

from extractor import CSVExtractor
from transformer import clean_and_transform_data
from bigquery_loader import BigQueryLoader


class ETLPipeline:
    """
    Main ETL pipeline orchestrator that coordinates all ETL processes.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ETL pipeline.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.logger = self._setup_logging()
        self.start_time = None
        self.stats = {}
        
    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging configuration.
        
        Returns:
            logging.Logger: Configured logger
        """
        log_level = self.config.get('log_level', 'INFO')
        log_format = self.config.get('log_format', 
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Create logs directory if it doesn't exist
        log_dir = Path(self.config.get('log_dir', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        # Configure logging
        log_file = log_dir / f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"ETL Pipeline initialized. Log file: {log_file}")
        
        return logger
    
    def extract_data(self) -> Optional[pd.DataFrame]:
        """
        Extract data from CSV file.
        
        Returns:
            Optional[pd.DataFrame]: Extracted data or None if extraction fails
        """
        self.logger.info("Starting data extraction phase")
        
        try:
            # Get extraction configuration
            extract_config = self.config.get('extraction', {})
            csv_file = extract_config.get('csv_file')
            
            if not csv_file:
                raise ValueError("CSV file path not specified in configuration")
            
            # Initialize extractor
            extractor = CSVExtractor(csv_file)
            
            # Extract data with configuration
            df = extractor.extract_data(
                delimiter=extract_config.get('delimiter', ','),
                parse_dates=extract_config.get('parse_dates'),
                dtype=extract_config.get('dtype'),
                na_values=extract_config.get('na_values')
            )
            
            if df is None:
                raise RuntimeError("Data extraction failed")
            
            # Log extraction statistics
            info = extractor.get_data_info(df)
            self.stats['extraction'] = info
            
            self.logger.info(f"Data extraction completed successfully")
            self.logger.info(f"Extracted {info['total_rows']} rows and {info['total_columns']} columns")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            return None
    
    def transform_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transform and clean the extracted data.
        
        Args:
            df (pd.DataFrame): Raw extracted data
        
        Returns:
            Optional[pd.DataFrame]: Transformed data or None if transformation fails
        """
        self.logger.info("Starting data transformation phase")
        
        try:
            # Get transformation configuration
            transform_config = self.config.get('transformation', {})
            
            # Perform cleaning and transformation
            df_transformed = clean_and_transform_data(df, transform_config)
            
            # Calculate transformation statistics
            initial_rows = len(df)
            final_rows = len(df_transformed)
            valid_rows = df_transformed.get('is_valid', pd.Series([True] * final_rows)).sum()
            
            self.stats['transformation'] = {
                'initial_rows': initial_rows,
                'final_rows': final_rows,
                'valid_rows': int(valid_rows) if pd.api.types.is_numeric_dtype(type(valid_rows)) else final_rows,
                'rows_removed': initial_rows - final_rows,
                'data_quality_score': (valid_rows / final_rows * 100) if final_rows > 0 else 0
            }
            
            self.logger.info(f"Data transformation completed successfully")
            self.logger.info(f"Rows: {initial_rows} -> {final_rows} (removed: {initial_rows - final_rows})")
            self.logger.info(f"Data quality score: {self.stats['transformation']['data_quality_score']:.2f}%")
            
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}")
            return None
    
    def load_data(self, df: pd.DataFrame) -> bool:
        """
        Load transformed data to BigQuery.
        
        Args:
            df (pd.DataFrame): Transformed data
        
        Returns:
            bool: True if loading succeeds, False otherwise
        """
        self.logger.info("Starting data loading phase")
        
        try:
            # Get loading configuration
            load_config = self.config.get('loading', {})
            
            # Initialize BigQuery loader
            loader = BigQueryLoader(
                project_id=load_config.get('project_id'),
                dataset_id=load_config.get('dataset_id'),
                credentials_path=load_config.get('credentials_path')
            )
            
            # Load data
            success = loader.load_data(
                df=df,
                table_id=load_config.get('table_id'),
                write_disposition=load_config.get('write_disposition', 'WRITE_TRUNCATE'),
                create_table=load_config.get('create_table', True)
            )
            
            if success:
                # Get table info for statistics
                table_info = loader.get_table_info(load_config.get('table_id'))
                if table_info:
                    self.stats['loading'] = {
                        'table_rows': table_info['num_rows'],
                        'table_size_bytes': table_info['num_bytes'],
                        'load_timestamp': datetime.now().isoformat()
                    }
                
                self.logger.info("Data loading completed successfully")
                return True
            else:
                self.logger.error("Data loading failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            return False
    
    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            bool: True if pipeline succeeds, False otherwise
        """
        self.start_time = datetime.now()
        self.logger.info("="*60)
        self.logger.info("Starting ETL Pipeline Execution")
        self.logger.info("="*60)
        
        try:
            # Phase 1: Extract
            df_raw = self.extract_data()
            if df_raw is None:
                return False
            
            # Phase 2: Transform
            df_transformed = self.transform_data(df_raw)
            if df_transformed is None:
                return False
            
            # Phase 3: Load
            if not self.load_data(df_transformed):
                return False
            
            # Calculate overall statistics
            end_time = datetime.now()
            execution_time = end_time - self.start_time
            
            self.stats['overall'] = {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'execution_time_seconds': execution_time.total_seconds(),
                'status': 'SUCCESS'
            }
            
            self.logger.info("="*60)
            self.logger.info("ETL Pipeline Completed Successfully")
            self.logger.info(f"Total execution time: {execution_time}")
            self.logger.info("="*60)
            
            # Log final statistics
            self._log_final_stats()
            
            return True
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {e}")
            
            # Log failure statistics
            end_time = datetime.now()
            execution_time = end_time - self.start_time if self.start_time else None
            
            self.stats['overall'] = {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': end_time.isoformat(),
                'execution_time_seconds': execution_time.total_seconds() if execution_time else None,
                'status': 'FAILED',
                'error_message': str(e)
            }
            
            return False
    
    def _log_final_stats(self):
        """Log final pipeline statistics."""
        self.logger.info("Pipeline Statistics:")
        self.logger.info("-" * 40)
        
        # Extraction stats
        if 'extraction' in self.stats:
            ext_stats = self.stats['extraction']
            self.logger.info(f"Extraction - Rows: {ext_stats['total_rows']}, Columns: {ext_stats['total_columns']}")
        
        # Transformation stats
        if 'transformation' in self.stats:
            trans_stats = self.stats['transformation']
            self.logger.info(f"Transformation - Input: {trans_stats['initial_rows']}, Output: {trans_stats['final_rows']}")
            self.logger.info(f"Data Quality Score: {trans_stats['data_quality_score']:.2f}%")
        
        # Loading stats
        if 'loading' in self.stats:
            load_stats = self.stats['loading']
            self.logger.info(f"Loading - Rows loaded: {load_stats['table_rows']}")
        
        # Overall stats
        if 'overall' in self.stats:
            overall_stats = self.stats['overall']
            self.logger.info(f"Total execution time: {overall_stats['execution_time_seconds']:.2f} seconds")


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from file.
    
    Args:
        config_file (str): Path to configuration file
    
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    import json
    
    try:
        with open(config_file, 'r') as f:
            if config_file.endswith('.json'):
                return json.load(f)
            else:
                # Assume it's a Python file with CONFIG variable
                exec(f.read(), globals())
                return globals().get('CONFIG', {})
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return {}


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description='CSV to BigQuery ETL Pipeline')
    parser.add_argument('--config', '-c', 
                       default='config/etl_config.json',
                       help='Path to configuration file')
    parser.add_argument('--csv-file', 
                       help='Path to CSV file (overrides config)')
    parser.add_argument('--project-id', 
                       help='Google Cloud Project ID (overrides config)')
    parser.add_argument('--dataset-id', 
                       help='BigQuery Dataset ID (overrides config)')
    parser.add_argument('--table-id', 
                       help='BigQuery Table ID (overrides config)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override with command line arguments
    if args.csv_file:
        config.setdefault('extraction', {})['csv_file'] = args.csv_file
    if args.project_id:
        config.setdefault('loading', {})['project_id'] = args.project_id
    if args.dataset_id:
        config.setdefault('loading', {})['dataset_id'] = args.dataset_id
    if args.table_id:
        config.setdefault('loading', {})['table_id'] = args.table_id
    
    # Validate required configuration
    required_fields = [
        ('extraction', 'csv_file'),
        ('loading', 'project_id'),
        ('loading', 'dataset_id'),
        ('loading', 'table_id')
    ]
    
    for section, field in required_fields:
        if not config.get(section, {}).get(field):
            print(f"Error: Required configuration field '{section}.{field}' is missing")
            return 1
    
    # Run ETL pipeline
    pipeline = ETLPipeline(config)
    success = pipeline.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())