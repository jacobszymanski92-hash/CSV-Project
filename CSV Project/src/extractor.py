"""
Data extraction module for CSV to BigQuery ETL pipeline.
Handles reading and initial validation of CSV data using pandas.
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path


class CSVExtractor:
    """
    Handles extraction of data from CSV files with validation and error handling.
    """
    
    def __init__(self, file_path: str, encoding: str = 'utf-8'):
        """
        Initialize the CSV extractor.
        
        Args:
            file_path (str): Path to the CSV file
            encoding (str): File encoding, defaults to 'utf-8'
        """
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.logger = logging.getLogger(__name__)
        
    def validate_file_exists(self) -> bool:
        """
        Check if the CSV file exists and is readable.
        
        Returns:
            bool: True if file exists and is readable, False otherwise
        """
        if not self.file_path.exists():
            self.logger.error(f"File does not exist: {self.file_path}")
            return False
        
        if not self.file_path.is_file():
            self.logger.error(f"Path is not a file: {self.file_path}")
            return False
            
        return True
    
    def extract_data(self, 
                    delimiter: str = ',', 
                    header: int = 0,
                    dtype: Optional[Dict[str, Any]] = None,
                    parse_dates: Optional[List[str]] = None,
                    na_values: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Extract data from CSV file using pandas.
        
        Args:
            delimiter (str): CSV delimiter, defaults to ','
            header (int): Row number to use as column names, defaults to 0
            dtype (Optional[Dict[str, Any]]): Data types for columns
            parse_dates (Optional[List[str]]): Columns to parse as dates
            na_values (Optional[List[str]]): Additional strings to recognize as NA/NaN
            
        Returns:
            Optional[pd.DataFrame]: Extracted DataFrame or None if extraction fails
        """
        if not self.validate_file_exists():
            return None
        
        try:
            # Set default NA values if not provided
            if na_values is None:
                na_values = ['', 'NULL', 'null', 'N/A', 'n/a', '#N/A']
            
            self.logger.info(f"Starting extraction from: {self.file_path}")
            
            # Read CSV file
            df = pd.read_csv(
                self.file_path,
                delimiter=delimiter,
                header=header,
                dtype=dtype,  # type: ignore[arg-type]
                parse_dates=parse_dates,
                na_values=na_values,
                encoding=self.encoding,
                low_memory=False  # Prevents DtypeWarning for mixed types
            )
            
            self.logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            self.logger.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.file_path}")
            return None
        except pd.errors.EmptyDataError:
            self.logger.error(f"No data found in file: {self.file_path}")
            return None
        except pd.errors.ParserError as e:
            self.logger.error(f"Error parsing CSV file: {e}")
            return None
        except UnicodeDecodeError as e:
            self.logger.error(f"Encoding error reading file: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error during extraction: {e}")
            return None
    
    def get_data_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get basic information about the extracted data.
        
        Args:
            df (pd.DataFrame): The extracted DataFrame
            
        Returns:
            Dict[str, Any]: Dictionary containing data information
        """
        info = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'data_types': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }
        
        return info
    
    def preview_data(self, df: pd.DataFrame, num_rows: int = 5) -> Dict[str, Any]:
        """
        Get a preview of the data for inspection.
        
        Args:
            df (pd.DataFrame): The extracted DataFrame
            num_rows (int): Number of rows to preview, defaults to 5
            
        Returns:
            Dict[str, Any]: Dictionary containing data preview
        """
        preview = {
            'head': df.head(num_rows).to_dict('records'),
            'tail': df.tail(num_rows).to_dict('records'),
            'sample': df.sample(min(num_rows, len(df))).to_dict('records') if len(df) > 0 else []
        }
        
        return preview


def extract_csv_data(file_path: str, **kwargs) -> Optional[pd.DataFrame]:
    """
    Convenience function to extract data from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file
        **kwargs: Additional arguments passed to extract_data method
        
    Returns:
        Optional[pd.DataFrame]: Extracted DataFrame or None if extraction fails
    """
    extractor = CSVExtractor(file_path)
    return extractor.extract_data(**kwargs)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Extract sample data
    extractor = CSVExtractor("data/sample_customers.csv")
    df = extractor.extract_data(parse_dates=['registration_date', 'last_purchase_date'])
    
    if df is not None:
        info = extractor.get_data_info(df)
        preview = extractor.preview_data(df)
        
        print("Data Information:")
        for key, value in info.items():
            print(f"  {key}: {value}")
        
        print("\nData Preview (first 3 rows):")
        for i, row in enumerate(preview['head'][:3]):
            print(f"  Row {i + 1}: {row}")