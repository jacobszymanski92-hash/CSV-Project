"""
Data cleaning and transformation module for CSV to BigQuery ETL pipeline.
Handles data quality issues, standardization, and transformations.
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Optional, Dict, Any, List, Callable, Literal, Union
from datetime import datetime


class DataCleaner:
    """
    Handles data cleaning operations including missing values, duplicates, and standardization.
    """
    
    def __init__(self):
        """Initialize the data cleaner."""
        self.logger = logging.getLogger(__name__)
        
    def handle_missing_values(self, 
                            df: pd.DataFrame, 
                            strategy: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame using various strategies.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            strategy (Dict[str, Any]): Strategy for handling missing values per column
                                     Format: {'column_name': 'strategy' or value}
                                     Strategies: 'drop', 'mean', 'median', 'mode', 'forward_fill', 'backward_fill', custom_value
        
        Returns:
            pd.DataFrame: DataFrame with missing values handled
        """
        df_cleaned = df.copy()
        
        if strategy is None:
            strategy = {}
        
        self.logger.info("Starting missing value handling")
        
        for column in df_cleaned.columns:
            if df_cleaned[column].isnull().sum() > 0:
                column_strategy = strategy.get(column, 'drop')
                
                self.logger.info(f"Handling missing values in '{column}' using strategy: {column_strategy}")
                
                if column_strategy == 'drop':
                    # Drop rows with missing values in this column
                    df_cleaned = df_cleaned.dropna(subset=[column])
                elif column_strategy == 'mean' and df_cleaned[column].dtype in ['int64', 'float64']:
                    df_cleaned[column].fillna(df_cleaned[column].mean(), inplace=True)
                elif column_strategy == 'median' and df_cleaned[column].dtype in ['int64', 'float64']:
                    df_cleaned[column].fillna(df_cleaned[column].median(), inplace=True)
                elif column_strategy == 'mode':
                    mode_value = df_cleaned[column].mode()
                    if len(mode_value) > 0:
                        df_cleaned[column].fillna(mode_value[0], inplace=True)
                elif column_strategy == 'forward_fill':
                    df_cleaned[column] = df_cleaned[column].ffill()
                elif column_strategy == 'backward_fill':
                    df_cleaned[column] = df_cleaned[column].bfill()
                else:
                    # Use custom value
                    df_cleaned[column].fillna(column_strategy, inplace=True)
        
        self.logger.info(f"Missing value handling complete. Rows: {len(df)} -> {len(df_cleaned)}")
        return df_cleaned
    
    def remove_duplicates(self, 
                         df: pd.DataFrame, 
                         subset: Optional[List[str]] = None,
                         keep: Union[Literal['first'], Literal['last'], Literal[False]] = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows from the DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            subset (Optional[List[str]]): Columns to consider for identifying duplicates
            keep (str): Which duplicates to keep ('first', 'last', False)
        
        Returns:
            pd.DataFrame: DataFrame with duplicates removed
        """
        df_cleaned = df.copy()
        
        initial_count = len(df_cleaned)
        df_cleaned = df_cleaned.drop_duplicates(subset=subset, keep=keep)
        final_count = len(df_cleaned)
        
        duplicates_removed = initial_count - final_count
        self.logger.info(f"Removed {duplicates_removed} duplicate rows. Rows: {initial_count} -> {final_count}")
        
        return df_cleaned
    
    def standardize_text(self, 
                        df: pd.DataFrame, 
                        columns: List[str],
                        operations: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Standardize text data in specified columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[str]): Columns to standardize
            operations (List[str]): List of operations ('lower', 'upper', 'title', 'strip', 'remove_special')
        
        Returns:
            pd.DataFrame: DataFrame with standardized text
        """
        if operations is None:
            operations = ['strip', 'lower']
        
        df_cleaned = df.copy()
        
        for column in columns:
            if column in df_cleaned.columns:
                self.logger.info(f"Standardizing text in column: {column}")
                
                for operation in operations:
                    if operation == 'lower':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.lower()
                    elif operation == 'upper':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.upper()
                    elif operation == 'title':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.title()
                    elif operation == 'strip':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.strip()
                    elif operation == 'remove_special':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        
        return df_cleaned
    
    def validate_email(self, df: pd.DataFrame, email_column: str) -> pd.DataFrame:
        """
        Validate and clean email addresses.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            email_column (str): Name of the email column
        
        Returns:
            pd.DataFrame: DataFrame with validated emails
        """
        df_cleaned = df.copy()
        
        if email_column in df_cleaned.columns:
            self.logger.info(f"Validating emails in column: {email_column}")
            
            # Email regex pattern
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            # Mark invalid emails
            valid_emails = df_cleaned[email_column].astype(str).str.match(email_pattern, na=False)
            invalid_count = (~valid_emails).sum()
            
            if invalid_count > 0:
                self.logger.warning(f"Found {invalid_count} invalid email addresses")
                # Option 1: Set invalid emails to NaN
                df_cleaned.loc[~valid_emails, email_column] = np.nan
                # Option 2: Could also remove rows with invalid emails
                # df_cleaned = df_cleaned[valid_emails]
        
        return df_cleaned
    
    def validate_phone(self, df: pd.DataFrame, phone_column: str) -> pd.DataFrame:
        """
        Validate and standardize phone numbers.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            phone_column (str): Name of the phone column
        
        Returns:
            pd.DataFrame: DataFrame with validated phone numbers
        """
        df_cleaned = df.copy()
        
        if phone_column in df_cleaned.columns:
            self.logger.info(f"Validating phone numbers in column: {phone_column}")
            
            # Remove non-numeric characters and standardize format
            df_cleaned[phone_column] = df_cleaned[phone_column].astype(str).str.replace(r'[^\d]', '', regex=True)
            
            # Format as XXX-XXX-XXXX for 10-digit numbers
            mask = df_cleaned[phone_column].str.len() == 10
            df_cleaned.loc[mask, phone_column] = (
                df_cleaned.loc[mask, phone_column].str[:3] + '-' +
                df_cleaned.loc[mask, phone_column].str[3:6] + '-' +
                df_cleaned.loc[mask, phone_column].str[6:]
            )
            
            # Set invalid phone numbers to NaN
            invalid_mask = ~mask & (df_cleaned[phone_column].str.len() != 12)
            df_cleaned.loc[invalid_mask, phone_column] = np.nan
        
        return df_cleaned


class DataTransformer:
    """
    Handles data transformations including type conversions and calculated fields.
    """
    
    def __init__(self):
        """Initialize the data transformer."""
        self.logger = logging.getLogger(__name__)
    
    def convert_data_types(self, 
                          df: pd.DataFrame, 
                          type_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Convert data types of specified columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            type_mapping (Dict[str, str]): Mapping of column names to target data types
        
        Returns:
            pd.DataFrame: DataFrame with converted data types
        """
        df_transformed = df.copy()
        
        for column, target_type in type_mapping.items():
            if column in df_transformed.columns:
                try:
                    self.logger.info(f"Converting {column} to {target_type}")
                    
                    if target_type == 'datetime':
                        df_transformed[column] = pd.to_datetime(df_transformed[column], errors='coerce')
                    elif target_type == 'category':
                        df_transformed[column] = df_transformed[column].astype('category')
                    else:
                        # Use proper type conversion with type: ignore for mypy
                        if target_type in ['int64', 'float64', 'bool', 'object', 'string']:
                            df_transformed[column] = df_transformed[column].astype(target_type)  # type: ignore
                        else:
                            # Try to convert with numpy dtype
                            import numpy as np
                            np_dtype = getattr(np, target_type, None)
                            if np_dtype:
                                df_transformed[column] = df_transformed[column].astype(np_dtype)  # type: ignore
                            else:
                                df_transformed[column] = df_transformed[column].astype(target_type)  # type: ignore
                        
                except Exception as e:
                    self.logger.error(f"Error converting {column} to {target_type}: {e}")
        
        return df_transformed
    
    def add_calculated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add calculated fields based on existing data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            pd.DataFrame: DataFrame with additional calculated fields
        """
        df_transformed = df.copy()
        
        try:
            # Calculate days since registration
            if 'registration_date' in df_transformed.columns:
                df_transformed['registration_date'] = pd.to_datetime(df_transformed['registration_date'])
                df_transformed['days_since_registration'] = (
                    pd.Timestamp.now() - df_transformed['registration_date']
                ).dt.days.astype(int)  # type: ignore[assignment]
            
            # Calculate days since last purchase
            if 'last_purchase_date' in df_transformed.columns:
                df_transformed['last_purchase_date'] = pd.to_datetime(df_transformed['last_purchase_date'])
                df_transformed['days_since_last_purchase'] = (
                    pd.Timestamp.now() - df_transformed['last_purchase_date']
                ).dt.days.astype(int)  # type: ignore[assignment]
            
            # Calculate customer lifetime value tier
            if 'total_spent' in df_transformed.columns:
                df_transformed['total_spent'] = pd.to_numeric(df_transformed['total_spent'], errors='coerce')
                df_transformed['ltv_tier'] = pd.cut(
                    df_transformed['total_spent'],
                    bins=[0, 500, 1000, 2000, float('inf')],
                    labels=['Low', 'Medium', 'High', 'Premium'],
                    include_lowest=True
                )
            
            # Create full name field
            if 'first_name' in df_transformed.columns and 'last_name' in df_transformed.columns:
                df_transformed['full_name'] = (
                    df_transformed['first_name'].astype(str) + ' ' + 
                    df_transformed['last_name'].astype(str)
                )
                # Replace 'nan nan' with NaN
                df_transformed['full_name'] = df_transformed['full_name'].replace('nan nan', np.nan)
            
            # Add data processing timestamp
            df_transformed['processed_at'] = datetime.now()
            
            self.logger.info("Added calculated fields successfully")
            
        except Exception as e:
            self.logger.error(f"Error adding calculated fields: {e}")
        
        return df_transformed
    
    def validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate business rules and flag invalid records.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            pd.DataFrame: DataFrame with validation flags
        """
        df_validated = df.copy()
        
        # Initialize validation flag
        df_validated['is_valid'] = True
        
        # Rule 1: Registration date should not be in the future
        if 'registration_date' in df_validated.columns:
            future_registration = df_validated['registration_date'] > datetime.now()
            df_validated.loc[future_registration, 'is_valid'] = False
            self.logger.info(f"Found {future_registration.sum()} records with future registration dates")
        
        # Rule 2: Last purchase date should not be before registration date
        if 'registration_date' in df_validated.columns and 'last_purchase_date' in df_validated.columns:
            invalid_purchase_date = (
                df_validated['last_purchase_date'] < df_validated['registration_date']
            )
            df_validated.loc[invalid_purchase_date, 'is_valid'] = False
            self.logger.info(f"Found {invalid_purchase_date.sum()} records with invalid purchase dates")
        
        # Rule 3: Total spent should be non-negative
        if 'total_spent' in df_validated.columns:
            negative_spent = df_validated['total_spent'] < 0
            df_validated.loc[negative_spent, 'is_valid'] = False
            self.logger.info(f"Found {negative_spent.sum()} records with negative total spent")
        
        valid_count = df_validated['is_valid'].sum()
        total_count = len(df_validated)
        self.logger.info(f"Validation complete: {valid_count}/{total_count} records are valid")
        
        return df_validated


def clean_and_transform_data(df: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Complete data cleaning and transformation pipeline.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        config (Dict[str, Any]): Configuration for cleaning and transformation
    
    Returns:
        pd.DataFrame: Cleaned and transformed DataFrame
    """
    if config is None:
        config = {}
    
    cleaner = DataCleaner()
    transformer = DataTransformer()
    
    # Step 1: Handle missing values
    missing_value_strategy = config.get('missing_value_strategy', {
        'phone': 'Unknown',
        'last_purchase_date': 'drop'
    })
    df = cleaner.handle_missing_values(df, missing_value_strategy)
    
    # Step 2: Remove duplicates
    df = cleaner.remove_duplicates(df, subset=['customer_id'])
    
    # Step 3: Standardize text
    text_columns = config.get('text_columns', ['first_name', 'last_name', 'city', 'state', 'country'])
    df = cleaner.standardize_text(df, text_columns, ['strip', 'title'])
    
    # Step 4: Validate emails
    if 'email' in df.columns:
        df = cleaner.validate_email(df, 'email')
    
    # Step 5: Validate phone numbers
    if 'phone' in df.columns:
        df = cleaner.validate_phone(df, 'phone')
    
    # Step 6: Convert data types
    type_mapping = config.get('type_mapping', {
        'customer_id': 'int64',
        'registration_date': 'datetime',
        'last_purchase_date': 'datetime',
        'total_spent': 'float64',
        'customer_segment': 'category'
    })
    df = transformer.convert_data_types(df, type_mapping)
    
    # Step 7: Add calculated fields
    df = transformer.add_calculated_fields(df)
    
    # Step 8: Validate business rules
    df = transformer.validate_business_rules(df)
    
    return df


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # This would typically be called with data from the extractor
    print("Data cleaning and transformation module ready for use")