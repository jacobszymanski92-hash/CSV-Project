"""
Demo script showing how to use the ETL pipeline components.
This script demonstrates the pipeline without requiring BigQuery setup.
"""

import sys
import logging
from pathlib import Path

# Add src directory to path for imports
current_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
src_path = current_dir / 'src'
sys.path.insert(0, str(src_path))

from extractor import CSVExtractor
from transformer import clean_and_transform_data


def demo_extraction():
    """Demonstrate CSV data extraction."""
    print("=" * 60)
    print("DEMO: Data Extraction")
    print("=" * 60)
    
    # Initialize extractor
    extractor = CSVExtractor("data/sample_customers.csv")
    
    # Extract data
    df = extractor.extract_data(
        parse_dates=['registration_date', 'last_purchase_date']
    )
    
    if df is not None:
        # Get data information
        info = extractor.get_data_info(df)
        preview = extractor.preview_data(df, 3)
        
        print(f"SUCCESS: Successfully extracted {info['total_rows']} rows and {info['total_columns']} columns")
        print(f"SUCCESS: Columns: {', '.join(info['columns'])}")
        print(f"SUCCESS: Memory usage: {info['memory_usage']:,} bytes")
        print(f"SUCCESS: Duplicate rows: {info['duplicate_rows']}")
        
        print("\nNull counts by column:")
        for col, nulls in info['null_counts'].items():
            if nulls > 0:
                print(f"  {col}: {nulls}")
        
        print("\nSample data (first 3 rows):")
        for i, row in enumerate(preview['head'][:3]):
            print(f"  Row {i+1}: Customer ID {row.get('customer_id', 'N/A')}, "
                  f"Name: {row.get('first_name', '')} {row.get('last_name', '')}")
        
        return df
    else:
        print("ERROR: Data extraction failed")
        return None


def demo_transformation(df):
    """Demonstrate data cleaning and transformation."""
    print("\n" + "=" * 60)
    print("DEMO: Data Transformation")
    print("=" * 60)
    
    if df is None:
        print("ERROR: No data to transform")
        return None
    
    initial_rows = len(df)
    print(f"Initial data: {initial_rows} rows")
    
    # Configure transformation
    config = {
        'missing_value_strategy': {
            'phone': 'Unknown',
            'last_purchase_date': 'drop'
        },
        'text_columns': ['first_name', 'last_name', 'city', 'state', 'country'],
        'type_mapping': {
            'customer_id': 'int64',
            'registration_date': 'datetime',
            'last_purchase_date': 'datetime',
            'total_spent': 'float64',
            'customer_segment': 'category'
        }
    }
    
    # Transform data
    df_transformed = clean_and_transform_data(df, config)
    
    final_rows = len(df_transformed)
    valid_rows = df_transformed['is_valid'].sum() if 'is_valid' in df_transformed.columns else final_rows
    
    print(f"SUCCESS: Transformation completed")
    print(f"SUCCESS: Rows: {initial_rows} -> {final_rows} (removed: {initial_rows - final_rows})")
    print(f"SUCCESS: Valid rows: {valid_rows}/{final_rows} ({valid_rows/final_rows*100:.1f}%)")
    
    # Show new calculated fields
    new_columns = [col for col in df_transformed.columns if col not in df.columns]
    if new_columns:
        print(f"SUCCESS: Added calculated fields: {', '.join(new_columns)}")
    
    # Show data quality improvements
    print("\nData quality improvements:")
    if 'email' in df_transformed.columns:
        valid_emails = df_transformed['email'].notna().sum()
        print(f"  Valid emails: {valid_emails}/{len(df_transformed)}")
    
    if 'phone' in df_transformed.columns:
        standardized_phones = df_transformed['phone'].str.contains(r'^\d{3}-\d{3}-\d{4}$', na=False).sum()
        print(f"  Standardized phone numbers: {standardized_phones}")
    
    # Show sample transformed data
    print("\nSample transformed data:")
    sample_data = df_transformed.head(3)
    for i, (_, row) in enumerate(sample_data.iterrows()):
        print(f"  Row {i+1}: {row.get('full_name', 'N/A')}, "
              f"LTV Tier: {row.get('ltv_tier', 'N/A')}, "
              f"Days since reg: {row.get('days_since_registration', 'N/A')}")
    
    return df_transformed


def demo_bigquery_schema(df):
    """Demonstrate BigQuery schema generation."""
    print("\n" + "=" * 60)
    print("DEMO: BigQuery Schema Generation")
    print("=" * 60)
    
    if df is None:
        print("ERROR: No data for schema generation")
        return
    
    # Import here to avoid requiring BigQuery for basic demo
    try:
        from bigquery_loader import BigQueryLoader
        
        loader = BigQueryLoader("demo-project", "demo-dataset")
        schema = loader.create_table_schema(df)
        
        print("SUCCESS: Generated BigQuery schema:")
        print(f"SUCCESS: Total fields: {len(schema)}")
        
        print("\nSchema fields:")
        for field in schema:
            nullable = "NULLABLE" if field.mode == "NULLABLE" else "REQUIRED"
            print(f"  {field.name}: {field.field_type} ({nullable})")
        
        print(f"\nNote: This schema would be used to create a BigQuery table.")
        print(f"To actually load data, update config/etl_config.json with your GCP settings.")
        
    except ImportError as e:
        print(f"ERROR: BigQuery client not available: {e}")


def main():
    """Run the complete demo."""
    # Setup logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("CSV to BigQuery ETL Pipeline Demo")
    print("This demo shows the pipeline capabilities without requiring BigQuery setup.\n")
    
    # Demo extraction
    df_raw = demo_extraction()
    
    # Demo transformation
    df_transformed = demo_transformation(df_raw)
    
    # Demo BigQuery schema generation
    demo_bigquery_schema(df_transformed)
    
    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print("To run the full pipeline with BigQuery:")
    print("1. Set up Google Cloud Project and BigQuery")
    print("2. Update config/etl_config.json with your settings")
    print("3. Run: python src/etl_pipeline.py")
    print("\nFor more information, see README.md")


if __name__ == "__main__":
    main()