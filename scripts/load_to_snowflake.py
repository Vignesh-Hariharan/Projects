#!/usr/bin/env python3
"""
Load marketing data to Snowflake.
Simple script to upload prepared data to Snowflake table.
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create Snowflake connection from environment variables."""
    
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'MARKETING_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DATA'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW')
    }
    
    # Validate required config
    required_fields = ['account', 'user', 'password']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        raise ValueError(f"Missing required Snowflake configuration: {missing_fields}")
    
    return snowflake.connector.connect(**config)

def load_marketing_data(data_file, table_name='MARKETING_DATA'):
    """Load marketing data from CSV to Snowflake table."""
    
    # Read data
    logger.info(f"Reading data from {data_file}")
    df = pd.read_csv(data_file)
    
    # Convert column names to uppercase for Snowflake
    df.columns = df.columns.str.upper()
    
    # Convert data types
    date_columns = ['CAMPAIGN_DATE', 'INGESTION_TIMESTAMP']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    logger.info(f"Prepared {len(df)} records for upload")
    
    # Connect to Snowflake
    logger.info("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    try:
        # Upload data
        logger.info(f"Loading data to {table_name}...")
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df, 
            table_name,
            auto_create_table=False,  # Table should already exist from Terraform
            overwrite=True  # Replace existing data
        )
        
        if success:
            logger.info(f"Successfully loaded {nrows} rows to {table_name}")
            return True
        else:
            logger.error("Failed to load data")
            return False
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return False
        
    finally:
        conn.close()

def validate_upload(table_name='MARKETING_DATA'):
    """Validate the uploaded data."""
    
    logger.info("Validating uploaded data...")
    conn = get_snowflake_connection()
    
    try:
        cursor = conn.cursor()
        
        # Check record count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"Total records in {table_name}: {count}")
        
        # Check data freshness
        cursor.execute(f"SELECT MAX(INGESTION_TIMESTAMP) FROM {table_name}")
        max_timestamp = cursor.fetchone()[0]
        logger.info(f"Latest data timestamp: {max_timestamp}")
        
        # Check for nulls in key columns
        key_columns = ['TV', 'RADIO', 'NEWSPAPER', 'SALES', 'CAMPAIGN_ID']
        for col in key_columns:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in {col}")
        
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False
        
    finally:
        conn.close()

def main():
    """Main execution function."""
    
    # Default data file
    data_file = Path("data/subset/marketing_performance.csv")
    
    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        logger.info("Run 'python scripts/prepare_data.py' first")
        return
    
    try:
        # Load data
        success = load_marketing_data(data_file)
        
        if success:
            # Validate upload
            validate_upload()
            logger.info("✅ Data loading completed successfully")
        else:
            logger.error("❌ Data loading failed")
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

