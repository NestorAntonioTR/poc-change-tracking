#!/usr/bin/env python3
"""
SQL Server to BigQuery Data Replication Script with Change Tracking

This script replicates data from SQL Server tables with change tracking enabled
to Google BigQuery. It supports both initial full loads and incremental updates
based on SQL Server's change tracking feature.

Requirements:
- SQL Server with Change Tracking enabled on the source database and tables
- Google Cloud project with BigQuery API enabled
- Service account with BigQuery Data Editor permissions
"""

import os
import sys
import logging
from dotenv import load_dotenv
from connector.replicator import Replicator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('replication.log')
    ]
)
logger = logging.getLogger('sql_to_bq_replicator')

# Load environment variables
load_dotenv()

# Configuration


GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET = os.getenv('BQ_DATASET')

SQL_SERVER = os.getenv('SQL_SERVER')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DRIVER = os.getenv('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}')

TABLES_TO_REPLICATE = os.getenv('TABLES_TO_REPLICATE', '').split(',')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Set log level from environment
logger.setLevel(getattr(logging, LOG_LEVEL))

def main():
    """Main entry point for the replication script."""
    logger.info("Starting SQL Server to BigQuery replication")
    
    # Validate configuration
    if not SQL_SERVER or not SQL_DATABASE:
        logger.error("SQL Server configuration is missing")
        return 1
    
    if not BQ_PROJECT_ID or not BQ_DATASET:
        logger.error("BigQuery configuration is missing")
        return 1
    
    if not TABLES_TO_REPLICATE:
        logger.error("No tables specified for replication")
        return 1
    
    # Initialize replicator
    replicator = Replicator()
    if not replicator.initialize():
        logger.error("Failed to initialize connections")
        return 1
    
    try:
        # Perform replication
        success = replicator.replicate_all_tables()
        
        if success:
            logger.info("Replication completed successfully")
            return 0
        else:
            logger.error("Replication completed with errors")
            return 1
    except Exception as e:
        logger.error(f"Replication failed with exception: {e}")
        return 1
    finally:
        replicator.close()


if __name__ == "__main__":
    import io  # Required for in-memory Parquet conversion
    sys.exit(main())
