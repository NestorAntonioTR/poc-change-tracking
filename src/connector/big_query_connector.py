
import os
import sys
import logging
from typing import Dict, List, Any

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

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

TABLES_TO_REPLICATE = os.getenv('TABLES_TO_REPLICATE', '').split(',')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Set log level from environment
logger.setLevel(getattr(logging, LOG_LEVEL))

class BigQueryConnection:
    """Manages connection to BigQuery and provides data loading methods."""
    
    def __init__(self):
        self.client = None
        
    def connect(self):
        """Establish connection to BigQuery."""
        try:
            if GOOGLE_APPLICATION_CREDENTIALS:
                credentials = service_account.Credentials.from_service_account_file(
                    GOOGLE_APPLICATION_CREDENTIALS,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=BQ_PROJECT_ID
                )
            else:
                # Use default credentials
                self.client = bigquery.Client(project=BQ_PROJECT_ID)
                
            logger.info(f"Connected to BigQuery project: {BQ_PROJECT_ID}")
            
            # Ensure dataset exists
            self._ensure_dataset_exists()
            
            return True
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            return False
    
    def _ensure_dataset_exists(self):
        """Ensure the BigQuery dataset exists, create if it doesn't."""
        try:
            dataset_ref = self.client.dataset(BQ_DATASET)
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {BQ_DATASET} already exists")
            except Exception:
                # Dataset does not exist, create it
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Set the location
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {BQ_DATASET}")
        except Exception as e:
            logger.error(f"Failed to ensure dataset exists: {e}")
    
    def close(self):
        """Close the BigQuery connection."""
        if self.client:
            self.client.close()
        logger.info("BigQuery connection closed")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in BigQuery."""
        try:
            table_ref = self.client.dataset(BQ_DATASET).table(table_name)
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
    
    def create_table(self, table_name: str, schema_columns: List[Dict[str, Any]]) -> bool:
        """Create a table in BigQuery with the given schema."""
        try:
            # Convert SQL Server schema to BigQuery schema
            bq_schema = self._convert_schema(schema_columns)
            
            # Create the table
            table_ref = self.client.dataset(BQ_DATASET).table(table_name)
            table = bigquery.Table(table_ref, schema=bq_schema)
            
            # Add clustering fields if primary keys are available
            pk_columns = [col['name'] for col in schema_columns if col.get('is_primary_key')]
            if pk_columns:
                table.clustering_fields = pk_columns[:4]  # BigQuery supports up to 4 clustering fields
            
            self.client.create_table(table)
            logger.info(f"Created table {BQ_DATASET}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _convert_schema(self, sql_schema: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """Convert SQL Server schema to BigQuery schema."""
        bq_schema = []
        
        for column in sql_schema:
            field_name = column['name']
            sql_type = column['type'].lower()
            is_nullable = column['is_nullable']
            
            # Map SQL Server types to BigQuery types
            if sql_type in ('char', 'varchar', 'nvarchar', 'nchar', 'text', 'ntext'):
                bq_type = 'STRING'
            elif sql_type in ('int', 'smallint', 'tinyint'):
                bq_type = 'INTEGER'
            elif sql_type == 'bigint':
                bq_type = 'INTEGER'
            elif sql_type in ('decimal', 'numeric', 'money', 'smallmoney'):
                bq_type = 'NUMERIC'
            elif sql_type in ('float', 'real'):
                bq_type = 'FLOAT'
            elif sql_type in ('date', 'datetime', 'datetime2', 'smalldatetime'):
                bq_type = 'DATETIME'
            elif sql_type == 'time':
                bq_type = 'TIME'
            elif sql_type == 'bit':
                bq_type = 'BOOLEAN'
            elif sql_type in ('binary', 'varbinary', 'image'):
                bq_type = 'BYTES'
            elif sql_type == 'uniqueidentifier':
                bq_type = 'STRING'
            else:
                logger.warning(f"Unknown SQL type {sql_type} for column {field_name}, defaulting to STRING")
                bq_type = 'STRING'
            
            # Create the schema field
            field = bigquery.SchemaField(
                name=field_name,
                field_type=bq_type,
                mode='NULLABLE' if is_nullable else 'REQUIRED'
            )
            
            bq_schema.append(field)
        
        return bq_schema
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, write_disposition: str = 'WRITE_TRUNCATE') -> bool:
        """Load a DataFrame into a BigQuery table."""
        if df.empty:
            logger.info(f"No data to load for table {table_name}")
            return True
        
        try:
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.PARQUET,
            )
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = df.to_parquet()
            
            # Load the data
            table_ref = self.client.dataset(BQ_DATASET).table(table_name)
            load_job = self.client.load_table_from_file(
                io.BytesIO(parquet_buffer),
                table_ref,
                job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            logger.info(f"Loaded {len(df)} rows into {BQ_DATASET}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            return False
    
    def delete_rows(self, table_name: str, pk_df: pd.DataFrame) -> bool:
        """Delete rows from a BigQuery table based on primary key values."""
        if pk_df.empty:
            logger.info(f"No rows to delete from {table_name}")
            return True
        
        try:
            # Get the primary key column names
            pk_columns = pk_df.columns.tolist()
            
            # Construct the DELETE query
            delete_conditions = []
            for _, row in pk_df.iterrows():
                condition_parts = []
                for pk in pk_columns:
                    value = row[pk]
                    if isinstance(value, str):
                        condition_parts.append(f"{pk} = '{value}'")
                    elif pd.isna(value):
                        condition_parts.append(f"{pk} IS NULL")
                    else:
                        condition_parts.append(f"{pk} = {value}")
                
                delete_conditions.append(f"({' AND '.join(condition_parts)})")
            
            delete_query = f"""
            DELETE FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}`
            WHERE {' OR '.join(delete_conditions)}
            """
            
            # Execute the query
            query_job = self.client.query(delete_query)
            query_job.result()
            
            logger.info(f"Deleted {len(pk_df)} rows from {BQ_DATASET}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete rows from {table_name}: {e}")
            return False
