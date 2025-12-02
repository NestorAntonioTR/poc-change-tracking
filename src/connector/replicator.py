import os
import sys
import logging
from dotenv import load_dotenv
from connector.big_query_connector import BigQueryConnection
from connector.state_manager import StateManager
from connector.sql_server_connector import SQLServerConnection

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

TABLES_TO_REPLICATE = os.getenv('TABLES_TO_REPLICATE', '').split(',')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

class Replicator:
    """Main replication orchestrator."""
    
    def __init__(self):
        self.sql_conn = SQLServerConnection()
        self.bq_conn = BigQueryConnection()
        self.state_manager = StateManager()
    
    def initialize(self) -> bool:
        """Initialize connections."""
        sql_connected = self.sql_conn.connect()
        bq_connected = self.bq_conn.connect()
        
        return sql_connected and bq_connected
    
    def close(self):
        """Close connections."""
        self.sql_conn.close()
        self.bq_conn.close()
    
    def replicate_table(self, table_name: str) -> bool:
        """Replicate a single table from SQL Server to BigQuery."""
        logger.info(f"Starting replication for table: {table_name}")
        
        # Check if change tracking is enabled
        if not self.sql_conn.is_change_tracking_enabled(table_name):
            logger.error(f"Change tracking is not enabled for table {table_name}")
            return False
        
        # Get the table schema
        schema = self.sql_conn.get_table_schema(table_name)
        if not schema:
            logger.error(f"Failed to get schema for table {table_name}")
            return False
        
        # Get primary key columns and add to schema
        pk_columns = self.sql_conn.get_primary_key_columns(table_name)
        for col in schema:
            if col['name'] in pk_columns:
                col['is_primary_key'] = True
        
        # Create the table in BigQuery if it doesn't exist
        if not self.bq_conn.table_exists(table_name):
            logger.info(f"Table {table_name} does not exist in BigQuery, creating it")
            if not self.bq_conn.create_table(table_name, schema):
                return False
            
            # Perform initial full load
            logger.info(f"Performing initial full load for {table_name}")
            df = self.sql_conn.get_all_data(table_name)
            if df.empty:
                logger.warning(f"No data found for initial load of {table_name}")
            else:
                if not self.bq_conn.load_dataframe(df, table_name):
                    return False
            
            # Get current change tracking version
            current_version = self.sql_conn.get_change_tracking_current_version()
            self.state_manager.update_sync_version(table_name, current_version)
            
            logger.info(f"Initial load completed for {table_name}, current version: {current_version}")
            return True
        
        # Get the last sync version
        last_sync_version = self.state_manager.get_last_sync_version(table_name)
        logger.info(f"Last sync version for {table_name}: {last_sync_version}")
        
        # Get changed data
        changed_df, deleted_df, current_version = self.sql_conn.get_changed_data(
            table_name, last_sync_version
        )
        
        if current_version == 0:
            logger.error(f"Failed to get change tracking data for {table_name}")
            return False
        
        # Process changes
        if not changed_df.empty:
            logger.info(f"Loading {len(changed_df)} changed rows for {table_name}")
            if not self.bq_conn.load_dataframe(changed_df, table_name, write_disposition='WRITE_APPEND'):
                return False
        
        # Process deletes
        if not deleted_df.empty:
            logger.info(f"Deleting {len(deleted_df)} rows from {table_name}")
            if not self.bq_conn.delete_rows(table_name, deleted_df):
                return False
        
        # Update the sync version
        self.state_manager.update_sync_version(table_name, current_version)
        
        logger.info(f"Replication completed for {table_name}, new version: {current_version}")
        return True
    
    def replicate_all_tables(self) -> bool:
        """Replicate all tables specified in the configuration."""
        success = True
        
        for table_name in TABLES_TO_REPLICATE:
            table_name = table_name.strip()
            if not table_name:
                continue
                
            table_success = self.replicate_table(table_name)
            if not table_success:
                logger.error(f"Failed to replicate table {table_name}")
                success = False
        
        return success
