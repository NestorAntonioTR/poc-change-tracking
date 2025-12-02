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
from typing import Dict, List, Tuple, Any
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from tqdm import tqdm

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


SQL_SERVER = os.getenv('SQL_SERVER')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DRIVER = os.getenv('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}')

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))



class SQLServerConnection:
    """Manages connection to SQL Server and provides query methods."""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to SQL Server."""
        try:
            connection_string = (
                f'DRIVER={SQL_DRIVER};'
                f'SERVER={SQL_SERVER};'
                f'DATABASE={SQL_DATABASE};'
                f'UID={SQL_USERNAME};'
                f'PWD={SQL_PASSWORD}'
            )
            self.conn = pyodbc.connect(connection_string)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQL Server: {SQL_SERVER}, Database: {SQL_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return False
    
    def close(self):
        """Close the SQL Server connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("SQL Server connection closed")
    
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a SQL query and return results."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get the schema of a table."""
        columns = []
        try:
            schema_query = f"""
            SELECT 
                c.name as column_name,
                t.name as data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable
            FROM 
                sys.columns c
            INNER JOIN 
                sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN 
                sys.tables tbl ON c.object_id = tbl.object_id
            WHERE 
                tbl.name = ?
            ORDER BY 
                c.column_id
            """
            results = self.execute_query(schema_query, (table_name,))
            
            for row in results:
                column = {
                    'name': row.column_name,
                    'type': row.data_type,
                    'max_length': row.max_length,
                    'precision': row.precision,
                    'scale': row.scale,
                    'is_nullable': row.is_nullable
                }
                columns.append(column)
            
            return columns
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return []
    
    def is_change_tracking_enabled(self, table_name: str) -> bool:
        """Check if change tracking is enabled for the table."""
        query = """
        SELECT 
            t.name AS TableName,
            CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END AS IsChangeTrackingEnabled
        FROM 
            sys.tables t
        LEFT JOIN 
            sys.change_tracking_tables ct ON t.object_id = ct.object_id
        WHERE 
            t.name = ?
        """
        result = self.execute_query(query, (table_name,))
        
        if not result:
            logger.error(f"Table {table_name} not found")
            return False
        
        return bool(result[0].IsChangeTrackingEnabled)
    
    def get_change_tracking_current_version(self) -> int:
        """Get the current change tracking version."""
        query = "SELECT CHANGE_TRACKING_CURRENT_VERSION()"
        result = self.execute_query(query)
        
        if not result:
            logger.error("Failed to get current change tracking version")
            return 0
        
        return result[0][0]
    
    def get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get the primary key columns for a table."""
        query = """
        SELECT 
            c.name AS column_name
        FROM 
            sys.indexes i
        INNER JOIN 
            sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN 
            sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN 
            sys.tables t ON i.object_id = t.object_id
        WHERE 
            i.is_primary_key = 1 AND t.name = ?
        ORDER BY 
            ic.key_ordinal
        """
        results = self.execute_query(query, (table_name,))
        
        if not results:
            logger.error(f"No primary key found for table {table_name}")
            return []
        
        return [row.column_name for row in results]
    
    def get_all_data(self, table_name: str, batch_size: int = BATCH_SIZE) -> pd.DataFrame:
        """Get all data from a table in batches."""
        try:
            # Get total row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.execute_query(count_query)
            total_rows = count_result[0][0] if count_result else 0
            
            if total_rows == 0:
                logger.info(f"Table {table_name} is empty")
                return pd.DataFrame()
            
            # Get primary key for efficient batching
            pk_columns = self.get_primary_key_columns(table_name)
            if not pk_columns:
                logger.warning(f"No primary key found for {table_name}, using OFFSET/FETCH for batching")
                return self._get_all_data_with_offset(table_name, batch_size, total_rows)
            
            # Use primary key for batching
            return self._get_all_data_with_pk(table_name, pk_columns[0], batch_size, total_rows)
            
        except Exception as e:
            logger.error(f"Failed to get data from {table_name}: {e}")
            return pd.DataFrame()
    
    def _get_all_data_with_offset(self, table_name: str, batch_size: int, total_rows: int) -> pd.DataFrame:
        """Get all data using OFFSET/FETCH for batching."""
        all_data = []
        
        with tqdm(total=total_rows, desc=f"Fetching {table_name}") as pbar:
            for offset in range(0, total_rows, batch_size):
                query = f"""
                SELECT * FROM {table_name}
                ORDER BY (SELECT NULL)
                OFFSET {offset} ROWS
                FETCH NEXT {batch_size} ROWS ONLY
                """
                batch = self.execute_query(query)
                
                if not batch:
                    break
                
                # Convert to list of dictionaries
                columns = [column[0] for column in self.cursor.description]
                batch_dicts = [dict(zip(columns, row)) for row in batch]
                all_data.extend(batch_dicts)
                
                pbar.update(len(batch))
        
        return pd.DataFrame(all_data)
    
    def _get_all_data_with_pk(self, table_name: str, pk_column: str, batch_size: int, total_rows: int) -> pd.DataFrame:
        """Get all data using primary key for batching."""
        all_data = []
        last_pk_value = None
        
        with tqdm(total=total_rows, desc=f"Fetching {table_name}") as pbar:
            while True:
                if last_pk_value is None:
                    query = f"""
                    SELECT TOP {batch_size} * FROM {table_name}
                    ORDER BY {pk_column}
                    """
                    params = None
                else:
                    query = f"""
                    SELECT TOP {batch_size} * FROM {table_name}
                    WHERE {pk_column} > ?
                    ORDER BY {pk_column}
                    """
                    params = (last_pk_value,)
                
                batch = self.execute_query(query, params)
                
                if not batch:
                    break
                
                # Convert to list of dictionaries
                columns = [column[0] for column in self.cursor.description]
                batch_dicts = [dict(zip(columns, row)) for row in batch]
                all_data.extend(batch_dicts)
                
                # Update last PK value for next batch
                last_pk_value = batch[-1][columns.index(pk_column)]
                
                pbar.update(len(batch))
                
                if len(batch) < batch_size:
                    break
        
        return pd.DataFrame(all_data)
    
    def get_changed_data(self, table_name: str, last_sync_version: int) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
        """
        Get changed data since the last sync version.
        
        Returns:
            Tuple containing:
            - DataFrame of inserted/updated rows
            - DataFrame of deleted primary keys
            - Current change tracking version
        """
        if not self.is_change_tracking_enabled(table_name):
            logger.error(f"Change tracking is not enabled for table {table_name}")
            return pd.DataFrame(), pd.DataFrame(), 0
        
        current_version = self.get_change_tracking_current_version()
        
        if current_version <= last_sync_version:
            logger.info(f"No changes detected for {table_name} since version {last_sync_version}")
            return pd.DataFrame(), pd.DataFrame(), current_version
        
        # Get primary key columns
        pk_columns = self.get_primary_key_columns(table_name)
        if not pk_columns:
            logger.error(f"Cannot track changes for {table_name} without a primary key")
            return pd.DataFrame(), pd.DataFrame(), 0
        
        pk_columns_str = ', '.join(pk_columns)
        
        # Get changed rows (inserts and updates)
        changed_query = f"""
        SELECT t.*, CT.SYS_CHANGE_OPERATION
        FROM {table_name} t
        INNER JOIN CHANGETABLE(CHANGES {table_name}, {last_sync_version}) CT
        ON {' AND '.join([f't.{pk} = CT.{pk}' for pk in pk_columns])}
        WHERE CT.SYS_CHANGE_OPERATION IN ('I', 'U')
        """
        
        try:
            # Execute query and convert to DataFrame
            changed_rows = self.execute_query(changed_query)
            if changed_rows:
                columns = [column[0] for column in self.cursor.description]
                changed_dicts = [dict(zip(columns, row)) for row in changed_rows]
                changed_df = pd.DataFrame(changed_dicts)
            else:
                changed_df = pd.DataFrame()
            
            # Get deleted rows (only primary keys)
            deleted_query = f"""
            SELECT {pk_columns_str}
            FROM CHANGETABLE(CHANGES {table_name}, {last_sync_version}) CT
            WHERE CT.SYS_CHANGE_OPERATION = 'D'
            """
            
            deleted_rows = self.execute_query(deleted_query)
            if deleted_rows:
                deleted_columns = pk_columns
                deleted_dicts = [dict(zip(deleted_columns, row)) for row in deleted_rows]
                deleted_df = pd.DataFrame(deleted_dicts)
            else:
                deleted_df = pd.DataFrame()
            
            logger.info(f"Found {len(changed_df)} changed rows and {len(deleted_df)} deleted rows for {table_name}")
            
            return changed_df, deleted_df, current_version
            
        except Exception as e:
            logger.error(f"Failed to get changed data for {table_name}: {e}")
            return pd.DataFrame(), pd.DataFrame(), 0
