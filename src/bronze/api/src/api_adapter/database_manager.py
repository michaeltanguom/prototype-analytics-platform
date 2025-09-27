"""
DatabaseManager module for handling DuckDB database operations and connections
"""

import duckdb
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime


class DatabaseConnectionError(Exception):
    """Raised when database connection operations fail"""
    pass


class DatabaseManager:
    """Manages DuckDB database connections and operations with transaction support"""
    
    def __init__(self):
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
    
    def create_connection(self, db_path: Path) -> duckdb.DuckDBPyConnection:
        """
        Create DuckDB database connection with proper error handling
        
        Args:
            db_path: Path to the database file
            
        Returns:
            DuckDB connection object
            
        Raises:
            DatabaseConnectionError: If connection fails or already exists
        """
        if self._connection is not None:
            raise DatabaseConnectionError("Connection already exists. Close existing connection first.")
        
        try:
            # Ensure parent directory exists
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create connection to DuckDB file
            self._connection = duckdb.connect(str(db_path))
            
            return self._connection
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create database connection: {e}")
    
    def close_connection(self) -> None:
        """
        Close database connection and release resources
        """
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass  # Connection might already be closed
            finally:
                self._connection = None
    
    def create_tables(self) -> None:
        """
        Create all required database tables with proper DuckDB schema
        
        Raises:
            DatabaseConnectionError: If no active connection exists
        """
        if not self._connection:
            raise DatabaseConnectionError("No active database connection")
        
        # Create api_responses table
        api_responses_sql = """
        CREATE TABLE IF NOT EXISTS api_responses (
            load_id VARCHAR NOT NULL,
            data_source VARCHAR NOT NULL,
            entity_id VARCHAR NOT NULL,
            page_number INTEGER NOT NULL,
            request_timestamp TIMESTAMP NOT NULL,
            response_timestamp TIMESTAMP NOT NULL,
            raw_response JSON NOT NULL,
            technical_metadata JSON NOT NULL,
            log_entry JSON NOT NULL,
            file_hash VARCHAR NOT NULL,
            PRIMARY KEY (load_id, entity_id, page_number)
        )
        """
        
        # Create processing_manifests table
        manifests_sql = """
        CREATE TABLE IF NOT EXISTS processing_manifests (
            load_id VARCHAR PRIMARY KEY,
            data_source VARCHAR NOT NULL,
            manifest_data JSON NOT NULL,
            processing_summary JSON NOT NULL,
            generated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_entities INTEGER NOT NULL,
            total_responses INTEGER NOT NULL
        )
        """
        
        # Create processing_state table
        state_sql = """
        CREATE TABLE IF NOT EXISTS processing_state (
            load_id VARCHAR NOT NULL,
            entity_id VARCHAR NOT NULL,
            data_source VARCHAR NOT NULL,
            last_page INTEGER DEFAULT 0,
            last_start_index INTEGER DEFAULT 0,
            total_results INTEGER,
            pages_processed INTEGER DEFAULT 0,
            completed BOOLEAN DEFAULT FALSE,
            processed_items JSON DEFAULT '[]',
            file_names JSON DEFAULT '[]',
            file_hashes JSON DEFAULT '{}',
            errors JSON DEFAULT '[]',
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (load_id, entity_id)
        )
        """
        
        try:
            # Create tables
            self._connection.execute(api_responses_sql)
            self._connection.execute(manifests_sql)
            self._connection.execute(state_sql)
            
            # Create indexes for performance
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_data_source_load_id ON api_responses (data_source, load_id)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_request_timestamp ON api_responses (request_timestamp)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_state_completed ON processing_state (completed)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_state_data_source ON processing_state (data_source)"
            )
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create tables: {e}")
    
    def execute_with_transaction(self, query: str, params: Tuple = ()) -> Any:
        """
        Execute query within a transaction with automatic rollback on failure
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query result
            
        Raises:
            DatabaseConnectionError: If no active connection
            Exception: If query execution fails
        """
        if not self._connection:
            raise DatabaseConnectionError("No active database connection")
        
        try:
            self._connection.begin()
            result = self._connection.execute(query, params)
            self._connection.commit()
            return result
            
        except Exception as e:
            self._connection.rollback()
            raise e
    
    def upsert_api_response(self, response_data: Dict[str, Any]) -> None:
        """
        Insert or update API response data using DuckDB INSERT ON CONFLICT
        
        Args:
            response_data: Dictionary containing response data
        """
        upsert_sql = """
        INSERT INTO api_responses (
            load_id, data_source, entity_id, page_number,
            request_timestamp, response_timestamp, raw_response,
            technical_metadata, log_entry, file_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (load_id, entity_id, page_number) DO UPDATE SET
            response_timestamp = EXCLUDED.response_timestamp,
            raw_response = EXCLUDED.raw_response,
            technical_metadata = EXCLUDED.technical_metadata,
            log_entry = EXCLUDED.log_entry,
            file_hash = EXCLUDED.file_hash
        """
        
        params = (
            response_data['load_id'],
            response_data['data_source'],
            response_data['entity_id'],
            response_data['page_number'],
            response_data['request_timestamp'],
            response_data['response_timestamp'],
            response_data['raw_response'],  # DuckDB handles JSON directly
            response_data['technical_metadata'],
            response_data['log_entry'],
            response_data['file_hash']
        )
        
        self.execute_with_transaction(upsert_sql, params)
    
    def insert_manifest_data(self, manifest_data: Dict[str, Any]) -> None:
        """
        Insert manifest data with INSERT OR REPLACE for idempotency
        
        Args:
            manifest_data: Dictionary containing manifest data
        """
        insert_sql = """
        INSERT OR REPLACE INTO processing_manifests (
            load_id, data_source, manifest_data, processing_summary,
            generated_timestamp, total_entities, total_responses
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            manifest_data['load_id'],
            manifest_data['data_source'],
            manifest_data['manifest_data'],  # DuckDB handles JSON directly
            manifest_data['processing_summary'],
            datetime.now(),
            manifest_data['total_entities'],
            manifest_data['total_responses']
        )
        
        self.execute_with_transaction(insert_sql, params)
    
    def check_response_exists(self, load_id: str, entity_id: str, page_num: int) -> bool:
        """
        Check if API response already exists in database
        
        Args:
            load_id: Load identifier
            entity_id: Entity identifier
            page_num: Page number
            
        Returns:
            True if response exists, False otherwise
        """
        if not self._connection:
            raise DatabaseConnectionError("No active database connection")
        
        query = """
        SELECT COUNT(*) FROM api_responses 
        WHERE load_id = ? AND entity_id = ? AND page_number = ?
        """
        
        result = self._connection.execute(query, (load_id, entity_id, page_num))
        count = result.fetchone()[0]
        
        return count > 0
    
    def verify_data_integrity(self, load_id: str, entity_id: str) -> bool:
        """
        Verify data integrity for a specific entity and load
        
        Args:
            load_id: Load identifier
            entity_id: Entity identifier
            
        Returns:
            True if data is valid, False otherwise
        """
        if not self._connection:
            raise DatabaseConnectionError("No active database connection")
        
        try:
            # Check if any records exist
            result = self._connection.execute(
                "SELECT COUNT(*) FROM api_responses WHERE load_id = ? AND entity_id = ?",
                (load_id, entity_id)
            )
            count = result.fetchone()[0]
            
            if count == 0:
                return False
            
            # Check for records with valid JSON data (DuckDB JSON validation)
            result = self._connection.execute(
                """SELECT COUNT(*) FROM api_responses 
                   WHERE load_id = ? AND entity_id = ? 
                   AND raw_response IS NOT NULL AND technical_metadata IS NOT NULL""",
                (load_id, entity_id)
            )
            valid_count = result.fetchone()[0]
            
            # All records should have valid data
            return valid_count == count
            
        except Exception:
            return False