"""
Test suite for DatabaseManager component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
import duckdb
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from api_adapter.database_manager import DatabaseManager, DatabaseConnectionError


class TestDatabaseManager:
    """Test suite for DatabaseManager DuckDB operations functionality"""
    
    def test_create_connection_with_valid_path_returns_working_connection(self):
        """
        Test that creating connection with valid database path returns functional connection
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            
            # Act
            connection = db_manager.create_connection(db_path)
            
            # Assert
            assert connection is not None
            assert isinstance(connection, duckdb.DuckDBPyConnection)
            # Test connection works
            result = connection.execute("SELECT 1").fetchone()
            assert result[0] == 1
            
            # Cleanup
            db_manager.close_connection()
    
    def test_create_connection_with_invalid_path_raises_database_connection_error(self):
        """
        Test that invalid database path raises DatabaseConnectionError
        """
        # Arrange
        invalid_path = Path("/invalid/path/that/does/not/exist/test.db")
        db_manager = DatabaseManager()
        
        # Act & Assert
        with pytest.raises(DatabaseConnectionError) as exc_info:
            db_manager.create_connection(invalid_path)
        
        assert "Failed to create database connection" in str(exc_info.value)
    
    def test_create_connection_twice_raises_error_when_connection_exists(self):
        """
        Test that attempting to create connection when one exists raises error
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)  # First connection
            
            # Act & Assert
            with pytest.raises(DatabaseConnectionError) as exc_info:
                db_manager.create_connection(db_path)  # Second connection attempt
            
            assert "Connection already exists" in str(exc_info.value)
            
            # Cleanup
            db_manager.close_connection()
    
    def test_close_connection_with_active_connection_closes_successfully(self):
        """
        Test that closing active connection releases resources properly
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            connection = db_manager.create_connection(db_path)
            
            # Act
            db_manager.close_connection()
            
            # Assert
            # Connection should be closed - attempting to use it should raise error
            with pytest.raises(Exception):  # DuckDB connection closed error
                connection.execute("SELECT 1")
    
    def test_close_connection_with_no_connection_handles_gracefully(self):
        """
        Test that closing connection when none exists doesn't raise error
        """
        # Arrange
        db_manager = DatabaseManager()
        
        # Act & Assert - should not raise any exceptions
        db_manager.close_connection()
    
    def test_create_tables_with_valid_connection_creates_required_tables(self):
        """
        Test that create_tables creates all required database tables
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            
            # Act
            db_manager.create_tables()
            
            # Assert - check that tables exist
            # Check api_responses table
            result = db_manager._connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'api_responses'"
            ).fetchone()
            assert result is not None
            
            # Check processing_manifests table
            result = db_manager._connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'processing_manifests'"
            ).fetchone()
            assert result is not None
            
            # Check processing_state table
            result = db_manager._connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'processing_state'"
            ).fetchone()
            assert result is not None
            
            # Cleanup
            db_manager.close_connection()
    
    def test_create_tables_without_connection_raises_database_connection_error(self):
        """
        Test that creating tables without connection raises error
        """
        # Arrange
        db_manager = DatabaseManager()
        
        # Act & Assert
        with pytest.raises(DatabaseConnectionError) as exc_info:
            db_manager.create_tables()
        
        assert "No active database connection" in str(exc_info.value)
    
    def test_execute_with_transaction_with_valid_query_commits_successfully(self):
        """
        Test that execute_with_transaction runs query and commits transaction
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            query = "INSERT INTO processing_state (load_id, entity_id, data_source) VALUES (?, ?, ?)"
            params = ("test_load", "entity_123", "scopus")
            
            # Act
            result = db_manager.execute_with_transaction(query, params)
            
            # Assert - verify data was inserted
            result = db_manager._connection.execute(
                "SELECT * FROM processing_state WHERE entity_id = ?", ("entity_123",)
            )
            row = result.fetchone()
            assert row is not None
            assert row[1] == "entity_123"  # entity_id column
            
            # Cleanup
            db_manager.close_connection()
    
    def test_execute_with_transaction_with_invalid_query_rolls_back(self):
        """
        Test that execute_with_transaction rolls back on query failure
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Insert valid data first
            db_manager.execute_with_transaction(
                "INSERT INTO processing_state (load_id, entity_id, data_source) VALUES (?, ?, ?)",
                ("test_load", "entity_123", "scopus")
            )
            
            invalid_query = "INSERT INTO nonexistent_table (col1, col2) VALUES (?, ?)"
            params = ("val1", "val2")
            
            # Act & Assert
            with pytest.raises(Exception):  # DuckDB error for non-existent table
                db_manager.execute_with_transaction(invalid_query, params)
            
            # Verify original data still exists (transaction was isolated)
            result = db_manager._connection.execute("SELECT COUNT(*) FROM processing_state")
            count = result.fetchone()[0]
            assert count == 1  # Original data preserved
            
            # Cleanup
            db_manager.close_connection()
    
    def test_execute_with_transaction_without_connection_raises_error(self):
        """
        Test that execute_with_transaction without connection raises error
        """
        # Arrange
        db_manager = DatabaseManager()
        query = "SELECT 1"
        
        # Act & Assert
        with pytest.raises(DatabaseConnectionError):
            db_manager.execute_with_transaction(query)
    
    def test_upsert_api_response_with_new_data_inserts_record(self):
        """
        Test that upsert_api_response inserts new record when it doesn't exist
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            response_data = {
                'load_id': 'test_load_123',
                'data_source': 'scopus',
                'entity_id': 'author_456',
                'page_number': 1,
                'request_timestamp': datetime.now(),
                'response_timestamp': datetime.now(),
                'raw_response': {'data': 'test'},
                'technical_metadata': {'version': '1.0'},
                'log_entry': {'level': 'INFO'},
                'file_hash': 'abc123'
            }
            
            # Act
            db_manager.upsert_api_response(response_data)
            
            # Assert
            result = db_manager._connection.execute(
                "SELECT COUNT(*) FROM api_responses WHERE entity_id = ?", 
                (response_data['entity_id'],)
            )
            count = result.fetchone()[0]
            assert count == 1
            
            # Cleanup
            db_manager.close_connection()
    
    def test_upsert_api_response_with_existing_data_updates_record(self):
        """
        Test that upsert_api_response updates existing record when primary key matches
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Insert initial data
            initial_data = {
                'load_id': 'test_load_123',
                'data_source': 'scopus',
                'entity_id': 'author_456',
                'page_number': 1,
                'request_timestamp': datetime.now(),
                'response_timestamp': datetime.now(),
                'raw_response': {'data': 'original'},
                'technical_metadata': {'version': '1.0'},
                'log_entry': {'level': 'INFO'},
                'file_hash': 'original_hash'
            }
            db_manager.upsert_api_response(initial_data)
            
            # Update with new data (same primary key)
            updated_data = initial_data.copy()
            updated_data['raw_response'] = {'data': 'updated'}
            updated_data['file_hash'] = 'updated_hash'
            updated_data['response_timestamp'] = datetime.now()
            
            # Act
            db_manager.upsert_api_response(updated_data)
            
            # Assert - should still be only one record, but with updated data
            result = db_manager._connection.execute(
                "SELECT COUNT(*) FROM api_responses WHERE entity_id = ?", 
                (initial_data['entity_id'],)
            )
            count = result.fetchone()[0]
            assert count == 1
            
            result = db_manager._connection.execute(
                "SELECT file_hash FROM api_responses WHERE entity_id = ?", 
                (initial_data['entity_id'],)
            )
            file_hash = result.fetchone()[0]
            assert file_hash == 'updated_hash'
            
            # Cleanup
            db_manager.close_connection()
    
    def test_check_response_exists_with_existing_record_returns_true(self):
        """
        Test that check_response_exists returns True for existing records
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Insert test data
            response_data = {
                'load_id': 'test_load_123',
                'data_source': 'scopus',
                'entity_id': 'author_456',
                'page_number': 1,
                'request_timestamp': datetime.now(),
                'response_timestamp': datetime.now(),
                'raw_response': {'data': 'test'},
                'technical_metadata': {'version': '1.0'},
                'log_entry': {'level': 'INFO'},
                'file_hash': 'abc123'
            }
            db_manager.upsert_api_response(response_data)
            
            # Act
            result = db_manager.check_response_exists('test_load_123', 'author_456', 1)
            
            # Assert
            assert result is True
            
            # Cleanup
            db_manager.close_connection()
    
    def test_check_response_exists_with_nonexistent_record_returns_false(self):
        """
        Test that check_response_exists returns False for non-existent records
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Act
            result = db_manager.check_response_exists('nonexistent_load', 'nonexistent_entity', 1)
            
            # Assert
            assert result is False
            
            # Cleanup
            db_manager.close_connection()
    
    def test_insert_manifest_data_with_valid_data_inserts_successfully(self):
        """
        Test that insert_manifest_data inserts manifest data correctly
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            manifest_data = {
                'load_id': 'test_load_123',
                'data_source': 'scopus',
                'manifest_data': {'total_entities': 5},
                'processing_summary': {'success_rate': 100},
                'total_entities': 5,
                'total_responses': 25
            }
            
            # Act
            db_manager.insert_manifest_data(manifest_data)
            
            # Assert
            result = db_manager._connection.execute(
                "SELECT * FROM processing_manifests WHERE load_id = ?", 
                (manifest_data['load_id'],)
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == 'test_load_123'  # load_id
            assert row[5] == 5  # total_entities
            
            # Cleanup
            db_manager.close_connection()
    
    def test_verify_data_integrity_with_valid_data_returns_true(self):
        """
        Test that verify_data_integrity returns True for valid data
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Insert test data
            response_data = {
                'load_id': 'test_load_123',
                'data_source': 'scopus',
                'entity_id': 'author_456',
                'page_number': 1,
                'request_timestamp': datetime.now(),
                'response_timestamp': datetime.now(),
                'raw_response': {'data': 'test'},
                'technical_metadata': {'version': '1.0'},
                'log_entry': {'level': 'INFO'},
                'file_hash': 'abc123'
            }
            db_manager.upsert_api_response(response_data)
            
            # Act
            result = db_manager.verify_data_integrity('test_load_123', 'author_456')
            
            # Assert
            assert result is True
            
            # Cleanup
            db_manager.close_connection()
    
    def test_verify_data_integrity_with_missing_data_returns_false(self):
        """
        Test that verify_data_integrity returns False for missing/invalid data
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db_manager = DatabaseManager()
            db_manager.create_connection(db_path)
            db_manager.create_tables()
            
            # Act
            result = db_manager.verify_data_integrity('nonexistent_load', 'nonexistent_entity')
            
            # Assert
            assert result is False
            
            # Cleanup
            db_manager.close_connection()