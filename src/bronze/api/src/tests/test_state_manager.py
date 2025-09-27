"""
Test suite for StateManager component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from api_adapter.state_manager import StateManager, ProcessingState
from api_adapter.database_manager import DatabaseManager


class TestStateManager:
    """Test suite for StateManager processing state functionality"""
    
    def test_load_state_with_existing_record_returns_processing_state(self):
        """
        Test that loading existing state returns properly populated ProcessingState object
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        
        # Mock database response
        mock_result = Mock()
        mock_result.fetchone.return_value = (
            'test_load_123',  # load_id
            'author_456',     # entity_id
            'scopus',         # data_source
            2,                # last_page
            50,               # last_start_index
            100,              # total_results
            2,                # pages_processed
            False,            # completed
            ['doi1', 'doi2'], # processed_items (JSON)
            ['file1.json', 'file2.json'],  # file_names (JSON)
            {'file1.json': 'hash1'},       # file_hashes (JSON)
            [],               # errors (JSON)
            '2023-01-01T00:00:00',  # created_timestamp
            '2023-01-01T01:00:00'   # last_updated
        )
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        result = state_manager.load_state('author_456', 'test_load_123')
        
        # Assert
        assert isinstance(result, ProcessingState)
        assert result.entity_id == 'author_456'
        assert result.load_id == 'test_load_123'
        assert result.last_page == 2
        assert result.total_results == 100
        assert result.completed is False
        assert result.processed_items == ['doi1', 'doi2']
        
        # Verify database was queried correctly
        mock_db_manager._connection.execute.assert_called_once()
        call_args = mock_db_manager._connection.execute.call_args
        assert 'SELECT * FROM processing_state' in call_args[0][0]
        assert call_args[0][1] == ('test_load_123', 'author_456')
    
    def test_load_state_with_nonexistent_record_returns_none(self):
        """
        Test that loading non-existent state returns None
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        
        # Mock database response - no record found
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        result = state_manager.load_state('nonexistent_entity', 'nonexistent_load')
        
        # Assert
        assert result is None
    
    def test_load_state_without_connection_raises_error(self):
        """
        Test that loading state without database connection raises error
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = None
        
        state_manager = StateManager(mock_db_manager)
        
        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            state_manager.load_state('entity_123', 'load_123')
        
        assert "No active database connection" in str(exc_info.value)
    
    def test_save_state_with_new_entity_inserts_record(self):
        """
        Test that saving state for new entity inserts new record
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager.execute_with_transaction = Mock()
        
        state_manager = StateManager(mock_db_manager)
        
        processing_state = ProcessingState(
            entity_id='author_456',
            load_id='test_load_123',
            data_source='scopus',
            last_page=1,
            total_results=50,
            processed_items=['doi1'],
            file_names=['file1.json'],
            completed=False
        )
        
        # Act
        state_manager.save_state(processing_state)
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        # Verify UPSERT query structure
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'INSERT INTO processing_state' in query
        assert 'ON CONFLICT' in query
        assert params[0] == 'test_load_123'  # load_id
        assert params[1] == 'author_456'     # entity_id
        assert params[2] == 'scopus'         # data_source
    
    def test_save_state_with_existing_entity_updates_record(self):
        """
        Test that saving state for existing entity updates the record
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager.execute_with_transaction = Mock()
        
        state_manager = StateManager(mock_db_manager)
        
        # Create state with updated values
        processing_state = ProcessingState(
            entity_id='author_456',
            load_id='test_load_123',
            data_source='scopus',
            last_page=3,
            total_results=75,
            pages_processed=3,
            completed=True,
            processed_items=['doi1', 'doi2', 'doi3'],
            file_names=['file1.json', 'file2.json', 'file3.json']
        )
        
        # Act
        state_manager.save_state(processing_state)
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        params = call_args[0][1]
        
        assert params[3] == 3    # last_page
        assert params[5] == 75   # total_results
        assert params[6] == 3    # pages_processed
        assert params[7] is True # completed
    
    def test_mark_completed_with_valid_entity_updates_completed_flag(self):
        """
        Test that mark_completed sets completed flag to True
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager.execute_with_transaction = Mock()
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        state_manager.mark_completed('author_456', 'test_load_123')
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'UPDATE processing_state' in query
        assert 'SET completed = TRUE' in query
        assert params == ('test_load_123', 'author_456')
    
    def test_get_incomplete_entities_with_all_complete_returns_empty_list(self):
        """
        Test that when all entities are complete, empty list is returned
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        
        # Mock database response - all entities are complete
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ('author_1',), ('author_2',), ('author_3',)
        ]  # All entities returned as completed
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        entity_ids = ['author_1', 'author_2', 'author_3']
        
        # Act
        result = state_manager.get_incomplete_entities(entity_ids, 'test_load_123')
        
        # Assert
        assert result == []
    
    def test_get_incomplete_entities_with_some_incomplete_returns_incomplete_list(self):
        """
        Test that incomplete entities are correctly identified and returned
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        
        # Mock database response - some entities are incomplete
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ('author_2',),  # Only author_2 is marked as complete
        ]
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        entity_ids = ['author_1', 'author_2', 'author_3']
        
        # Act
        result = state_manager.get_incomplete_entities(entity_ids, 'test_load_123')
        
        # Assert
        assert result == ['author_1', 'author_3']  # author_2 was complete, so excluded
        
        # Verify correct query was made
        mock_db_manager._connection.execute.assert_called_once()
        call_args = mock_db_manager._connection.execute.call_args
        query = call_args[0][0]
        assert 'WHERE load_id = ?' in query
        assert 'AND completed = TRUE' in query
    
    def test_get_incomplete_entities_with_no_existing_state_returns_all_entities(self):
        """
        Test that entities without state records are considered incomplete
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        
        # Mock database response - no entities have state records
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        entity_ids = ['author_1', 'author_2', 'author_3']
        
        # Act
        result = state_manager.get_incomplete_entities(entity_ids, 'test_load_123')
        
        # Assert
        assert result == entity_ids  # All entities returned as incomplete
    
    def test_update_page_progress_with_valid_data_updates_state(self):
        """
        Test that update_page_progress correctly updates page tracking
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager.execute_with_transaction = Mock()
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        state_manager.update_page_progress('author_456', 'test_load_123', 5)
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'UPDATE processing_state' in query
        assert 'SET last_page = ?' in query
        assert 'pages_processed = ?' in query  # Look for this anywhere in the query
        assert params[0] == 5  # page_num
        assert params[1] == 5  # pages_processed
    
    def test_add_processed_item_with_new_identifier_adds_to_list(self):
        """
        Test that add_processed_item adds identifier to processed items
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        mock_db_manager.execute_with_transaction = Mock()
        
        # Mock existing state with some processed items
        mock_result = Mock()
        mock_result.fetchone.return_value = (
            'test_load_123', 'author_456', 'scopus', 1, 25, 100, 1, False,
            ['existing_doi'],  # existing processed_items
            [], {}, [], '2023-01-01T00:00:00', '2023-01-01T00:00:00'
        )
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        state_manager.add_processed_item('author_456', 'test_load_123', 'new_doi')
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        query = call_args[0][0]
        assert 'UPDATE processing_state' in query
        assert 'SET processed_items = ?' in query
        
        # The updated processed_items should include both existing and new DOI
        updated_items = call_args[0][1][0]
        assert 'existing_doi' in updated_items
        assert 'new_doi' in updated_items
    
    def test_record_file_hash_with_valid_data_updates_hashes(self):
        """
        Test that record_file_hash correctly stores file hash mapping
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        mock_db_manager.execute_with_transaction = Mock()
        
        # Mock existing state
        mock_result = Mock()
        mock_result.fetchone.return_value = (
            'test_load_123', 'author_456', 'scopus', 1, 25, 100, 1, False,
            [], ['existing_file.json'], {'existing_file.json': 'existing_hash'}, 
            [], '2023-01-01T00:00:00', '2023-01-01T00:00:00'
        )
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        
        # Act
        state_manager.record_file_hash('author_456', 'test_load_123', 'new_file.json', 'new_hash_123')
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        query = call_args[0][0]
        assert 'UPDATE processing_state' in query
        assert 'SET file_hashes = ?' in query
        
        # The updated file_hashes should include both existing and new mapping
        updated_hashes = call_args[0][1][0]
        assert updated_hashes['existing_file.json'] == 'existing_hash'
        assert updated_hashes['new_file.json'] == 'new_hash_123'
    
    def test_log_processing_error_with_exception_records_error_details(self):
        """
        Test that log_processing_error correctly stores error information
        """
        # Arrange
        mock_db_manager = Mock(spec=DatabaseManager)
        mock_db_manager._connection = Mock()
        mock_db_manager.execute_with_transaction = Mock()
        
        # Mock existing state
        mock_result = Mock()
        mock_result.fetchone.return_value = (
            'test_load_123', 'author_456', 'scopus', 1, 25, 100, 1, False,
            [], [], {}, [], '2023-01-01T00:00:00', '2023-01-01T00:00:00'
        )
        mock_db_manager._connection.execute.return_value = mock_result
        
        state_manager = StateManager(mock_db_manager)
        test_exception = ValueError("Test error message")
        
        # Act
        state_manager.log_processing_error('author_456', 'test_load_123', test_exception, 3)
        
        # Assert
        mock_db_manager.execute_with_transaction.assert_called_once()
        call_args = mock_db_manager.execute_with_transaction.call_args
        
        query = call_args[0][0]
        assert 'UPDATE processing_state' in query
        assert 'SET errors = ?' in query
        
        # The updated errors should contain the error details
        updated_errors = call_args[0][1][0]
        assert len(updated_errors) == 1
        error_entry = updated_errors[0]
        assert error_entry['page_number'] == 3
        assert error_entry['error_message'] == "Test error message"
        assert error_entry['error_type'] == "ValueError"
        assert 'timestamp' in error_entry