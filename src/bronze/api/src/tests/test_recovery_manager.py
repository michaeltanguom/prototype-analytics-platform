"""
Test suite for RecoveryManager component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime
from api_adapter.recovery_manager import RecoveryManager
from api_adapter.state_manager import StateManager, ProcessingState
from api_adapter.file_hasher import FileHasher


class TestRecoveryManager:
    """Test suite for RecoveryManager partial failure restart functionality"""
    
    def test_identify_failed_pages_with_errors_in_state_returns_page_numbers(self):
        """
        Test that identify_failed_pages returns correct page numbers from error log
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        # Mock state with errors
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            errors=[
                {"timestamp": "2023-01-01T10:00:00", "page_number": 3, "error": "HTTP 503"},
                {"timestamp": "2023-01-01T10:01:00", "page_number": 7, "error": "Timeout"},
                {"timestamp": "2023-01-01T10:02:00", "page_number": 3, "error": "HTTP 500"}  # Duplicate page
            ]
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act
        result = recovery_manager.identify_failed_pages("author_123", "test_load")
        
        # Assert
        assert result == [3, 7]  # Unique page numbers from errors
        mock_state_manager.load_state.assert_called_once_with("author_123", "test_load")
    
    def test_identify_failed_pages_with_no_errors_returns_empty_list(self):
        """
        Test that identify_failed_pages returns empty list when no errors exist
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        # Mock state with no errors
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            errors=[]
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act
        result = recovery_manager.identify_failed_pages("author_123", "test_load")
        
        # Assert
        assert result == []
    
    def test_identify_failed_pages_with_no_state_returns_empty_list(self):
        """
        Test that identify_failed_pages handles missing state gracefully
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state_manager.load_state.return_value = None
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act
        result = recovery_manager.identify_failed_pages("nonexistent_entity", "test_load")
        
        # Assert
        assert result == []
    
    def test_validate_existing_files_with_all_valid_files_returns_true(self):
        """
        Test that validate_existing_files returns True when all files are valid
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        # Mock state with file names and hashes
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            file_names=["file1.json", "file2.json", "file3.json"],
            file_hashes={"file1.json": "hash1", "file2.json": "hash2", "file3.json": "hash3"}
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Mock file existence and validation
        with patch('pathlib.Path.exists', return_value=True), \
             patch.object(recovery_manager, '_validate_file_integrity', return_value=True):
            
            # Act
            result = recovery_manager.validate_existing_files("author_123", "test_load")
            
            # Assert
            assert result is True
    
    def test_validate_existing_files_with_missing_files_returns_false(self):
        """
        Test that validate_existing_files returns False when files are missing
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            file_names=["file1.json", "missing_file.json"],
            file_hashes={"file1.json": "hash1", "missing_file.json": "hash2"}
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Mock file existence - first exists, second doesn't
        def mock_exists(self):
            return "file1.json" in str(self)
        
        with patch('pathlib.Path.exists', side_effect=mock_exists):
            
            # Act
            result = recovery_manager.validate_existing_files("author_123", "test_load")
            
            # Assert
            assert result is False
    
    def test_validate_existing_files_with_corrupted_files_returns_false(self):
        """
        Test that validate_existing_files returns False when files are corrupted
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            file_names=["file1.json", "corrupted_file.json"],
            file_hashes={"file1.json": "hash1", "corrupted_file.json": "hash2"}
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Mock file validation - first is valid, second is corrupted
        def mock_validate_integrity(file_path):
            return "corrupted_file.json" not in str(file_path)
        
        with patch('pathlib.Path.exists', return_value=True), \
             patch.object(recovery_manager, '_validate_file_integrity', side_effect=mock_validate_integrity):
            
            # Act
            result = recovery_manager.validate_existing_files("author_123", "test_load")
            
            # Assert
            assert result is False
    
    def test_restart_from_page_updates_state_to_begin_at_specified_page(self):
        """
        Test that restart_from_page correctly updates processing state
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            last_page=5,
            pages_processed=5,
            completed=False
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act
        recovery_manager.restart_from_page("author_123", "test_load", 3)
        
        # Assert
        mock_state_manager.save_state.assert_called_once()
        saved_state = mock_state_manager.save_state.call_args[0][0]
        
        assert saved_state.last_page == 2  # One less than restart page
        assert saved_state.pages_processed == 2
        assert saved_state.completed is False
    
    def test_restart_from_page_clears_data_from_restart_page_onwards(self):
        """
        Test that restart_from_page removes data from the restart page and beyond
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state = ProcessingState(
            entity_id="author_123",
            load_id="test_load",
            file_names=["page1.json", "page2.json", "page3.json", "page4.json", "page5.json"],
            file_hashes={
                "page1.json": "hash1", "page2.json": "hash2", "page3.json": "hash3",
                "page4.json": "hash4", "page5.json": "hash5"
            },
            processed_items=["doi1", "doi2", "doi3", "doi4", "doi5"],
            errors=[
                {"page_number": 2, "error": "minor error"},
                {"page_number": 4, "error": "major error"}
            ]
        )
        
        mock_state_manager.load_state.return_value = mock_state
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act - restart from page 3
        recovery_manager.restart_from_page("author_123", "test_load", 3)
        
        # Assert
        saved_state = mock_state_manager.save_state.call_args[0][0]
        
        # Should keep only data from pages 1-2
        assert len(saved_state.file_names) == 2
        assert "page1.json" in saved_state.file_names
        assert "page2.json" in saved_state.file_names
        assert "page3.json" not in saved_state.file_names
        
        # Should keep only relevant file hashes
        assert len(saved_state.file_hashes) == 2
        assert "page1.json" in saved_state.file_hashes
        assert "page2.json" in saved_state.file_hashes
        
        # Should keep only errors from valid pages
        assert len(saved_state.errors) == 1
        assert saved_state.errors[0]["page_number"] == 2
    
    def test_restart_from_page_with_missing_state_handles_gracefully(self):
        """
        Test that restart_from_page handles missing state without errors
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        mock_state_manager.load_state.return_value = None
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        # Act & Assert - should not raise exceptions
        recovery_manager.restart_from_page("nonexistent_entity", "test_load", 3)
        
        # Should not attempt to save state
        mock_state_manager.save_state.assert_not_called()
    
    def test_validate_file_integrity_with_matching_hash_returns_true(self):
        """
        Test that _validate_file_integrity returns True when file hash matches
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        test_file_path = Path("/test/file.json")
        expected_hash = "correct_hash_123"
        
        # Mock file reading and hash generation
        mock_file_content = {"data": "test_content"}
        
        with patch('pathlib.Path.open', create=True) as mock_open, \
             patch('json.load', return_value=mock_file_content):
            
            mock_file_hasher.generate_content_hash.return_value = expected_hash
            
            # Act
            result = recovery_manager._validate_file_integrity(test_file_path, expected_hash)
            
            # Assert
            assert result is True
            mock_file_hasher.generate_content_hash.assert_called_once_with(mock_file_content)
    
    def test_validate_file_integrity_with_mismatched_hash_returns_false(self):
        """
        Test that _validate_file_integrity returns False when file hash doesn't match
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        test_file_path = Path("/test/file.json")
        expected_hash = "correct_hash_123"
        
        # Mock file reading and hash generation
        mock_file_content = {"data": "corrupted_content"}
        
        with patch('pathlib.Path.open', create=True) as mock_open, \
             patch('json.load', return_value=mock_file_content):
            
            mock_file_hasher.generate_content_hash.return_value = "different_hash_456"
            
            # Act
            result = recovery_manager._validate_file_integrity(test_file_path, expected_hash)
            
            # Assert
            assert result is False
    
    def test_validate_file_integrity_with_unreadable_file_returns_false(self):
        """
        Test that _validate_file_integrity returns False when file cannot be read
        """
        # Arrange
        mock_state_manager = Mock(spec=StateManager)
        mock_file_hasher = Mock(spec=FileHasher)
        
        recovery_manager = RecoveryManager(mock_state_manager, mock_file_hasher)
        
        test_file_path = Path("/test/unreadable_file.json")
        expected_hash = "some_hash"
        
        # Mock file reading to raise exception
        with patch('pathlib.Path.open', side_effect=OSError("Permission denied")):
            
            # Act
            result = recovery_manager._validate_file_integrity(test_file_path, expected_hash)
            
            # Assert
            assert result is False