"""
Test suite for FileHasher component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import hashlib
import json
from api_adapter.file_hasher import FileHasher

class TestFileHasher:
    """Test suite for FileHasher content deduplication functionality"""
    
    def test_generate_content_hash_with_identical_data_returns_same_hash(self):
        """
        Test that identical dictionaries produce the same hash value
        """
        # Arrange
        data1 = {"key": "value", "number": 123, "nested": {"inner": "data"}}
        data2 = {"key": "value", "number": 123, "nested": {"inner": "data"}}
        
        # Act
        hash1 = FileHasher.generate_content_hash(data1)
        hash2 = FileHasher.generate_content_hash(data2)
        
        # Assert
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hash length
        assert isinstance(hash1, str)
    
    def test_generate_content_hash_with_different_data_returns_different_hash(self):
        """
        Test that different dictionaries produce different hash values
        """
        # Arrange
        data1 = {"key": "value1", "number": 123}
        data2 = {"key": "value2", "number": 123}
        
        # Act
        hash1 = FileHasher.generate_content_hash(data1)
        hash2 = FileHasher.generate_content_hash(data2)
        
        # Assert
        assert hash1 != hash2
        assert len(hash1) == 32
        assert len(hash2) == 32
    
    def test_generate_content_hash_with_reordered_keys_returns_same_hash(self):
        """
        Test that dictionaries with same data but different key order produce same hash
        """
        # Arrange
        data1 = {"b": 2, "a": 1, "c": {"nested": 3, "other": 4}}
        data2 = {"a": 1, "b": 2, "c": {"other": 4, "nested": 3}}
        
        # Act
        hash1 = FileHasher.generate_content_hash(data1)
        hash2 = FileHasher.generate_content_hash(data2)
        
        # Assert
        assert hash1 == hash2
    
    def test_generate_content_hash_with_empty_dictionary_returns_valid_hash(self):
        """
        Test that empty dictionary produces a valid hash
        """
        # Arrange
        empty_data = {}
        
        # Act
        result_hash = FileHasher.generate_content_hash(empty_data)
        
        # Assert
        assert isinstance(result_hash, str)
        assert len(result_hash) == 32
        # Verify it's a valid MD5 hash format (hexadecimal)
        assert all(c in '0123456789abcdef' for c in result_hash)
    
    def test_generate_content_hash_with_none_values_handles_gracefully(self):
        """
        Test that dictionaries containing None values are handled properly
        """
        # Arrange
        data_with_none = {"key": None, "other": "value", "nested": {"inner": None}}
        
        # Act
        result_hash = FileHasher.generate_content_hash(data_with_none)
        
        # Assert
        assert isinstance(result_hash, str)
        assert len(result_hash) == 32
    
    def test_generate_content_hash_with_complex_nested_data_returns_consistent_hash(self):
        """
        Test that complex nested structures produce consistent hashes
        """
        # Arrange
        complex_data = {
            "api_response": {
                "search-results": {
                    "opensearch:totalResults": "150",
                    "entry": [
                        {"doi": "10.1000/test1", "title": "Test Paper 1"},
                        {"doi": "10.1000/test2", "title": "Test Paper 2"}
                    ]
                }
            },
            "metadata": {
                "timestamp": "2023-01-01T00:00:00Z",
                "source": "scopus"
            }
        }
        
        # Act
        hash1 = FileHasher.generate_content_hash(complex_data)
        hash2 = FileHasher.generate_content_hash(complex_data)
        
        # Assert
        assert hash1 == hash2
        assert len(hash1) == 32
    
    def test_generate_content_hash_with_list_ordering_affects_hash(self):
        """
        Test that list ordering affects hash values (lists are ordered)
        """
        # Arrange
        data1 = {"items": [1, 2, 3], "key": "value"}
        data2 = {"items": [3, 2, 1], "key": "value"}
        
        # Act
        hash1 = FileHasher.generate_content_hash(data1)
        hash2 = FileHasher.generate_content_hash(data2)
        
        # Assert
        assert hash1 != hash2  # Lists with different order should produce different hashes
    
    def test_compare_file_hashes_with_identical_hashes_returns_true(self):
        """
        Test that comparing identical hashes returns True
        """
        # Arrange
        hash_value = "abc123def456"
        
        # Act
        result = FileHasher.compare_file_hashes(hash_value, hash_value)
        
        # Assert
        assert result is True
    
    def test_compare_file_hashes_with_different_hashes_returns_false(self):
        """
        Test that comparing different hashes returns False
        """
        # Arrange
        hash1 = "abc123def456"
        hash2 = "xyz789uvw012"
        
        # Act
        result = FileHasher.compare_file_hashes(hash1, hash2)
        
        # Assert
        assert result is False
    
    def test_compare_file_hashes_with_none_values_returns_false(self):
        """
        Test that comparing None values returns False
        """
        # Arrange
        hash1 = "abc123def456"
        hash2 = None
        
        # Act
        result1 = FileHasher.compare_file_hashes(hash1, hash2)
        result2 = FileHasher.compare_file_hashes(None, hash1)
        result3 = FileHasher.compare_file_hashes(None, None)
        
        # Assert
        assert result1 is False
        assert result2 is False
        assert result3 is False
    
    def test_generate_content_hash_deterministic_across_sessions(self):
        """
        Test that the same data produces the same hash across different test runs
        """
        # Arrange
        test_data = {
            "scopus_response": {
                "total": 100,
                "entries": ["item1", "item2"]
            }
        }
        
        # Act
        hash1 = FileHasher.generate_content_hash(test_data)
        hash2 = FileHasher.generate_content_hash(test_data)
        
        # Assert
        assert hash1 == hash2
        # This hash should be deterministic - if algorithm changes, this test will catch it
        expected_hash = hashlib.md5(
            json.dumps(test_data, sort_keys=True).encode('utf-8')
        ).hexdigest()
        assert hash1 == expected_hash