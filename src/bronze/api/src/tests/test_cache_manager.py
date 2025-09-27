"""
Test suite for CacheManager component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from api_adapter.cache_manager import CacheManager
from api_adapter.http_client import APIRequest, APIResponse
from datetime import datetime


class TestCacheManager:
    """Test suite for CacheManager response caching functionality"""
    
    def test_get_cached_response_with_nonexistent_key_returns_none(self):
        """
        Test that cache miss returns None for non-existent keys
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Act
            result = cache_manager.get_cached_response("nonexistent_key")
            
            # Assert
            assert result is None
    
    def test_store_response_with_valid_data_creates_cache_file(self):
        """
        Test that storing response creates cache file with correct data
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            api_response = APIResponse(
                raw_data={'data': 'test_response'},
                metadata={'url': 'https://api.test.com'},
                status_code=200,
                headers={'Content-Type': 'application/json'}
            )
            
            cache_key = "test_key_123"
            
            # Act
            cache_manager.store_response(cache_key, api_response)
            
            # Assert
            cache_file = cache_dir / f"{cache_key}.cache"
            assert cache_file.exists()
    
    def test_get_cached_response_with_existing_valid_cache_returns_response(self):
        """
        Test that cache hit returns stored APIResponse for valid cache
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Store a response first
            original_response = APIResponse(
                raw_data={'data': 'cached_response'},
                metadata={'url': 'https://api.test.com'},
                status_code=200,
                headers={'X-Custom': 'header_value'}
            )
            
            cache_key = "test_cache_key"
            cache_manager.store_response(cache_key, original_response)
            
            # Act
            result = cache_manager.get_cached_response(cache_key)
            
            # Assert
            assert result is not None
            assert isinstance(result, APIResponse)
            assert result.raw_data == {'data': 'cached_response'}
            assert result.status_code == 200
            assert result.headers['X-Custom'] == 'header_value'
    
    def test_get_cached_response_with_expired_cache_returns_none(self):
        """
        Test that expired cache entries return None
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=1)  # 1 second expiration
            
            # Store a response
            api_response = APIResponse(
                raw_data={'data': 'test'},
                metadata={},
                status_code=200
            )
            
            cache_key = "expiring_key"
            cache_manager.store_response(cache_key, api_response)
            
            # Wait for expiration
            time.sleep(1.1)
            
            # Act
            result = cache_manager.get_cached_response(cache_key)
            
            # Assert
            assert result is None
    
    def test_generate_cache_key_with_identical_requests_returns_same_key(self):
        """
        Test that identical API requests generate the same cache key
        """
        # Arrange
        cache_manager = CacheManager(Path("/tmp"), expiration_seconds=3600)
        
        request1 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param1': 'value1', 'param2': 'value2'},
            headers={'Accept': 'application/json'},
            method='GET'
        )
        
        request2 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param1': 'value1', 'param2': 'value2'},
            headers={'Accept': 'application/json'},
            method='GET'
        )
        
        # Act
        key1 = cache_manager.generate_cache_key(request1)
        key2 = cache_manager.generate_cache_key(request2)
        
        # Assert
        assert key1 == key2
        assert len(key1) == 32  # MD5 hash length
    
    def test_generate_cache_key_with_different_requests_returns_different_keys(self):
        """
        Test that different API requests generate different cache keys
        """
        # Arrange
        cache_manager = CacheManager(Path("/tmp"), expiration_seconds=3600)
        
        request1 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param1': 'value1'},
            method='GET'
        )
        
        request2 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param1': 'value2'},  # Different parameter value
            method='GET'
        )
        
        # Act
        key1 = cache_manager.generate_cache_key(request1)
        key2 = cache_manager.generate_cache_key(request2)
        
        # Assert
        assert key1 != key2
    
    def test_generate_cache_key_with_reordered_parameters_returns_same_key(self):
        """
        Test that parameter order doesn't affect cache key generation
        """
        # Arrange
        cache_manager = CacheManager(Path("/tmp"), expiration_seconds=3600)
        
        request1 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param1': 'value1', 'param2': 'value2'},
            method='GET'
        )
        
        request2 = APIRequest(
            url="https://api.test.com/endpoint",
            parameters={'param2': 'value2', 'param1': 'value1'},  # Reordered
            method='GET'
        )
        
        # Act
        key1 = cache_manager.generate_cache_key(request1)
        key2 = cache_manager.generate_cache_key(request2)
        
        # Assert
        assert key1 == key2
    
    def test_clear_cache_removes_all_cache_files(self):
        """
        Test that clear_cache removes all cached files from directory
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Store multiple responses
            for i in range(3):
                api_response = APIResponse(
                    raw_data={'data': f'response_{i}'},
                    metadata={},
                    status_code=200
                )
                cache_manager.store_response(f"key_{i}", api_response)
            
            # Verify files exist
            cache_files = list(cache_dir.glob("*.cache"))
            assert len(cache_files) == 3
            
            # Act
            cache_manager.clear_cache()
            
            # Assert
            cache_files_after = list(cache_dir.glob("*.cache"))
            assert len(cache_files_after) == 0
    
    def test_acquire_lock_with_no_existing_lock_creates_lock_and_returns_true(self):
        """
        Test that acquiring lock when none exists creates lock file and returns True
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Act
            result = cache_manager.acquire_lock()
            
            # Assert
            assert result is True
            lock_file = cache_dir / "cache.lock"
            assert lock_file.exists()
            
            # Cleanup
            cache_manager.release_lock()
    
    def test_acquire_lock_with_existing_lock_returns_false(self):
        """
        Test that acquiring lock when one exists returns False
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager1 = CacheManager(cache_dir, expiration_seconds=3600)
            cache_manager2 = CacheManager(cache_dir, expiration_seconds=3600)
            
            # First manager acquires lock
            cache_manager1.acquire_lock()
            
            # Act
            result = cache_manager2.acquire_lock()
            
            # Assert
            assert result is False
            
            # Cleanup
            cache_manager1.release_lock()
    
    def test_acquire_lock_with_stale_lock_removes_stale_and_creates_new(self):
        """
        Test that stale locks (older than timeout) are removed and new lock created
        """
        # Arrange
        import os
        
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600, lock_timeout=1)
            
            # Create a stale lock file manually
            lock_file = cache_dir / "cache.lock"
            lock_file.write_text("stale_lock")
            
            # Make it old by modifying the file time using os.utime
            old_time = time.time() - 2  # 2 seconds ago (older than 1 second timeout)
            os.utime(lock_file, (old_time, old_time))
            
            # Act
            result = cache_manager.acquire_lock()
            
            # Assert
            assert result is True
            assert lock_file.exists()
            
            # Cleanup
            cache_manager.release_lock()
    
    def test_release_lock_removes_lock_file(self):
        """
        Test that releasing lock removes the lock file
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Acquire lock first
            cache_manager.acquire_lock()
            lock_file = cache_dir / "cache.lock"
            assert lock_file.exists()
            
            # Act
            cache_manager.release_lock()
            
            # Assert
            assert not lock_file.exists()
    
    def test_release_lock_with_no_lock_handles_gracefully(self):
        """
        Test that releasing non-existent lock doesn't raise error
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600)
            
            # Act & Assert - should not raise any exceptions
            cache_manager.release_lock()
    
    def test_store_response_with_cache_disabled_does_nothing(self):
        """
        Test that storing response when caching is disabled does nothing
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600, enabled=False)
            
            api_response = APIResponse(
                raw_data={'data': 'test'},
                metadata={},
                status_code=200
            )
            
            # Act
            cache_manager.store_response("test_key", api_response)
            
            # Assert
            cache_files = list(cache_dir.glob("*.cache"))
            assert len(cache_files) == 0
    
    def test_get_cached_response_with_cache_disabled_returns_none(self):
        """
        Test that getting cached response when caching is disabled returns None
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_manager = CacheManager(cache_dir, expiration_seconds=3600, enabled=False)
            
            # Act
            result = cache_manager.get_cached_response("any_key")
            
            # Assert
            assert result is None