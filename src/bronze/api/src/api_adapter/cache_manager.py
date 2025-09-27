"""
CacheManager module for handling response caching during development
"""

import json
import hashlib
import time
from pathlib import Path
from typing import Optional, Dict, Any
from api_adapter.http_client import APIRequest, APIResponse
from datetime import datetime


class CacheManager:
    """Manages HTTP response caching for development mode"""
    
    def __init__(self, cache_dir: Path, expiration_seconds: int = 3600, 
                 lock_timeout: int = 30, enabled: bool = True):
        self.cache_dir = cache_dir
        self.expiration_seconds = expiration_seconds
        self.lock_timeout = lock_timeout
        self.enabled = enabled
        self.lock_file = cache_dir / "cache.lock"
        
        # Ensure cache directory exists
        cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_cached_response(self, cache_key: str) -> Optional[APIResponse]:
        """
        Retrieve cached response if it exists and is not expired
        
        Args:
            cache_key: Unique key for the cached response
            
        Returns:
            APIResponse if cached and valid, None otherwise
        """
        if not self.enabled:
            return None
        
        cache_file = self.cache_dir / f"{cache_key}.cache"
        
        if not cache_file.exists():
            return None
        
        try:
            # Check if cache is expired
            file_age = time.time() - cache_file.stat().st_mtime
            if file_age > self.expiration_seconds:
                # Cache expired, remove file
                cache_file.unlink()
                return None
            
            # Load cached response
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
            
            # Reconstruct APIResponse object
            return APIResponse(
                raw_data=cached_data['raw_data'],
                metadata=cached_data['metadata'],
                status_code=cached_data['status_code'],
                headers=cached_data['headers'],
                request_timestamp=datetime.fromisoformat(cached_data['request_timestamp'])
            )
            
        except (json.JSONDecodeError, KeyError, OSError):
            # Corrupted cache file, remove it
            try:
                cache_file.unlink()
            except OSError:
                pass
            return None
    
    def store_response(self, cache_key: str, response: APIResponse) -> None:
        """
        Store API response in cache
        
        Args:
            cache_key: Unique key for caching the response
            response: APIResponse object to cache
        """
        if not self.enabled:
            return
        
        cache_file = self.cache_dir / f"{cache_key}.cache"
        
        try:
            # Serialise response data
            cache_data = {
                'raw_data': response.raw_data,
                'metadata': response.metadata,
                'status_code': response.status_code,
                'headers': response.headers,
                'request_timestamp': response.request_timestamp.isoformat(),
                'cached_at': datetime.now().isoformat()
            }
            
            # Write to cache file
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
        except (OSError, TypeError) as e:
            # Failed to write cache, continue without caching
            pass
    
    def generate_cache_key(self, request: APIRequest) -> str:
        """
        Generate a unique cache key based on request parameters
        
        Args:
            request: APIRequest object
            
        Returns:
            MD5 hash string to use as cache key
        """
        # Create a deterministic string representation of the request
        cache_data = {
            'url': request.url,
            'method': request.method,
            'parameters': dict(sorted(request.parameters.items())),
            'headers': dict(sorted(request.headers.items()))
        }
        
        # Convert to JSON string with sorted keys for consistency
        cache_string = json.dumps(cache_data, sort_keys=True)
        
        # Generate MD5 hash
        return hashlib.md5(cache_string.encode('utf-8')).hexdigest()
    
    def clear_cache(self) -> None:
        """
        Remove all cached files from the cache directory
        """
        try:
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink()
        except OSError:
            pass
    
    def acquire_lock(self) -> bool:
        """
        Acquire cache lock to prevent concurrent access
        
        Returns:
            True if lock acquired successfully, False otherwise
        """
        if self.lock_file.exists():
            # Check if lock is stale
            try:
                lock_age = time.time() - self.lock_file.stat().st_mtime
                if lock_age > self.lock_timeout:
                    # Stale lock, remove it
                    self.lock_file.unlink()
                else:
                    # Active lock exists
                    return False
            except OSError:
                # Error checking lock file, assume it's stale
                try:
                    self.lock_file.unlink()
                except OSError:
                    return False
        
        # Try to create lock file
        try:
            with open(self.lock_file, 'w') as f:
                f.write(str(time.time()))
            return True
        except OSError:
            return False
    
    def release_lock(self) -> None:
        """
        Release cache lock by removing lock file
        """
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
        except OSError:
            pass