"""
RateLimitTracker module for tracking API usage across sessions
"""

import json
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, timedelta


class RateLimitTracker:
    """Tracks API request usage across sessions for weekly limit monitoring"""
    
    def __init__(self, tracking_file: Path, weekly_limits: Optional[Dict[str, int]] = None):
        self.tracking_file = tracking_file
        self.weekly_limits = weekly_limits or {}
        self._request_log = self._load_request_log()
    
    def track_request(self, data_source: str, timestamp: datetime) -> None:
        """
        Record an API request for tracking against weekly limits
        
        Args:
            data_source: Name of the API (e.g., 'scopus', 'scival')
            timestamp: When the request was made
        """
        # Initialize data source if not exists
        if data_source not in self._request_log:
            self._request_log[data_source] = []
        
        # Add request timestamp
        self._request_log[data_source].append(timestamp.isoformat())
        
        # Clean old entries before saving
        self._cleanup_old_entries(data_source)
        
        # Persist to file
        self._save_request_log()
    
    def check_weekly_limit(self, data_source: str) -> bool:
        """
        Check if the data source is within its weekly request limit
        
        Args:
            data_source: Name of the API to check
            
        Returns:
            True if within limit or no limit configured, False if at/over limit
        """
        # If no limit configured for this source, allow unlimited
        if data_source not in self.weekly_limits:
            return True
        
        weekly_limit = self.weekly_limits[data_source]
        current_usage = self.get_current_usage(data_source)
        
        return current_usage < weekly_limit
    
    def get_current_usage(self, data_source: str) -> int:
        """
        Get current weekly usage count for a data source
        
        Args:
            data_source: Name of the API
            
        Returns:
            Number of requests made in the past 7 days
        """
        if data_source not in self._request_log:
            return 0
        
        # Clean old entries first
        self._cleanup_old_entries(data_source)
        
        # Count remaining entries (all should be within 7 days)
        return len(self._request_log[data_source])
    
    def _load_request_log(self) -> Dict[str, list]:
        """
        Load request log from persistent storage
        
        Returns:
            Dictionary of data source to list of timestamp strings
        """
        if not self.tracking_file.exists():
            return {}
        
        try:
            with open(self.tracking_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            # Corrupted or unreadable file, start fresh
            return {}
    
    def _save_request_log(self) -> None:
        """
        Save request log to persistent storage
        """
        try:
            # Ensure parent directory exists
            self.tracking_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.tracking_file, 'w') as f:
                json.dump(self._request_log, f, indent=2)
        except OSError:
            # Failed to write, continue without persistence
            pass
    
    def _cleanup_old_entries(self, data_source: str) -> None:
        """
        Remove request entries older than 7 days for a data source
        
        Args:
            data_source: Name of the API to clean up
        """
        if data_source not in self._request_log:
            return
        
        # Calculate cutoff time (7 days ago)
        cutoff_time = datetime.now() - timedelta(days=7)
        
        # Filter entries to keep only those within the last 7 days
        filtered_entries = []
        for timestamp_str in self._request_log[data_source]:
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp >= cutoff_time:
                    filtered_entries.append(timestamp_str)
            except ValueError:
                # Invalid timestamp format, skip it
                continue
        
        self._request_log[data_source] = filtered_entries