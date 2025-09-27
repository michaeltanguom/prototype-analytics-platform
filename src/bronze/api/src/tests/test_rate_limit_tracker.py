"""
Test suite for RateLimitTracker component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from api_adapter.rate_limit_tracker import RateLimitTracker


class TestRateLimitTracker:
    """Test suite for RateLimitTracker API usage tracking functionality"""
    
    def test_track_request_with_new_data_source_creates_log_entry(self):
        """
        Test that tracking request for new data source creates initial log entry
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            request_time = datetime.now()
            
            # Act
            tracker.track_request("scopus", request_time)
            
            # Assert
            assert tracking_file.exists()
            
            # Verify data was recorded
            current_usage = tracker.get_current_usage("scopus")
            assert current_usage == 1
    
    def test_track_request_with_existing_data_source_increments_count(self):
        """
        Test that tracking multiple requests accumulates the count
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            request_time = datetime.now()
            
            # Act - make multiple requests
            tracker.track_request("scopus", request_time)
            tracker.track_request("scopus", request_time + timedelta(seconds=30))
            tracker.track_request("scopus", request_time + timedelta(seconds=60))
            
            # Assert
            current_usage = tracker.get_current_usage("scopus")
            assert current_usage == 3
    
    def test_track_request_with_multiple_data_sources_tracks_separately(self):
        """
        Test that different data sources are tracked independently
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            request_time = datetime.now()
            
            # Act
            tracker.track_request("scopus", request_time)
            tracker.track_request("scopus", request_time + timedelta(seconds=30))
            tracker.track_request("scival", request_time + timedelta(seconds=60))
            tracker.track_request("wos", request_time + timedelta(seconds=90))
            
            # Assert
            assert tracker.get_current_usage("scopus") == 2
            assert tracker.get_current_usage("scival") == 1
            assert tracker.get_current_usage("wos") == 1
    
    def test_get_current_usage_with_no_requests_returns_zero(self):
        """
        Test that current usage returns zero for data sources with no tracked requests
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            # Act
            result = tracker.get_current_usage("nonexistent_source")
            
            # Assert
            assert result == 0
    
    def test_get_current_usage_excludes_requests_older_than_seven_days(self):
        """
        Test that current usage calculation excludes requests older than 7 days
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            now = datetime.now()
            old_request = now - timedelta(days=8)  # 8 days ago
            recent_request = now - timedelta(days=2)  # 2 days ago
            current_request = now
            
            # Act
            tracker.track_request("scopus", old_request)
            tracker.track_request("scopus", recent_request)
            tracker.track_request("scopus", current_request)
            
            # Assert
            current_usage = tracker.get_current_usage("scopus")
            assert current_usage == 2  # Only recent and current requests counted
    
    def test_check_weekly_limit_with_usage_below_limit_returns_true(self):
        """
        Test that weekly limit check returns True when current usage is below limit
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            weekly_limits = {"scopus": 1000}
            tracker = RateLimitTracker(tracking_file, weekly_limits)
            
            # Make some requests below the limit
            now = datetime.now()
            for i in range(10):
                tracker.track_request("scopus", now + timedelta(seconds=i))
            
            # Act
            result = tracker.check_weekly_limit("scopus")
            
            # Assert
            assert result is True
    
    def test_check_weekly_limit_with_usage_at_limit_returns_false(self):
        """
        Test that weekly limit check returns False when current usage equals or exceeds limit
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            weekly_limits = {"scopus": 5}  # Low limit for testing
            tracker = RateLimitTracker(tracking_file, weekly_limits)
            
            # Make requests equal to the limit
            now = datetime.now()
            for i in range(5):
                tracker.track_request("scopus", now + timedelta(seconds=i))
            
            # Act
            result = tracker.check_weekly_limit("scopus")
            
            # Assert
            assert result is False
    
    def test_check_weekly_limit_with_usage_exceeding_limit_returns_false(self):
        """
        Test that weekly limit check returns False when current usage exceeds limit
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            weekly_limits = {"scopus": 3}
            tracker = RateLimitTracker(tracking_file, weekly_limits)
            
            # Make requests exceeding the limit
            now = datetime.now()
            for i in range(5):  # 5 requests, limit is 3
                tracker.track_request("scopus", now + timedelta(seconds=i))
            
            # Act
            result = tracker.check_weekly_limit("scopus")
            
            # Assert
            assert result is False
    
    def test_check_weekly_limit_with_no_configured_limit_returns_true(self):
        """
        Test that weekly limit check returns True when no limit is configured for data source
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            weekly_limits = {}  # No limits configured
            tracker = RateLimitTracker(tracking_file, weekly_limits)
            
            # Make many requests
            now = datetime.now()
            for i in range(100):
                tracker.track_request("unlimited_source", now + timedelta(seconds=i))
            
            # Act
            result = tracker.check_weekly_limit("unlimited_source")
            
            # Assert
            assert result is True
    
    def test_track_request_with_file_persistence_survives_restart(self):
        """
        Test that tracked requests are persisted and survive tracker restart
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            
            # First tracker instance
            tracker1 = RateLimitTracker(tracking_file)
            request_time = datetime.now()
            tracker1.track_request("scopus", request_time)
            tracker1.track_request("scopus", request_time + timedelta(seconds=30))
            
            # Create new tracker instance (simulating restart)
            tracker2 = RateLimitTracker(tracking_file)
            
            # Act
            usage_after_restart = tracker2.get_current_usage("scopus")
            
            # Assert
            assert usage_after_restart == 2
    
    def test_track_request_with_corrupted_file_handles_gracefully(self):
        """
        Test that tracker handles corrupted tracking file gracefully
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            
            # Create corrupted file
            tracking_file.write_text("invalid json content {")
            
            tracker = RateLimitTracker(tracking_file)
            request_time = datetime.now()
            
            # Act & Assert - should not raise exceptions
            tracker.track_request("scopus", request_time)
            usage = tracker.get_current_usage("scopus")
            assert usage == 1
    
    def test_get_current_usage_filters_by_exact_seven_day_window(self):
        """
        Test that current usage calculation uses exactly 7 days (168 hours)
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            tracker = RateLimitTracker(tracking_file)
            
            now = datetime.now()
            # Use larger time differences to avoid precision issues
            eight_days_ago = now - timedelta(days=8)        # Should be excluded
            six_days_ago = now - timedelta(days=6)          # Should be included
            three_days_ago = now - timedelta(days=3)        # Should be included
            current_request = now                           # Should be included
            
            # Act
            tracker.track_request("scopus", eight_days_ago)   # Should be excluded
            tracker.track_request("scopus", six_days_ago)     # Should be included
            tracker.track_request("scopus", three_days_ago)   # Should be included
            tracker.track_request("scopus", current_request)  # Should be included
            
            # Assert
            current_usage = tracker.get_current_usage("scopus")
            assert current_usage == 3  # Excludes the 8-day-old request
    
    def test_check_weekly_limit_calculates_remaining_quota_correctly(self):
        """
        Test that weekly limit checking properly calculates remaining quota
        """
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            tracking_file = Path(temp_dir) / "rate_limits.json"
            weekly_limits = {"scopus": 100}
            tracker = RateLimitTracker(tracking_file, weekly_limits)
            
            # Use 75% of quota
            now = datetime.now()
            for i in range(75):
                tracker.track_request("scopus", now + timedelta(seconds=i))
            
            # Act
            can_proceed = tracker.check_weekly_limit("scopus")
            current_usage = tracker.get_current_usage("scopus")
            
            # Assert
            assert can_proceed is True  # Still under limit
            assert current_usage == 75
            
            # Test edge case - exactly at limit
            for i in range(25):  # Add remaining 25 requests
                tracker.track_request("scopus", now + timedelta(seconds=100+i))
            
            can_proceed_at_limit = tracker.check_weekly_limit("scopus")
            assert can_proceed_at_limit is False  # At limit