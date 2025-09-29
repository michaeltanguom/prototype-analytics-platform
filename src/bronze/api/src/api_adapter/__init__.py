"""
Generic REST API Adapter package for bibliometric data sources
Provides modular, configurable components for API data ingestion with state management
"""

from .config_loader import ConfigLoader, ConfigurationError, EnvironmentError
from .database_manager import DatabaseManager, DatabaseConnectionError
from .state_manager import StateManager, ProcessingState
from .http_client import HTTPClient, PermanentAPIError, APIRequest, APIResponse
from .payload_validator import PayloadValidator
from .pagination_strategy import PaginationFactory
from .manifest_generator import ManifestGenerator
from .recovery_manager import RecoveryManager
from .rate_limit_tracker import RateLimitTracker
from .cache_manager import CacheManager
from .file_hasher import FileHasher

__all__ = [
    'ConfigLoader',
    'ConfigurationError', 
    'EnvironmentError',
    'DatabaseManager',
    'DatabaseConnectionError',
    'StateManager',
    'ProcessingState',
    'HTTPClient',
    'PermanentAPIError',
    'APIRequest',
    'APIResponse',
    'PayloadValidator',
    'PaginationFactory',
    'ManifestGenerator',
    'RecoveryManager',
    'RateLimitTracker',
    'CacheManager',
    'FileHasher'
]