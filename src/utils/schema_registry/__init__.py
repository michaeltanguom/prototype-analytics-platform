"""
Schema Registry Module for Data Pipeline Validation
Location: src/utilities/schema_registry/__init__.py

This module provides schema detection, versioning, and compatibility checking
across the medallion architecture data layer.
"""

from .core import SchemaRegistry
from .detector import SchemaDetector
from .comparator import SchemaComparator
from .rules_engine import CompatibilityRulesEngine
from .database import SchemaRegistryDB
from .config import ConfigManager, SchemaRegistryConfig, NotificationConfig

__version__ = "1.0.0"
__all__ = [
    "SchemaRegistry",
    "SchemaDetector", 
    "SchemaComparator",
    "CompatibilityRulesEngine",
    "SchemaRegistryDB",
    "ConfigManager",
    "SchemaRegistryConfig",
    "NotificationConfig"
]