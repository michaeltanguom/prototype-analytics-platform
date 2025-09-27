"""
Configuration management for schema registry
Location: src/utilities/schema_registry/config.py
"""

import yaml
import tomllib
import tomllib
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass, field


@dataclass
class NotificationConfig:
    """Notification configuration for different compatibility levels"""
    method: str  # "log", "email", "both"
    level: str   # "info", "warning", "error"
    email: bool = False
    halt_pipeline: bool = False

@dataclass
class SchemaRegistryConfig:
    """Main configuration for schema registry"""
    database_path: str
    reports_directory: str
    notification_configs: Dict[str, NotificationConfig]
    version_increment_rules: Dict[str, str]
    enable_auto_versioning: bool = True
    metadata_filtering: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)
    

class ConfigManager:
    def __init__(self, config_path: str = None):
        self.config_path = config_path or "config/schema_registry_config.yml"
        self.config = self._load_config()
    
    def _load_config(self) -> SchemaRegistryConfig:
        """Load configuration from YAML or TOML file"""
        config_file = Path(self.config_path)
        
        if config_file.exists():
            if config_file.suffix.lower() == '.toml':
                return self._load_toml_config(config_file)
            else:
                return self._load_yaml_config(config_file) 
        else:
            return self._create_default_config()
    
    def _load_toml_config(self, config_file: Path) -> SchemaRegistryConfig:
        """Load TOML configuration"""
        with open(config_file, 'rb') as f:
            config_data = tomllib.load(f)
        return self._parse_config(config_data)
    
    def _load_yaml_config(self, config_file: Path) -> SchemaRegistryConfig:
        """Load YAML configuration"""
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
            return self._parse_config(config_data)
    
    def _create_default_config(self) -> SchemaRegistryConfig:
        """Create default configuration"""
        notification_configs = {
            "safe": NotificationConfig(
                method="log",
                level="info",
                email=False,
                halt_pipeline=False
            ),
            "warning": NotificationConfig(
                method="both",
                level="warning", 
                email=True,
                halt_pipeline=False
            ),
            "breaking": NotificationConfig(
                method="both",
                level="error",
                email=True,
                halt_pipeline=True
            )
        }
        
        version_increment_rules = {
            "safe": "patch",      # 1.0.0 -> 1.0.1
            "warning": "minor",   # 1.0.0 -> 1.1.0
            "breaking": "major"   # 1.0.0 -> 2.0.0
        }
        
        return SchemaRegistryConfig(
            database_path="src/utilities/schema_registry/schema_registry.db",
            reports_directory="src/utilities/schema_registry/schema_reports",
            notification_configs=notification_configs,
            enable_auto_versioning=True,
            version_increment_rules=version_increment_rules
        )
    
    def _parse_config(self, config_data: Dict[str, Any]) -> SchemaRegistryConfig:
        """Parse configuration from dictionary"""
        notification_configs = {}
        
        # Notification parsing
        for level, config in config_data.get("notifications", {}).items():
            notification_configs[level] = NotificationConfig(
                method=config.get("method", "log"),
                level=config.get("level", "info"),
                email=config.get("email", False),
                halt_pipeline=config.get("halt_pipeline", False)
            )

        # Parse metadata filtering patterns
        metadata_filtering = {}
        schema_registry_config = config_data.get("schema_registry", {})
        filtering_config = schema_registry_config.get("metadata_filtering", {})
        
        for data_source, filter_config in filtering_config.items():
            metadata_filtering[data_source] = {
                'patterns': filter_config.get('patterns', [])
            }
        
        return SchemaRegistryConfig(
            database_path=config_data.get("database_path", "src/utilities/schema_registry/schema_registry.db"),
            reports_directory=config_data.get("reports_directory", "src/utilities/schema_registry/schema_reports"),
            notification_configs=notification_configs,
            enable_auto_versioning=config_data.get("enable_auto_versioning", True),
            version_increment_rules=config_data.get("version_increment_rules", {
                "safe": "patch",
                "warning": "minor", 
                "breaking": "major"
            })
        )
    
    def save_config(self) -> None:
        """Save current configuration to YAML file"""
        config_file = Path(self.config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        
        config_dict = {
            "database_path": self.config.database_path,
            "reports_directory": self.config.reports_directory,
            "enable_auto_versioning": self.config.enable_auto_versioning,
            "version_increment_rules": self.config.version_increment_rules,
            "notifications": {}
        }
        
        for level, notif_config in self.config.notification_configs.items():
            config_dict["notifications"][level] = {
                "method": notif_config.method,
                "level": notif_config.level,
                "email": notif_config.email,
                "halt_pipeline": notif_config.halt_pipeline
            }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
    
    def get_notification_config(self, compatibility_level: str) -> NotificationConfig:
        """Get notification configuration for a specific compatibility level"""
        return self.config.notification_configs.get(
            compatibility_level, 
            self.config.notification_configs.get("safe")
        )
    
    def get_metadata_filtering_patterns(self, data_source: str) -> List[str]:
        """Get metadata filtering patterns for a specific data source"""
        if not self.config.metadata_filtering:
            return []
        
        filter_config = self.config.metadata_filtering.get(data_source, {})
        return filter_config.get('patterns', [])