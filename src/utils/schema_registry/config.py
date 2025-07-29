"""
Configuration management for schema registry
Location: src/utilities/schema_registry/config.py
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass


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
    version_increment_rules: Dict[str, str]  # compatibility_level -> version_type
    enable_auto_versioning: bool = True
    

class ConfigManager:
    """Manages schema registry configuration"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or "config/schema_registry_config.yml"
        self.config = self._load_config()
    
    def _load_config(self) -> SchemaRegistryConfig:
        """Load configuration from YAML file or create defaults"""
        config_file = Path(self.config_path)
        
        if config_file.exists():
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            return self._parse_config(config_data)
        else:
            return self._create_default_config()
    
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
        
        for level, config in config_data.get("notifications", {}).items():
            notification_configs[level] = NotificationConfig(
                method=config.get("method", "log"),
                level=config.get("level", "info"),
                email=config.get("email", False),
                halt_pipeline=config.get("halt_pipeline", False)
            )
        
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