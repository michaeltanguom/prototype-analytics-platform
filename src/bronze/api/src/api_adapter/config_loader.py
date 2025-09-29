"""
ConfigLoader module for loading and validating TOML configuration files
"""

import os
import tomllib
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, List

class ConfigurationError(Exception):
    """Raised when configuration is invalid or incomplete"""
    pass


class EnvironmentError(Exception):
    """Raised when required environment variables are missing"""
    pass


@dataclass
class APIConfig:
    """Configuration data class for API adapter from TOML file"""
    name: str
    base_url: str
    authentication: Dict[str, Any]
    pagination: Dict[str, Any]
    rate_limits: Dict[str, Any]
    retries: Dict[str, Any]
    default_parameters: Dict[str, Any] = field(default_factory=dict)
    cache: Dict[str, Any] = field(default_factory=dict)
    response_mapping: Dict[str, Any] = field(default_factory=dict)
    logging: Dict[str, Any] = field(default_factory=dict)
    payload_validation: Dict[str, Any] = field(default_factory=dict)


class ConfigLoader:
    """Loads and validates TOML configuration files"""
    
    # Required configuration sections and their mandatory keys
    REQUIRED_SECTIONS = {
        'api': ['name', 'base_url'],
        'authentication': ['type'],
        'pagination': ['strategy'],
        'rate_limits': ['requests_per_second'],
        'retries': ['max_attempts']
    }
    
    # Optional sections that can have default empty values
    OPTIONAL_SECTIONS = [
        'default_parameters',
        'cache', 
        'response_mapping',
        'logging',
        'payload_validation'
    ]
    
    @staticmethod
    def load_toml_config(config_path: Path) -> APIConfig:
        """
        Load API configuration from TOML file
        
        Args:
            config_path: Path to the TOML configuration file
            
        Returns:
            APIConfig object with all configuration data
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ConfigurationError: If required configuration is missing
            Exception: If TOML syntax is invalid
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'rb') as f:
                config_data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise Exception(f"Invalid TOML syntax in {config_path}: {e}")
        
        # Validate required sections and keys
        ConfigLoader._validate_required_sections(config_data)
        
        # Build APIConfig object
        return APIConfig(
            name=config_data['api']['name'],
            base_url=config_data['api']['base_url'],
            authentication=config_data['authentication'],
            pagination=config_data['pagination'],
            rate_limits=config_data['rate_limits'],
            retries=config_data['retries'],
            default_parameters=config_data.get('default_parameters', {}),
            cache=config_data.get('cache', {}),
            response_mapping=config_data.get('response_mapping', {}),
            logging=config_data.get('logging', {}),
            payload_validation=config_data.get('payload_validation', {})
        )
    
    @staticmethod
    def _validate_required_sections(config_data: Dict[str, Any]) -> None:
        """
        Validate that all required configuration sections and keys are present
        
        Args:
            config_data: Parsed TOML configuration data
            
        Raises:
            ConfigurationError: If any required section or key is missing
        """
        missing_items = []
        
        for section_name, required_keys in ConfigLoader.REQUIRED_SECTIONS.items():
            if section_name not in config_data:
                missing_items.append(f"Section [{section_name}]")
            else:
                section_data = config_data[section_name]
                for key in required_keys:
                    if key not in section_data:
                        missing_items.append(f"Key '{key}' in section [{section_name}]")
        
        if missing_items:
            raise ConfigurationError(
                f"Missing required configuration items: {', '.join(missing_items)}"
            )
    
    @staticmethod
    def validate_environment_variables(config: APIConfig) -> bool:
        """
        Validate that all required environment variables are set
        
        Args:
            config: APIConfig object to validate
            
        Returns:
            True if all environment variables are present
            
        Raises:
            EnvironmentError: If any required environment variables are missing
        """
        missing_vars = []
        
        # Check authentication environment variables
        auth_config = config.authentication
        for key, value in auth_config.items():
            if key.endswith('_env') and isinstance(value, str):
                # This is an environment variable reference
                env_var_name = value
                if not os.getenv(env_var_name):
                    missing_vars.append(env_var_name)
        
        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
        
        return True
    
    @staticmethod
    def get_environment_value(env_var_name: str) -> str:
        """
        Get environment variable value with proper error handling
        
        Args:
            env_var_name: Name of the environment variable
            
        Returns:
            Value of the environment variable
            
        Raises:
            EnvironmentError: If environment variable is not set
        """
        value = os.getenv(env_var_name)
        if value is None:
            raise EnvironmentError(f"Environment variable '{env_var_name}' is not set")
        return value