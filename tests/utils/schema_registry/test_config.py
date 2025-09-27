import pytest
import sys
import os

# Get the project root directory (4 levels up from this test file)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

from src.utils.schema_registry.config import ConfigManager

def test_load_toml_config_with_valid_file_returns_parsed_config():
    """
    Test that ConfigManager correctly loads and parses TOML configuration
    including metadata filtering patterns for schema registry.
    """
    # Arrange
    toml_content = """
    database_path = "test_schema_registry.db"
    reports_directory = "test_reports"
    enable_auto_versioning = true
    
    [schema_registry.metadata_filtering.scopus_search_api]
    patterns = ["@_fa", "@ref", "@href", "@type", "@role"]
    
    [schema_registry.metadata_filtering.scopus_author_api]  
    patterns = ["@_fa", "@ref", "@href"]
    
    [notifications.safe]
    method = "log"
    level = "info"
    email = false
    halt_pipeline = false
    """
    
    # Create temporary TOML file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write(toml_content)
        temp_toml_path = f.name
    
    try:
        # Act
        config_manager = ConfigManager(config_path=temp_toml_path)
        
        # Assert - Basic config loaded
        assert config_manager.config.database_path == "test_schema_registry.db"
        assert config_manager.config.enable_auto_versioning == True
        
        # Assert - Metadata filtering patterns loaded
        assert hasattr(config_manager.config, 'metadata_filtering')
        assert 'scopus_search_api' in config_manager.config.metadata_filtering
        assert '@_fa' in config_manager.config.metadata_filtering['scopus_search_api']['patterns']
        assert '@ref' in config_manager.config.metadata_filtering['scopus_search_api']['patterns']
        
        # Assert - Different API has different patterns
        assert 'scopus_author_api' in config_manager.config.metadata_filtering
        assert len(config_manager.config.metadata_filtering['scopus_author_api']['patterns']) == 3
        
    finally:
        # Cleanup
        import os
        os.unlink(temp_toml_path)

def test_load_yaml_config_with_existing_file_still_works():
    """
    Test that existing YAML configuration loading remains functional
    after TOML extension (backwards compatibility).
    """
    # Arrange
    yaml_content = """
    database_path: "existing_schema_registry.db"
    reports_directory: "existing_reports"
    enable_auto_versioning: true
    
    notifications:
      safe:
        method: "log"
        level: "info"
        email: false
        halt_pipeline: false
    """
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(yaml_content)
        temp_yaml_path = f.name
    
    try:
        # Act  
        config_manager = ConfigManager(config_path=temp_yaml_path)
        
        # Assert - YAML still loads correctly
        assert config_manager.config.database_path == "existing_schema_registry.db"
        assert config_manager.config.enable_auto_versioning == True
        
    finally:
        import os
        os.unlink(temp_yaml_path)

def test_get_metadata_filtering_patterns_with_known_data_source_returns_patterns():
    """
    Test that ConfigManager provides method to retrieve metadata filtering
    patterns for specific data sources.
    """
    # Arrange
    toml_content = """
    [schema_registry.metadata_filtering.scopus_search_api]
    patterns = ["@_fa", "@ref", "@href"]
    """
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write(toml_content)
        temp_toml_path = f.name
    
    try:
        config_manager = ConfigManager(config_path=temp_toml_path)
        
        # Act
        patterns = config_manager.get_metadata_filtering_patterns('scopus_search_api')
        
        # Assert
        assert '@_fa' in patterns
        assert '@ref' in patterns
        assert '@href' in patterns
        assert len(patterns) == 3
        
    finally:
        import os
        os.unlink(temp_toml_path)

def test_get_metadata_filtering_patterns_with_unknown_data_source_returns_empty_list():
    """
    Test that ConfigManager gracefully handles requests for unknown
    data sources by returning empty filtering patterns.
    """
    # Arrange
    config_manager = ConfigManager()  # Default config
    
    # Act
    patterns = config_manager.get_metadata_filtering_patterns('unknown_api')
    
    # Assert
    assert patterns == []