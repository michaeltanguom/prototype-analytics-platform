"""
Test suite for ConfigLoader component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, mock_open
from api_adapter.config_loader import ConfigLoader, APIConfig, ConfigurationError, EnvironmentError


class TestConfigLoader:
    """Test suite for ConfigLoader TOML configuration loading functionality"""
    
    def test_load_toml_config_with_valid_file_returns_api_config(self):
        """
        Test that loading a valid TOML file returns properly populated APIConfig
        """
        # Arrange
        valid_toml_content = """
        [api]
        name = "scopus"
        base_url = "https://api.elsevier.com/content/search/scopus"
        
        [authentication]
        type = "api_key_and_token"
        api_key_env = "SCOPUS_API_KEY"
        inst_token_env = "SCOPUS_INST_ID"
        
        [pagination]
        strategy = "offset_limit"
        items_per_page = 25
        start_param = "start"
        
        [rate_limits]
        requests_per_second = 2
        weekly_limit = 1000
        
        [retries]
        max_attempts = 3
        backoff_factor = 2
        
        [default_parameters]
        view = "COMPLETE"
        httpAccept = "application/json"
        
        [cache]
        enabled = true
        expiration_seconds = 86400
        
        [response_mapping]
        total_results_path = "search-results.opensearch:totalResults"
        items_path = "search-results.entry"
        
        [logging]
        log_file_name = "scopus_search_api.log"
        
        [payload_validation]
        response_completeness = true
        rate_limit_headers = true
        empty_response_detection = true
        json_structure_validation = true
        
        [payload_validation.headers]
        total_results_field = "search-results.opensearch:totalResults"
        rate_limit_remaining = "X-RateLimit-Remaining"
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(valid_toml_content)
            config_path = Path(f.name)
        
        try:
            # Act
            result = ConfigLoader.load_toml_config(config_path)
            
            # Assert
            assert isinstance(result, APIConfig)
            assert result.name == "scopus"
            assert result.base_url == "https://api.elsevier.com/content/search/scopus"
            assert result.authentication['type'] == "api_key_and_token"
            assert result.pagination['strategy'] == "offset_limit"
            assert result.pagination['items_per_page'] == 25
            assert result.rate_limits['requests_per_second'] == 2
            assert result.retries['max_attempts'] == 3
            assert result.default_parameters['view'] == "COMPLETE"
            assert result.cache['enabled'] is True
            assert result.response_mapping['total_results_path'] == "search-results.opensearch:totalResults"
        finally:
            os.unlink(config_path)
    
    def test_load_toml_config_with_missing_required_fields_raises_configuration_error(self):
        """
        Test that missing required TOML sections raise ConfigurationError
        """
        # Arrange
        incomplete_toml = """
        [api]
        name = "scopus"
        # missing base_url
        
        [authentication]
        type = "api_key"
        # missing other required sections
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(incomplete_toml)
            config_path = Path(f.name)
        
        try:
            # Act & Assert
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigLoader.load_toml_config(config_path)
            
            assert "Missing required configuration" in str(exc_info.value)
        finally:
            os.unlink(config_path)
    
    def test_load_toml_config_with_invalid_syntax_raises_parse_error(self):
        """
        Test that invalid TOML syntax raises appropriate parse error
        """
        # Arrange
        invalid_toml = """
        [api
        name = "scopus"  # missing closing bracket
        base_url = "https://api.example.com"
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(invalid_toml)
            config_path = Path(f.name)
        
        try:
            # Act & Assert
            with pytest.raises(Exception):  # TOML parsing error
                ConfigLoader.load_toml_config(config_path)
        finally:
            os.unlink(config_path)
    
    def test_load_toml_config_with_nonexistent_file_raises_file_not_found(self):
        """
        Test that attempting to load non-existent file raises FileNotFoundError
        """
        # Arrange
        nonexistent_path = Path("nonexistent_config.toml")
        
        # Act & Assert
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_toml_config(nonexistent_path)
    
    def test_validate_environment_variables_with_all_present_returns_true(self):
        """
        Test that when all required environment variables are set, validation returns True
        """
        # Arrange
        config = APIConfig(
            name="scopus",
            base_url="https://api.example.com",
            authentication={'api_key_env': 'SCOPUS_API_KEY', 'inst_token_env': 'SCOPUS_INST_ID'},
            pagination={'strategy': 'offset_limit'},
            rate_limits={'requests_per_second': 2},
            retries={'max_attempts': 3}
        )
        
        with patch.dict(os.environ, {'SCOPUS_API_KEY': 'test_key', 'SCOPUS_INST_ID': 'test_token'}):
            # Act
            result = ConfigLoader.validate_environment_variables(config)
            
            # Assert
            assert result is True
    
    def test_validate_environment_variables_with_missing_vars_raises_environment_error(self):
        """
        Test that missing environment variables raise EnvironmentError with details
        """
        # Arrange
        config = APIConfig(
            name="scopus",
            base_url="https://api.example.com",
            authentication={'api_key_env': 'MISSING_API_KEY', 'inst_token_env': 'MISSING_TOKEN'},
            pagination={'strategy': 'offset_limit'},
            rate_limits={'requests_per_second': 2},
            retries={'max_attempts': 3}
        )
        
        with patch.dict(os.environ, {}, clear=True):
            # Act & Assert
            with pytest.raises(EnvironmentError) as exc_info:
                ConfigLoader.validate_environment_variables(config)
            
            error_message = str(exc_info.value)
            assert "MISSING_API_KEY" in error_message
            assert "MISSING_TOKEN" in error_message
    
    def test_validate_environment_variables_with_partial_missing_raises_environment_error(self):
        """
        Test that partially missing environment variables raise EnvironmentError
        """
        # Arrange
        config = APIConfig(
            name="scopus",
            base_url="https://api.example.com",
            authentication={'api_key_env': 'PRESENT_KEY', 'inst_token_env': 'MISSING_TOKEN'},
            pagination={'strategy': 'offset_limit'},
            rate_limits={'requests_per_second': 2},
            retries={'max_attempts': 3}
        )
        
        with patch.dict(os.environ, {'PRESENT_KEY': 'test_value'}, clear=True):
            # Act & Assert
            with pytest.raises(EnvironmentError) as exc_info:
                ConfigLoader.validate_environment_variables(config)
            
            error_message = str(exc_info.value)
            assert "MISSING_TOKEN" in error_message
            assert "PRESENT_KEY" not in error_message
    
    def test_get_environment_value_with_existing_variable_returns_value(self):
        """
        Test that get_environment_value returns the correct value for existing variables
        """
        # Arrange
        test_value = "test_api_key_123"
        
        with patch.dict(os.environ, {'TEST_ENV_VAR': test_value}):
            # Act
            result = ConfigLoader.get_environment_value('TEST_ENV_VAR')
            
            # Assert
            assert result == test_value
    
    def test_get_environment_value_with_missing_variable_raises_environment_error(self):
        """
        Test that get_environment_value raises EnvironmentError for missing variables
        """
        # Arrange
        with patch.dict(os.environ, {}, clear=True):
            # Act & Assert
            with pytest.raises(EnvironmentError) as exc_info:
                ConfigLoader.get_environment_value('NONEXISTENT_VAR')
            
            assert "NONEXISTENT_VAR" in str(exc_info.value)
    
    def test_load_toml_config_with_minimal_configuration_succeeds(self):
        """
        Test that loading minimal required configuration succeeds
        """
        # Arrange
        minimal_toml = """
        [api]
        name = "test"
        base_url = "https://api.test.com"
        
        [authentication]
        type = "bearer"
        
        [pagination]
        strategy = "page_based"
        
        [rate_limits]
        requests_per_second = 1
        
        [retries]
        max_attempts = 2
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(minimal_toml)
            config_path = Path(f.name)
        
        try:
            # Act
            result = ConfigLoader.load_toml_config(config_path)
            
            # Assert
            assert isinstance(result, APIConfig)
            assert result.name == "test"
            assert result.base_url == "https://api.test.com"
            # Optional sections should have empty defaults
            assert result.default_parameters == {}
            assert result.cache == {}
            assert result.response_mapping == {}
        finally:
            os.unlink(config_path)
    
    def test_validate_environment_variables_with_empty_authentication_returns_true(self):
        """
        Test that validation succeeds when no environment variables are required
        """
        # Arrange
        config = APIConfig(
            name="test",
            base_url="https://api.test.com",
            authentication={},  # No environment variables required
            pagination={'strategy': 'offset_limit'},
            rate_limits={'requests_per_second': 1},
            retries={'max_attempts': 2}
        )
        
        # Act
        result = ConfigLoader.validate_environment_variables(config)
        
        # Assert
        assert result is True
    
    def test_get_environment_value_with_empty_string_value_returns_empty_string(self):
        """
        Test that empty string environment values are returned correctly
        """
        # Arrange
        with patch.dict(os.environ, {'EMPTY_VAR': ''}):
            # Act
            result = ConfigLoader.get_environment_value('EMPTY_VAR')
            
            # Assert
            assert result == ""