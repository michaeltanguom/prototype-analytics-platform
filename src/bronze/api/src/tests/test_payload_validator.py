"""
Test suite for PayloadValidator component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
from unittest.mock import Mock
from api_adapter.payload_validator import PayloadValidator
from api_adapter.http_client import APIResponse
from datetime import datetime


class TestPayloadValidator:
    """Test suite for PayloadValidator response validation functionality"""
    
    def test_validate_response_with_all_validations_enabled_and_passing_returns_true(self):
        """
        Test that when all validations are enabled and pass, validate_response returns True
        """
        # Arrange
        config = {
            'response_completeness': True,
            'rate_limit_headers': True,
            'empty_response_detection': True,
            'json_structure_validation': True,
            'headers': {
                'total_results_field': 'search-results.opensearch:totalResults',
                'rate_limit_remaining': 'X-RateLimit-Remaining'
            }
        }
        
        validator = PayloadValidator(config)
        
        # Mock response with valid data - make sure counts match
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '2',  # Changed to match actual items
                    'entry': [{'id': '1'}, {'id': '2'}]  # 2 items
                }
            },
            metadata={'total_results': 2},
            status_code=200,
            headers={'X-RateLimit-Remaining': '95'}
        )
        
        # Act
        result = validator.validate_response(api_response, config)
        
        # Assert
        assert result is True
    
    def test_validate_response_with_failing_validation_returns_false(self):
        """
        Test that when any validation fails, validate_response returns False
        """
        # Arrange
        config = {
            'response_completeness': True,
            'rate_limit_headers': True,
            'empty_response_detection': True,
            'json_structure_validation': True,
            'headers': {
                'total_results_field': 'search-results.opensearch:totalResults',
                'rate_limit_remaining': 'X-RateLimit-Remaining'
            }
        }
        
        validator = PayloadValidator(config)
        
        # Mock response with missing rate limit headers
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '25',
                    'entry': [{'id': '1'}, {'id': '2'}]
                }
            },
            metadata={'total_results': 25},
            status_code=200,
            headers={}  # Missing rate limit headers
        )
        
        # Act
        result = validator.validate_response(api_response, config)
        
        # Assert
        assert result is False
    
    def test_validate_response_completeness_with_matching_counts_returns_true(self):
        """
        Test that response completeness validation passes when counts match
        """
        # Arrange
        config = {
            'headers': {
                'total_results_field': 'search-results.opensearch:totalResults'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '2',
                    'entry': [{'id': '1'}, {'id': '2'}]
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_response_completeness(api_response, config)
        
        # Assert
        assert result is True
    
    def test_validate_response_completeness_with_mismatched_counts_returns_false(self):
        """
        Test that response completeness validation fails when totalResults doesn't match actual items
        """
        # Arrange
        config = {
            'headers': {
                'total_results_field': 'search-results.opensearch:totalResults'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '50',  # Claims 50 results
                    'entry': [{'id': '1'}, {'id': '2'}]  # But only has 2
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_response_completeness(api_response, config)
        
        # Assert
        assert result is False
    
    def test_validate_response_completeness_with_missing_total_results_field_returns_false(self):
        """
        Test that response completeness validation fails when total results field is missing
        """
        # Arrange
        config = {
            'headers': {
                'total_results_field': 'search-results.opensearch:totalResults'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'entry': [{'id': '1'}, {'id': '2'}]
                    # Missing opensearch:totalResults
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_response_completeness(api_response, config)
        
        # Assert
        assert result is False
    
    def test_validate_rate_limit_headers_with_present_headers_returns_true(self):
        """
        Test that rate limit validation passes when required headers are present
        """
        # Arrange
        config = {
            'headers': {
                'rate_limit_remaining': 'X-RateLimit-Remaining'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={},
            metadata={},
            status_code=200,
            headers={'X-RateLimit-Remaining': '95'}
        )
        
        # Act
        result = validator.validate_rate_limit_headers(api_response, config)
        
        # Assert
        assert result is True
    
    def test_validate_rate_limit_headers_with_missing_headers_returns_false(self):
        """
        Test that rate limit validation fails when required headers are missing
        """
        # Arrange
        config = {
            'headers': {
                'rate_limit_remaining': 'X-RateLimit-Remaining'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={},
            metadata={},
            status_code=200,
            headers={}  # Missing rate limit headers
        )
        
        # Act
        result = validator.validate_rate_limit_headers(api_response, config)
        
        # Assert
        assert result is False
    
    def test_validate_json_structure_with_valid_structure_returns_true(self):
        """
        Test that JSON structure validation passes for well-formed responses
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'entry': [{'id': '1'}],
                    'opensearch:totalResults': '25'
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_json_structure(api_response)
        
        # Assert
        assert result is True
    
    def test_validate_json_structure_with_missing_top_level_keys_returns_false(self):
        """
        Test that JSON structure validation fails when expected top-level keys are missing
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data={},  # Empty response
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_json_structure(api_response)
        
        # Assert
        assert result is False
    
    def test_validate_json_structure_with_invalid_json_returns_false(self):
        """
        Test that JSON structure validation fails for malformed JSON data
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data="not valid json",  # String instead of dict
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_json_structure(api_response)
        
        # Assert
        assert result is False
    
    def test_detect_empty_response_with_zero_results_returns_true(self):
        """
        Test that empty response detection correctly identifies responses with no data
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '0',
                    'entry': []
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.detect_empty_response(api_response)
        
        # Assert
        assert result is True
    
    def test_detect_empty_response_with_data_present_returns_false(self):
        """
        Test that empty response detection returns False when data is present
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '2',
                    'entry': [{'id': '1'}, {'id': '2'}]
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.detect_empty_response(api_response)
        
        # Assert
        assert result is False
    
    def test_detect_empty_response_with_missing_entry_field_returns_true(self):
        """
        Test that empty response detection handles missing entry fields
        """
        # Arrange
        validator = PayloadValidator({})
        
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '0'
                    # Missing 'entry' field
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.detect_empty_response(api_response)
        
        # Assert
        assert result is True
    
    def test_validate_response_with_disabled_validations_skips_checks(self):
        """
        Test that disabled validations are properly skipped
        """
        # Arrange
        config = {
            'response_completeness': False,
            'rate_limit_headers': False,
            'empty_response_detection': True,
            'json_structure_validation': True
        }
        
        validator = PayloadValidator(config)
        
        # Mock response that would fail completeness and rate limit checks
        api_response = APIResponse(
            raw_data={
                'search-results': {
                    'opensearch:totalResults': '50',  # Mismatched count
                    'entry': [{'id': '1'}]  # Only 1 item
                }
            },
            metadata={},
            status_code=200,
            headers={}  # Missing rate limit headers
        )
        
        # Act
        result = validator.validate_response(api_response, config)
        
        # Assert
        # Should pass because the failing validations are disabled
        assert result is True
    
    def test_validate_response_completeness_with_nested_path_extracts_correctly(self):
        """
        Test that response completeness validation can extract total results from nested paths
        """
        # Arrange
        config = {
            'headers': {
                'total_results_field': 'results.metadata.totalCount'
            }
        }
        
        validator = PayloadValidator(config)
        
        api_response = APIResponse(
            raw_data={
                'results': {
                    'metadata': {
                        'totalCount': '3'
                    },
                    'data': [{'id': '1'}, {'id': '2'}, {'id': '3'}]
                }
            },
            metadata={},
            status_code=200
        )
        
        # Act
        result = validator.validate_response_completeness(api_response, config)
        
        # Assert
        assert result is True