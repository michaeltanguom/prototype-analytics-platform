"""
Test suite for HTTPClient component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import requests
from api_adapter.http_client import HTTPClient, APIRequest, APIResponse, PermanentAPIError
from api_adapter.config_loader import APIConfig


class TestHTTPClient:
    """Test suite for HTTPClient API communication functionality"""
    
    def test_authenticate_with_api_key_credentials_sets_headers(self):
        """
        Test that API key authentication configures headers correctly
        """
        # Arrange
        credentials = {
            'type': 'api_key',
            'api_key': 'test_api_key_123'
        }
        http_client = HTTPClient()
        
        # Act
        http_client.authenticate(credentials)
        
        # Assert
        assert 'X-API-Key' in http_client.headers
        assert http_client.headers['X-API-Key'] == 'test_api_key_123'
    
    def test_authenticate_with_bearer_token_sets_authorisation_header(self):
        """
        Test that bearer token authentication sets Authorisation header
        """
        # Arrange
        credentials = {
            'type': 'bearer_token',
            'token': 'bearer_token_abc123'
        }
        http_client = HTTPClient()
        
        # Act
        http_client.authenticate(credentials)
        
        # Assert
        assert 'Authorisation' in http_client.headers
        assert http_client.headers['Authorisation'] == 'Bearer bearer_token_abc123'
    
    def test_authenticate_with_api_key_and_token_sets_both_headers(self):
        """
        Test that combined authentication sets multiple headers
        """
        # Arrange
        credentials = {
            'type': 'api_key_and_token',
            'api_key': 'scopus_key_123',
            'inst_token': 'institution_token_456'
        }
        http_client = HTTPClient()
        
        # Act
        http_client.authenticate(credentials)
        
        # Assert
        assert 'X-ELS-APIKey' in http_client.headers
        assert 'X-ELS-Insttoken' in http_client.headers
        assert http_client.headers['X-ELS-APIKey'] == 'scopus_key_123'
        assert http_client.headers['X-ELS-Insttoken'] == 'institution_token_456'
    
    def test_authenticate_with_unsupported_type_raises_value_error(self):
        """
        Test that unsupported authentication type raises ValueError
        """
        # Arrange
        credentials = {
            'type': 'unsupported_auth_type',
            'secret': 'some_secret'
        }
        http_client = HTTPClient()
        
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            http_client.authenticate(credentials)
        
        assert "Unsupported authentication type" in str(exc_info.value)
    
    @patch('requests.Session')
    def test_make_request_with_successful_response_returns_api_response(self, mock_session_class):
        """
        Test that successful HTTP request returns properly formed APIResponse
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test_response'}
        mock_response.headers = {'Content-Type': 'application/json', 'X-RateLimit-Remaining': '95'}
        mock_response.raise_for_status.return_value = None
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient()
        api_request = APIRequest(
            url='https://api.test.com/endpoint',
            parameters={'param1': 'value1'},
            headers={'Accept': 'application/json'},
            method='GET'
        )
        
        # Act
        result = http_client.make_request(api_request)
        
        # Assert
        assert isinstance(result, APIResponse)
        assert result.raw_data == {'data': 'test_response'}
        assert result.status_code == 200
        assert result.headers['X-RateLimit-Remaining'] == '95'
        assert isinstance(result.request_timestamp, datetime)
        
        # Verify request was made correctly
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert call_args[1]['params'] == {'param1': 'value1'}
    
    @patch('requests.Session')
    def test_make_request_with_http_503_retries_with_exponential_backoff(self, mock_session_class):
        """
        Test that temporary HTTP failures trigger retry with exponential backoff
        """
        # Arrange
        # First two calls fail with 503, third succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 503
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
        
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {'data': 'success'}
        mock_response_success.headers = {}
        mock_response_success.raise_for_status.return_value = None
        
        mock_session = Mock()
        mock_session.get.side_effect = [
            mock_response_fail,  # First attempt fails
            mock_response_fail,  # Second attempt fails
            mock_response_success # Third attempt succeeds
        ]
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient(max_retries=3, backoff_factor=2)  # True exponential backoff
        api_request = APIRequest(url='https://api.test.com/endpoint', parameters={})
        
        # Act
        with patch('time.sleep') as mock_sleep:
            result = http_client.make_request(api_request)
        
        # Assert
        assert result.status_code == 200
        assert mock_session.get.call_count == 3
        assert mock_sleep.call_count == 2  # Two retry delays
        
        # Verify exponential backoff: 1 second, then 2 seconds
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_calls[0] == 1.0  # First retry delay (2^0 = 1)
        assert sleep_calls[1] == 2.0  # Second retry delay (2^1 = 2)
    
    @patch('requests.Session')
    @patch('time.sleep')  # Mock sleep to avoid actual delays
    def test_make_request_with_permanent_failure_after_max_retries_raises_error(self, mock_sleep, mock_session_class):
        """
        Test that exceeding max retries raises PermanentAPIError
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Internal Server Error")
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient(max_retries=2, backoff_factor=2)  # Use proper exponential backoff
        api_request = APIRequest(url='https://api.test.com/endpoint', parameters={})
        
        # Act & Assert
        with pytest.raises(PermanentAPIError) as exc_info:
            http_client.make_request(api_request)
        
        assert "Failed after 2 retry attempts" in str(exc_info.value)
        assert mock_session.get.call_count == 3  # Initial + 2 retries

    @patch('requests.Session')
    def test_make_request_with_non_retryable_error_raises_immediately(self, mock_session_class):
        """
        Test that non-retryable errors (4xx) don't trigger retries
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient(max_retries=3)
        api_request = APIRequest(url='https://api.test.com/endpoint', parameters={})
        
        # Act & Assert
        with pytest.raises(requests.exceptions.HTTPError):
            http_client.make_request(api_request)
        
        assert mock_session.get.call_count == 1  # No retries for 4xx errors
    
    @patch('time.sleep')
    def test_apply_rate_limit_with_requests_per_second_waits_appropriate_time(self, mock_sleep):
        """
        Test that rate limiting enforces correct delay between requests
        """
        # Arrange
        http_client = HTTPClient(requests_per_second=2.0)  # 0.5 seconds between requests
        
        # Simulate that no time has passed since last request
        http_client.last_request_time = time.time()
        
        # Act
        http_client.apply_rate_limit()
        
        # Assert
        mock_sleep.assert_called_once()
        sleep_duration = mock_sleep.call_args[0][0]
        assert 0.4 <= sleep_duration <= 0.6  # Should be ~0.5 seconds, allowing for timing variance
    
    @patch('time.sleep')
    def test_apply_rate_limit_with_sufficient_time_passed_returns_immediately(self, mock_sleep):
        """
        Test that rate limiting skips delay when sufficient time has passed
        """
        # Arrange
        http_client = HTTPClient(requests_per_second=2.0)
        
        # Simulate that sufficient time has passed since last request
        http_client.last_request_time = time.time() - 1.0  # 1 second ago
        
        # Act
        http_client.apply_rate_limit()
        
        # Assert
        mock_sleep.assert_not_called()
    
    def test_apply_rate_limit_with_no_previous_request_returns_immediately(self):
        """
        Test that rate limiting doesn't delay on first request
        """
        # Arrange
        http_client = HTTPClient(requests_per_second=2.0)
        
        # Act & Assert - should not raise any exceptions or block
        with patch('time.sleep') as mock_sleep:
            http_client.apply_rate_limit()
        
        mock_sleep.assert_not_called()
    
    @patch('requests.Session')
    def test_make_request_with_post_method_uses_correct_http_method(self, mock_session_class):
        """
        Test that POST requests are handled correctly
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'created': True}
        mock_response.headers = {}
        mock_response.raise_for_status.return_value = None
        
        mock_session = Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient()
        api_request = APIRequest(
            url='https://api.test.com/create',
            parameters={'data': 'test'},
            method='POST'
        )
        
        # Act
        result = http_client.make_request(api_request)
        
        # Assert
        assert result.status_code == 201
        mock_session.post.assert_called_once()
        # GET should not be called
        mock_session.get.assert_not_called()
    
    def test_close_connection_with_active_session_closes_successfully(self):
        """
        Test that closing connection properly cleans up session
        """
        # Arrange
        http_client = HTTPClient()
        mock_session = Mock()
        http_client.session = mock_session
        
        # Act
        http_client.close_connection()
        
        # Assert
        mock_session.close.assert_called_once()
        assert http_client.session is None
    
    def test_close_connection_with_no_session_handles_gracefully(self):
        """
        Test that closing connection when no session exists doesn't raise error
        """
        # Arrange
        http_client = HTTPClient()
        
        # Act & Assert - should not raise any exceptions
        http_client.close_connection()
    
    @patch('requests.Session')
    def test_make_request_updates_last_request_time(self, mock_session_class):
        """
        Test that make_request updates the last request timestamp
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}
        mock_response.headers = {}
        mock_response.raise_for_status.return_value = None
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient()
        api_request = APIRequest(url='https://api.test.com/endpoint', parameters={})
        
        # Store initial time
        initial_time = http_client.last_request_time
        
        # Act
        http_client.make_request(api_request)
        
        # Assert
        assert http_client.last_request_time is not None
        assert http_client.last_request_time != initial_time
    
    @patch('requests.Session')
    def test_make_request_includes_authentication_headers(self, mock_session_class):
        """
        Test that make_request includes authentication headers in request
        """
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}
        mock_response.headers = {}
        mock_response.raise_for_status.return_value = None
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        http_client = HTTPClient()
        http_client.authenticate({
            'type': 'api_key',
            'api_key': 'test_key_123'
        })
        
        api_request = APIRequest(
            url='https://api.test.com/endpoint',
            parameters={},
            headers={'Custom-Header': 'custom_value'}
        )
        
        # Act
        http_client.make_request(api_request)
        
        # Assert
        call_args = mock_session.get.call_args
        headers = call_args[1]['headers']
        
        # Should include both auth headers and request-specific headers
        assert 'X-API-Key' in headers
        assert headers['X-API-Key'] == 'test_key_123'
        assert 'Custom-Header' in headers
        assert headers['Custom-Header'] == 'custom_value'