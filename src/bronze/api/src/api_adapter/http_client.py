"""
HTTPClient module for handling HTTP API requests with authentication and retry logic
"""

import time
import requests
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field


class PermanentAPIError(Exception):
    """Raised when API requests fail permanently after all retries"""
    pass


@dataclass
class APIRequest:
    """Represents a single API request"""
    url: str
    parameters: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)
    method: str = "GET"


@dataclass  
class APIResponse:
    """Standardised API response wrapper"""
    raw_data: Dict[str, Any]
    metadata: Dict[str, Any]
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    request_timestamp: datetime = field(default_factory=datetime.now)


class HTTPClient:
    """HTTP client with authentication, retry logic, and rate limiting"""
    
    # HTTP status codes that should trigger retries
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0, 
                 requests_per_second: float = 2.0):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.requests_per_second = requests_per_second
        self.headers: Dict[str, str] = {}
        self.session: Optional[requests.Session] = None
        self.last_request_time: Optional[float] = None
    
    def authenticate(self, credentials: Dict[str, Any]) -> None:
        """
        Configure authentication headers based on credential type
        
        Args:
            credentials: Dictionary containing authentication information
            
        Raises:
            ValueError: If authentication type is not supported
        """
        auth_type = credentials.get('type')
        
        if auth_type == 'api_key':
            self.headers['X-API-Key'] = credentials['api_key']
            
        elif auth_type == 'bearer_token':
            self.headers['Authorization'] = f"Bearer {credentials['token']}"
            
        elif auth_type == 'api_key_and_token':
            # Scopus-style authentication
            self.headers['X-ELS-APIKey'] = credentials['api_key']
            self.headers['X-ELS-Insttoken'] = credentials['inst_token']
            
        else:
            raise ValueError(f"Unsupported authentication type: {auth_type}")
    
    def make_request(self, request: APIRequest) -> APIResponse:
        """
        Make HTTP request with integrated retry logic and exponential backoff
        
        Args:
            request: APIRequest object containing request details
            
        Returns:
            APIResponse object with response data
            
        Raises:
            PermanentAPIError: If request fails after all retry attempts
            requests.exceptions.HTTPError: For non-retryable HTTP errors
        """
        # Apply rate limiting before making request
        self.apply_rate_limit()
        
        # Create session if not exists
        if self.session is None:
            self.session = requests.Session()
        
        # Combine authentication headers with request headers
        combined_headers = {**self.headers, **request.headers}
        
        retry_count = 0
        request_timestamp = datetime.now()
        response = None
        
        while retry_count <= self.max_retries:
            try:
                # Make the HTTP request based on method
                if request.method.upper() == 'GET':
                    response = self.session.get(
                        request.url,
                        params=request.parameters,
                        headers=combined_headers
                    )
                elif request.method.upper() == 'POST':
                    response = self.session.post(
                        request.url,
                        data=request.parameters,
                        headers=combined_headers
                    )
                else:
                    # For other methods, use requests directly
                    response = self.session.request(
                        request.method,
                        request.url,
                        params=request.parameters if request.method.upper() == 'GET' else None,
                        data=request.parameters if request.method.upper() != 'GET' else None,
                        headers=combined_headers
                    )
                
                # Check for HTTP errors
                response.raise_for_status()
                
                # Update last request time
                self.last_request_time = time.time()
                
                # Parse JSON response
                try:
                    raw_data = response.json()
                except ValueError:
                    # Handle non-JSON responses
                    raw_data = {'text': response.text}
                
                # Create metadata
                metadata = {
                    'url': request.url,
                    'method': request.method,
                    'parameters': request.parameters,
                    'retry_count': retry_count
                }
                
                # Return successful response
                return APIResponse(
                    raw_data=raw_data,
                    metadata=metadata,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    request_timestamp=request_timestamp
                )
                
            except requests.exceptions.HTTPError as e:
                # Check if this is a retryable error (need response object)
                if response and response.status_code in self.RETRYABLE_STATUS_CODES:
                    retry_count += 1
                    
                    if retry_count <= self.max_retries:
                        # True exponential backoff: base_delay * (factor ^ (retry_count - 1))
                        # Uses 1 second base delay
                        delay = 1.0 * (self.backoff_factor ** (retry_count - 1))
                        time.sleep(delay)
                        continue
                    else:
                        # Exceeded max retries
                        raise PermanentAPIError(
                            f"Failed after {self.max_retries} retry attempts. "
                            f"Last error: {str(e)}"
                        )
                else:
                    # Non-retryable error (4xx) or no response, raise immediately
                    raise e
                    
            except requests.exceptions.RequestException as e:
                # Network-level errors are retryable
                retry_count += 1
                
                if retry_count <= self.max_retries:
                    delay = 1.0 * (self.backoff_factor ** (retry_count - 1))
                    time.sleep(delay)
                    continue
                else:
                    raise PermanentAPIError(
                        f"Failed after {self.max_retries} retry attempts. "
                        f"Last error: {str(e)}"
                    )
    
    def apply_rate_limit(self) -> None:
        """
        Apply rate limiting delay to respect API quotas
        """
        if self.last_request_time is None:
            # First request, no delay needed
            return
        
        # Calculate time since last request
        time_since_last = time.time() - self.last_request_time
        
        # Calculate minimum delay between requests
        min_delay = 1.0 / self.requests_per_second
        
        # If insufficient time has passed, sleep for remaining time
        if time_since_last < min_delay:
            delay = min_delay - time_since_last
            time.sleep(delay)
    
    def close_connection(self) -> None:
        """
        Close HTTP session and release resources
        """
        if self.session:
            self.session.close()
            self.session = None