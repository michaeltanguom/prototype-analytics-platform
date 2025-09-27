"""
PayloadValidator module for validating API response data quality
"""

from typing import Dict, Any
from api_adapter.http_client import APIResponse


class PayloadValidator:
    """Validates API responses based on configurable validation rules"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def validate_response(self, response: APIResponse, config: Dict[str, Any]) -> bool:
        """
        Run all enabled validations on the API response
        
        Args:
            response: APIResponse object to validate
            config: Configuration specifying which validations to run
            
        Returns:
            True if all enabled validations pass, False otherwise
        """
        validations = []
        
        # Check each validation if enabled
        if config.get('response_completeness', False):
            validations.append(self.validate_response_completeness(response, config))
        
        if config.get('rate_limit_headers', False):
            validations.append(self.validate_rate_limit_headers(response, config))
        
        if config.get('empty_response_detection', False):
            validations.append(not self.detect_empty_response(response))  # Invert - empty is bad
        
        if config.get('json_structure_validation', False):
            validations.append(self.validate_json_structure(response))
        
        # All enabled validations must pass
        return all(validations) if validations else True
    
    def validate_response_completeness(self, response: APIResponse, config: Dict[str, Any]) -> bool:
        """
        Validate that totalResults matches actual item count in response
        
        Args:
            response: APIResponse object to validate
            config: Configuration containing field mapping
            
        Returns:
            True if counts match, False otherwise
        """
        try:
            # Extract total results using configured path
            total_results_path = config.get('headers', {}).get('total_results_field', '')
            if not total_results_path:
                return False
            
            # Navigate the nested path
            data = response.raw_data
            path_parts = total_results_path.split('.')
            
            for part in path_parts:
                if ':' in part:
                    # Handle namespaced keys like 'opensearch:totalResults'
                    data = data[part]
                else:
                    data = data[part]
            
            total_results = int(data)
            
            # Extract actual items count
            # Try common patterns for items array
            actual_count = self._extract_items_count(response.raw_data)
            
            return actual_count == total_results
            
        except (KeyError, ValueError, TypeError):
            return False
    
    def validate_rate_limit_headers(self, response: APIResponse, config: Dict[str, Any]) -> bool:
        """
        Validate that required rate limit headers are present
        
        Args:
            response: APIResponse object to validate
            config: Configuration containing header names
            
        Returns:
            True if required headers are present, False otherwise
        """
        rate_limit_header = config.get('headers', {}).get('rate_limit_remaining')
        if not rate_limit_header:
            return True  # No header required
        
        return rate_limit_header in response.headers
    
    def validate_json_structure(self, response: APIResponse) -> bool:
        """
        Validate that response has valid JSON structure with expected fields
        
        Args:
            response: APIResponse object to validate
            
        Returns:
            True if structure is valid, False otherwise
        """
        try:
            # Check if raw_data is a dictionary (valid JSON object)
            if not isinstance(response.raw_data, dict):
                return False
            
            # Check if response has some content (not empty)
            if not response.raw_data:
                return False
            
            # Basic structure validation - should have some recognisable content
            # Look for common API response patterns
            has_results = any(key in response.raw_data for key in [
                'search-results', 'results', 'data', 'response', 'entries'
            ])
            
            return has_results
            
        except (AttributeError, TypeError):
            return False
    
    def detect_empty_response(self, response: APIResponse) -> bool:
        """
        Detect if the API response contains no actual data
        
        Args:
            response: APIResponse object to check
            
        Returns:
            True if response is empty, False if it contains data
        """
        try:
            # Look for common patterns indicating empty responses
            
            # Check for zero total results
            if 'search-results' in response.raw_data:
                search_results = response.raw_data['search-results']
                
                # Check opensearch:totalResults
                total_results = search_results.get('opensearch:totalResults', '0')
                if int(total_results) == 0:
                    return True
                
                # Check entry array
                entry = search_results.get('entry', [])
                if not entry or len(entry) == 0:
                    return True
            
            # Check for other common empty patterns
            if 'results' in response.raw_data:
                results = response.raw_data['results']
                if isinstance(results, dict):
                    total_count = results.get('totalCount', results.get('count', 0))
                    if int(total_count) == 0:
                        return True
                    
                    data = results.get('data', results.get('items', []))
                    if not data or len(data) == 0:
                        return True
            
            # General check for completely empty response
            if not response.raw_data or response.raw_data == {}:
                return True
            
            return False
            
        except (KeyError, ValueError, TypeError):
            # If we can't parse the response, consider it empty
            return True
    
    def _extract_items_count(self, raw_data: Dict[str, Any]) -> int:
        """
        Extract the actual count of items from response data
        
        Args:
            raw_data: Raw response data dictionary
            
        Returns:
            Count of actual items in response
        """
        # Try common patterns for item arrays
        if 'search-results' in raw_data:
            entry = raw_data['search-results'].get('entry', [])
            return len(entry) if isinstance(entry, list) else 0
        
        if 'results' in raw_data:
            results = raw_data['results']
            if isinstance(results, dict):
                data = results.get('data', results.get('items', []))
                return len(data) if isinstance(data, list) else 0
        
        if 'data' in raw_data:
            data = raw_data['data']
            return len(data) if isinstance(data, list) else 0
        
        if 'items' in raw_data:
            items = raw_data['items']
            return len(items) if isinstance(items, list) else 0
        
        return 0