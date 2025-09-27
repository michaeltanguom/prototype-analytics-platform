"""
PaginationStrategy module for handling different API pagination patterns
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Protocol
from dataclasses import dataclass


@dataclass
class APIResponse:
    """Standardised API response wrapper for pagination testing"""
    raw_data: Dict[str, Any]
    metadata: Dict[str, Any]


class PaginationStrategy(Protocol):
    """Protocol for different pagination strategies"""
    
    def get_next_page_params(self, current_params: Dict[str, Any], 
                           response: Optional[APIResponse], page_num: int) -> Optional[Dict[str, Any]]:
        """Return parameters for next page, or None if no more pages"""
        ...
    
    def extract_total_results(self, response: APIResponse) -> Optional[int]:
        """Extract total result count from response"""
        ...


class OffsetLimitPagination:
    """Offset-based pagination strategy (used by Scopus)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.items_per_page = config['items_per_page']
        self.start_param = config['start_param']
        self.count_param = config.get('count_param', 'count')
        self.total_results_path = config.get('total_results_path', '')
    
    def get_next_page_params(self, current_params: Dict[str, Any], 
                           response: Optional[APIResponse], page_num: int) -> Optional[Dict[str, Any]]:
        """Calculate offset for next page"""
        start_index = (page_num - 1) * self.items_per_page
        
        # Check if we've exceeded total results
        if response and hasattr(response, 'metadata') and response.metadata:
            total_results = response.metadata.get('total_results')
            if total_results is not None and start_index >= total_results:
                return None
        
        return {
            self.start_param: start_index,
            self.count_param: self.items_per_page
        }
    
    def extract_total_results(self, response: APIResponse) -> Optional[int]:
        """Extract total results from response using configured path"""
        try:
            data = response.raw_data
            path_parts = self.total_results_path.split('.')
            
            for part in path_parts:
                if ':' in part:
                    # Handle namespaced keys like 'opensearch:totalResults'
                    data = data[part]
                else:
                    data = data[part]
            
            return int(data)
        except (KeyError, ValueError, TypeError):
            return None


class PageBasedPagination:
    """Page-based pagination strategy (used by SciVal)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.items_per_page = config['items_per_page']
        self.page_param = config['page_param']
        self.size_param = config.get('size_param', 'size')
        self.total_results_path = config.get('total_results_path', '')
    
    def get_next_page_params(self, current_params: Dict[str, Any], 
                           response: Optional[APIResponse], page_num: int) -> Optional[Dict[str, Any]]:
        """Calculate page number for next page"""
        
        # Check if we have response data to determine if more pages exist
        if response:
            total_results = self.extract_total_results(response)
            if total_results is not None:
                max_pages = (total_results + self.items_per_page - 1) // self.items_per_page
                if page_num > max_pages:
                    return None
        
        return {
            self.page_param: page_num,
            self.size_param: self.items_per_page
        }
    
    def extract_total_results(self, response: APIResponse) -> Optional[int]:
        """Extract total results from response"""
        try:
            data = response.raw_data
            path_parts = self.total_results_path.split('.')
            
            for part in path_parts:
                data = data[part]
            
            return int(data)
        except (KeyError, ValueError, TypeError):
            return None


class CursorBasedPagination:
    """Cursor-based pagination strategy (used by Web of Science)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.items_per_page = config['items_per_page']
        self.cursor_param = config['cursor_param']
        self.limit_param = config.get('limit_param', 'limit')
        self.next_cursor_path = config.get('next_cursor_path', '')
        self.current_cursor = None
    
    def get_next_page_params(self, current_params: Dict[str, Any], 
                           response: Optional[APIResponse], page_num: int) -> Optional[Dict[str, Any]]:
        """Get cursor for next page"""
        
        if page_num == 1:
            # First page - no cursor needed
            params = {self.limit_param: self.items_per_page}
            if self.current_cursor:
                params[self.cursor_param] = self.current_cursor
            return params
        
        if response is None:
            return None
            
        # Extract next cursor from response
        try:
            data = response.raw_data
            path_parts = self.next_cursor_path.split('.')
            
            for part in path_parts:
                data = data[part]
            
            next_cursor = data
            if not next_cursor:
                return None  # No more pages
                
            self.current_cursor = next_cursor
            return {
                self.cursor_param: next_cursor,
                self.limit_param: self.items_per_page
            }
            
        except (KeyError, TypeError):
            return None
    
    def extract_total_results(self, response: APIResponse) -> Optional[int]:
        """Cursor-based APIs often don't provide total count upfront"""
        return None


class PaginationFactory:
    """Factory for creating appropriate pagination strategy based on config"""
    
    STRATEGIES = {
        'offset_limit': OffsetLimitPagination,
        'page_based': PageBasedPagination,
        'cursor_based': CursorBasedPagination
    }
    
    @classmethod
    def create_strategy(cls, pagination_config: Dict[str, Any]) -> PaginationStrategy:
        """Create pagination strategy instance based on configuration"""
        strategy_type = pagination_config['strategy']
        
        if strategy_type not in cls.STRATEGIES:
            raise ValueError(f"Unsupported pagination strategy: {strategy_type}")
        
        strategy_class = cls.STRATEGIES[strategy_type]
        return strategy_class(pagination_config)