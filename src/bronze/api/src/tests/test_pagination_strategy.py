"""
Test suite for PaginationStrategy components
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
from unittest.mock import Mock
from api_adapter.pagination_strategy import (
    PaginationStrategy, OffsetLimitPagination, PageBasedPagination, 
    CursorBasedPagination, PaginationFactory, APIResponse
)


class TestOffsetLimitPagination:
    """Test suite for offset-based pagination strategy"""
    
    def test_get_next_page_params_with_first_page_returns_zero_offset(self):
        """
        Test that first page request returns start=0 and correct count
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'count_param': 'count'
        }
        pagination = OffsetLimitPagination(config)
        current_params = {}
        
        # Act
        result = pagination.get_next_page_params(current_params, None, 1)
        
        # Assert
        assert result == {'start': 0, 'count': 25}
    
    def test_get_next_page_params_with_second_page_returns_correct_offset(self):
        """
        Test that second page request calculates correct offset
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'count_param': 'count'
        }
        pagination = OffsetLimitPagination(config)
        current_params = {}
        
        # Act
        result = pagination.get_next_page_params(current_params, None, 2)
        
        # Assert
        assert result == {'start': 25, 'count': 25}
    
    def test_get_next_page_params_with_third_page_returns_correct_offset(self):
        """
        Test that third page with 25 items per page returns start=50
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'count_param': 'count'
        }
        pagination = OffsetLimitPagination(config)
        current_params = {}
        
        # Act
        result = pagination.get_next_page_params(current_params, None, 3)
        
        # Assert
        assert result == {'start': 50, 'count': 25}
    
    def test_get_next_page_params_with_custom_parameter_names_uses_correct_keys(self):
        """
        Test that custom parameter names are used correctly
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'start_param': 'offset',
            'count_param': 'limit'
        }
        pagination = OffsetLimitPagination(config)
        current_params = {}
        
        # Act
        result = pagination.get_next_page_params(current_params, None, 2)
        
        # Assert
        assert result == {'offset': 50, 'limit': 50}
    
    def test_get_next_page_params_when_offset_exceeds_total_returns_none(self):
        """
        Test that pagination stops when offset exceeds total results
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'count_param': 'count'
        }
        pagination = OffsetLimitPagination(config)
        
        # Mock response with total results
        mock_response = Mock(spec=APIResponse)
        mock_response.metadata = {'total_results': 50}
        
        # Act - page 3 would start at index 50, which equals total results
        result = pagination.get_next_page_params({}, mock_response, 3)
        
        # Assert
        assert result is None
    
    def test_extract_total_results_with_valid_scopus_response_returns_integer(self):
        """
        Test extraction of total results from Scopus-style response
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'total_results_path': 'search-results.opensearch:totalResults'
        }
        pagination = OffsetLimitPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {
            'search-results': {
                'opensearch:totalResults': '150',
                'entry': []
            }
        }
        
        # Act
        result = pagination.extract_total_results(mock_response)
        
        # Assert
        assert result == 150
    
    def test_extract_total_results_with_missing_field_returns_none(self):
        """
        Test that missing total results field returns None
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'total_results_path': 'search-results.opensearch:totalResults'
        }
        pagination = OffsetLimitPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {
            'search-results': {
                'entry': []
                # missing opensearch:totalResults
            }
        }
        
        # Act
        result = pagination.extract_total_results(mock_response)
        
        # Assert
        assert result is None
    
    def test_extract_total_results_with_nested_path_extracts_correctly(self):
        """
        Test extraction from deeply nested response structure
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'start_param': 'start',
            'total_results_path': 'results.metadata.totalCount'
        }
        pagination = OffsetLimitPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {
            'results': {
                'metadata': {
                    'totalCount': 75
                },
                'data': []
            }
        }
        
        # Act
        result = pagination.extract_total_results(mock_response)
        
        # Assert
        assert result == 75


class TestPageBasedPagination:
    """Test suite for page-based pagination strategy"""
    
    def test_get_next_page_params_with_first_page_returns_page_one(self):
        """
        Test that first page request returns page=1
        """
        # Arrange
        config = {
            'items_per_page': 100,
            'page_param': 'page',
            'size_param': 'size'
        }
        pagination = PageBasedPagination(config)
        
        # Act
        result = pagination.get_next_page_params({}, None, 1)
        
        # Assert
        assert result == {'page': 1, 'size': 100}
    
    def test_get_next_page_params_with_subsequent_pages_increments_correctly(self):
        """
        Test that page numbers increment correctly
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'page_param': 'page',
            'size_param': 'pageSize'
        }
        pagination = PageBasedPagination(config)
        
        # Act
        result = pagination.get_next_page_params({}, None, 5)
        
        # Assert
        assert result == {'page': 5, 'pageSize': 50}
    
    def test_get_next_page_params_when_exceeding_max_pages_returns_none(self):
        """
        Test that pagination stops when exceeding calculated max pages
        """
        # Arrange
        config = {
            'items_per_page': 25,
            'page_param': 'page',
            'size_param': 'size',
            'total_results_path': 'totalResults'
        }
        pagination = PageBasedPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {'totalResults': 75}  # 3 pages of 25 items each
        
        # Act - requesting page 4 when only 3 pages exist
        result = pagination.get_next_page_params({}, mock_response, 4)
        
        # Assert
        assert result is None
    
    def test_extract_total_results_with_direct_path_returns_value(self):
        """
        Test extraction of total results from simple path
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'page_param': 'page',
            'total_results_path': 'totalResults'
        }
        pagination = PageBasedPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {'totalResults': 200}
        
        # Act
        result = pagination.extract_total_results(mock_response)
        
        # Assert
        assert result == 200


class TestCursorBasedPagination:
    """Test suite for cursor-based pagination strategy"""
    
    def test_get_next_page_params_with_first_page_returns_no_cursor(self):
        """
        Test that first page request doesn't include cursor parameter
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'cursor_param': 'cursor',
            'limit_param': 'limit'
        }
        pagination = CursorBasedPagination(config)
        
        # Act
        result = pagination.get_next_page_params({}, None, 1)
        
        # Assert
        assert result == {'limit': 50}
        assert 'cursor' not in result
    
    def test_get_next_page_params_with_subsequent_page_uses_cursor_from_response(self):
        """
        Test that subsequent pages use cursor from previous response
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'cursor_param': 'cursor',
            'limit_param': 'limit',
            'next_cursor_path': 'pagination.nextCursor'
        }
        pagination = CursorBasedPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {
            'pagination': {'nextCursor': 'abc123def456'},
            'data': ['item1', 'item2']
        }
        
        # Act
        result = pagination.get_next_page_params({}, mock_response, 2)
        
        # Assert
        assert result == {'cursor': 'abc123def456', 'limit': 50}
    
    def test_get_next_page_params_with_no_next_cursor_returns_none(self):
        """
        Test that pagination stops when no next cursor is available
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'cursor_param': 'cursor',
            'limit_param': 'limit',
            'next_cursor_path': 'pagination.nextCursor'
        }
        pagination = CursorBasedPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {
            'pagination': {'nextCursor': None},  # No more pages
            'data': ['item1']
        }
        
        # Act
        result = pagination.get_next_page_params({}, mock_response, 2)
        
        # Assert
        assert result is None
    
    def test_extract_total_results_returns_none(self):
        """
        Test that cursor-based pagination doesn't provide total count upfront
        """
        # Arrange
        config = {
            'items_per_page': 50,
            'cursor_param': 'cursor',
            'limit_param': 'limit'
        }
        pagination = CursorBasedPagination(config)
        
        mock_response = Mock(spec=APIResponse)
        mock_response.raw_data = {'data': ['item1', 'item2']}
        
        # Act
        result = pagination.extract_total_results(mock_response)
        
        # Assert
        assert result is None


class TestPaginationFactory:
    """Test suite for pagination strategy factory"""
    
    def test_create_strategy_with_offset_limit_returns_offset_pagination(self):
        """
        Test that factory creates OffsetLimitPagination for offset_limit strategy
        """
        # Arrange
        config = {
            'strategy': 'offset_limit',
            'items_per_page': 25,
            'start_param': 'start'
        }
        
        # Act
        result = PaginationFactory.create_strategy(config)
        
        # Assert
        assert isinstance(result, OffsetLimitPagination)
    
    def test_create_strategy_with_page_based_returns_page_pagination(self):
        """
        Test that factory creates PageBasedPagination for page_based strategy
        """
        # Arrange
        config = {
            'strategy': 'page_based',
            'items_per_page': 100,
            'page_param': 'page'
        }
        
        # Act
        result = PaginationFactory.create_strategy(config)
        
        # Assert
        assert isinstance(result, PageBasedPagination)
    
    def test_create_strategy_with_cursor_based_returns_cursor_pagination(self):
        """
        Test that factory creates CursorBasedPagination for cursor_based strategy
        """
        # Arrange
        config = {
            'strategy': 'cursor_based',
            'items_per_page': 50,
            'cursor_param': 'cursor'
        }
        
        # Act
        result = PaginationFactory.create_strategy(config)
        
        # Assert
        assert isinstance(result, CursorBasedPagination)
    
    def test_create_strategy_with_unsupported_strategy_raises_value_error(self):
        """
        Test that factory raises ValueError for unsupported strategy types
        """
        # Arrange
        config = {
            'strategy': 'unsupported_strategy',
            'items_per_page': 25
        }
        
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            PaginationFactory.create_strategy(config)
        
        assert "Unsupported pagination strategy" in str(exc_info.value)
        assert "unsupported_strategy" in str(exc_info.value)
    
    def test_create_strategy_with_missing_strategy_key_raises_key_error(self):
        """
        Test that factory raises KeyError when strategy key is missing
        """
        # Arrange
        config = {
            'items_per_page': 25
            # missing 'strategy' key
        }
        
        # Act & Assert
        with pytest.raises(KeyError):
            PaginationFactory.create_strategy(config)