"""
Test suite for APIOrchestrator component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
from unittest.mock import Mock, MagicMock, mock_open, patch
from datetime import datetime, timezone, timedelta
from pathlib import Path
from api_adapter.api_orchestrator import APIOrchestrator


class TestAPIOrchestrator:
    """Test suite for APIOrchestrator coordination functionality"""
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Mock all dependencies
        self.config_loader = Mock()
        self.database_manager = Mock()
        self.state_manager = Mock()
        self.http_client = Mock()
        self.payload_validator = Mock()
        self.manifest_generator = Mock()
        self.recovery_manager = Mock()
        self.rate_limit_tracker = Mock()
        self.cache_manager = Mock()
        
        # Create orchestrator with mocked dependencies
        self.orchestrator = APIOrchestrator(
            config_loader=self.config_loader,
            database_manager=self.database_manager,
            state_manager=self.state_manager,
            http_client=self.http_client,
            payload_validator=self.payload_validator,
            manifest_generator=self.manifest_generator,
            recovery_manager=self.recovery_manager,
            rate_limit_tracker=self.rate_limit_tracker,
            cache_manager=self.cache_manager
        )
        
        self.base_time = datetime.now(timezone.utc)
    
    def test_load_entities_from_file_with_valid_file_returns_entity_list(self):
        """
        Test that a valid entity file returns list of entity IDs
        """
        # Arrange
        file_content = "author_001\nauthor_002\nauthor_003\n"
        mock_file_path = Path("test_entities.txt")
        
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                # Act
                entities = self.orchestrator.load_entities_from_file(mock_file_path)
        
        # Assert
        assert entities == ["author_001", "author_002", "author_003"]
        assert len(entities) == 3
    
    def test_load_entities_from_file_with_missing_file_raises_file_not_found_error(self):
        """
        Test that missing entity file raises FileNotFoundError
        """
        # Arrange
        mock_file_path = Path("nonexistent.txt")
        
        with patch.object(Path, "exists", return_value=False):
            # Act & Assert
            with pytest.raises(FileNotFoundError) as exc_info:
                self.orchestrator.load_entities_from_file(mock_file_path)
        
        assert "Entity file not found" in str(exc_info.value)
    
    def test_load_entities_from_file_with_empty_file_raises_value_error(self):
        """
        Test that empty entity file raises ValueError
        """
        # Arrange
        empty_content = ""
        mock_file_path = Path("empty_entities.txt")
        
        with patch("builtins.open", mock_open(read_data=empty_content)):
            with patch.object(Path, "exists", return_value=True):
                # Act & Assert
                with pytest.raises(ValueError) as exc_info:
                    self.orchestrator.load_entities_from_file(mock_file_path)
        
        assert "Entity file is empty" in str(exc_info.value)
    
    def test_load_entities_from_file_with_whitespace_only_lines_filters_empty_lines(self):
        """
        Test that file with whitespace and empty lines filters correctly
        """
        # Arrange
        file_content = "author_001\n\n  \nauthor_002\n\t\nauthor_003\n"
        mock_file_path = Path("test_entities.txt")
        
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                # Act
                entities = self.orchestrator.load_entities_from_file(mock_file_path)
        
        # Assert
        assert entities == ["author_001", "author_002", "author_003"]
        assert len(entities) == 3
    
    def test_generate_load_id_with_data_source_returns_unique_identifier(self):
        """
        Test that load ID generation creates unique identifier with data source
        """
        # Arrange
        data_source = "scopus_search"
        
        # Act
        load_id = self.orchestrator.generate_load_id(data_source)
        
        # Assert
        assert load_id.startswith("scopus_search_")
        assert len(load_id.split("_")) >= 4  # source_date_time_uuid
        assert isinstance(load_id, str)
    
    def test_generate_load_id_with_different_sources_returns_different_identifiers(self):
        """
        Test that different data sources generate different load IDs
        """
        # Arrange
        source1 = "scopus_search"
        source2 = "scival_metrics"
        
        # Act
        load_id1 = self.orchestrator.generate_load_id(source1)
        load_id2 = self.orchestrator.generate_load_id(source2)
        
        # Assert
        assert load_id1 != load_id2
        assert load_id1.startswith("scopus_search_")
        assert load_id2.startswith("scival_metrics_")
    
    def test_pass_load_id_to_components_initialises_processing_results(self):
        """
        Test that passing load ID initialises processing results structure
        """
        # Arrange
        load_id = "test_load_123"
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        self.config_loader.get_config.return_value = mock_config
        
        # Act
        self.orchestrator.pass_load_id_to_components(load_id)
        
        # Assert
        assert self.orchestrator.current_load_id == load_id
        assert self.orchestrator.processing_results['load_id'] == load_id
        assert self.orchestrator.processing_results['data_source'] == "scopus_search"
        assert 'start_time' in self.orchestrator.processing_results
        assert self.orchestrator.processing_results['entities'] == []
    
    def test_process_single_entity_with_successful_completion_marks_entity_complete(self):
        """
        Test that successful entity processing marks entity as completed
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        # Mock state manager - no existing state
        self.state_manager.load_state.return_value = None
        
        # Mock rate limit check
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 0, 'count': 25},  # First page
                None  # No more pages
            ]
            mock_strategy.extract_total_results.return_value = 50
            mock_factory.create_strategy.return_value = mock_strategy
            
            # Mock HTTP response
            mock_response = Mock()
            mock_response.data = {'search-results': {'entry': []}}
            mock_response.timestamp = self.base_time
            mock_response.metadata = {}
            mock_response.log_data = {}
            mock_response.content_hash = 'test_hash'
            self.http_client.make_request.return_value = mock_response
            
            # Mock payload validation
            self.payload_validator.validate_response.return_value = True
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        self.state_manager.mark_completed.assert_called_once_with(entity_id, load_id)
        self.database_manager.upsert_api_response.assert_called_once()
        assert len(self.orchestrator.processing_results['entities']) == 1
        entity_result = self.orchestrator.processing_results['entities'][0]
        assert entity_result['entity_id'] == entity_id
        assert entity_result['status'] == 'completed'
        assert entity_result['pages_processed'] == 1
    
    def test_process_single_entity_with_api_failure_records_error_and_continues(self):
        """
        Test that API failures are recorded as errors but don't stop processing
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        # Mock state manager
        self.state_manager.load_state.return_value = None
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy with failure on second page
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 0, 'count': 25},  # First page
                {'start': 25, 'count': 25},  # Second page (will fail)
                None  # No more pages
            ]
            mock_strategy.extract_total_results.return_value = 50
            mock_factory.create_strategy.return_value = mock_strategy
            
            # Mock HTTP responses - first succeeds, second fails
            mock_response_success = Mock()
            mock_response_success.data = {'search-results': {'entry': []}}
            mock_response_success.timestamp = self.base_time
            mock_response_success.metadata = {}
            mock_response_success.log_data = {}
            mock_response_success.content_hash = 'test_hash'
            
            self.http_client.make_request.side_effect = [
                mock_response_success,
                Exception("API Error")
            ]
            
            self.payload_validator.validate_response.return_value = True
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        self.state_manager.log_processing_error.assert_called()
        assert len(self.orchestrator.processing_results['entities']) == 1
        entity_result = self.orchestrator.processing_results['entities'][0]
        assert entity_result['entity_id'] == entity_id
        assert entity_result['status'] == 'completed'  # Still completed despite page error
        assert len(entity_result['errors']) == 1
        assert entity_result['errors'][0]['type'] == 'Exception'
        assert entity_result['errors'][0]['message'] == 'API Error'
        assert entity_result['errors'][0]['page_num'] == 2
    
    def test_process_single_entity_with_rate_limit_exceeded_raises_exception(self):
        """
        Test that exceeding weekly rate limit raises exception
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock rate limit check to return False
        self.rate_limit_tracker.check_weekly_limit.return_value = False
        
        # Act & Assert
        self.orchestrator.pass_load_id_to_components(load_id)
        with pytest.raises(Exception) as exc_info:
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        assert "Weekly API rate limit would be exceeded" in str(exc_info.value)
    
    def test_process_single_entity_with_existing_state_resumes_from_last_page(self):
        """
        Test that processing resumes from the last completed page when state exists
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock existing state
        mock_existing_state = Mock()
        mock_existing_state.completed = False
        mock_existing_state.last_page = 2
        mock_existing_state.pages_processed = 2
        mock_existing_state.total_results = 100
        self.state_manager.load_state.return_value = mock_existing_state
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 50, 'count': 25},  # Resume from page 3
                None  # No more pages
            ]
            mock_strategy.extract_total_results.return_value = 100
            mock_factory.create_strategy.return_value = mock_strategy
            
            # Mock HTTP response
            mock_response = Mock()
            mock_response.data = {'search-results': {'entry': []}}
            mock_response.timestamp = self.base_time
            mock_response.metadata = {}
            mock_response.log_data = {}
            mock_response.content_hash = 'test_hash'
            self.http_client.make_request.return_value = mock_response
            
            self.payload_validator.validate_response.return_value = True
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        entity_result = self.orchestrator.processing_results['entities'][0]
        assert entity_result['pages_processed'] == 3  # 2 existing + 1 new
        self.state_manager.update_page_progress.assert_called_with(entity_id, load_id, 3)
    
    def test_process_single_entity_with_cache_enabled_uses_cached_response(self):
        """
        Test that cached responses are used when cache is enabled
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Setup cache manager
        self.cache_manager.is_enabled.return_value = True
        mock_cached_response = Mock()
        mock_cached_response.data = {'search-results': {'entry': []}}
        mock_cached_response.timestamp = self.base_time
        mock_cached_response.metadata = {}
        mock_cached_response.log_data = {}
        mock_cached_response.content_hash = 'cached_hash'
        self.cache_manager.get_cached_response.return_value = mock_cached_response
        self.cache_manager.generate_cache_key.return_value = "test_cache_key"
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        self.state_manager.load_state.return_value = None
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 0, 'count': 25},
                None
            ]
            mock_strategy.extract_total_results.return_value = 25
            mock_factory.create_strategy.return_value = mock_strategy
            
            self.payload_validator.validate_response.return_value = True
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        self.cache_manager.get_cached_response.assert_called_once()
        self.http_client.make_request.assert_not_called()  # Should not make HTTP request
        self.database_manager.upsert_api_response.assert_called_once()
    
    def test_handle_partial_failure_records_error_and_marks_partial(self):
        """
        Test that partial failures are recorded and entity marked as partial
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        error = Exception("Partial processing error")
        
        # Setup processing results with entity
        self.orchestrator.current_load_id = load_id
        self.orchestrator.processing_results = {
            'entities': [
                {
                    'entity_id': entity_id,
                    'status': 'processing'
                }
            ]
        }
        
        # Act
        self.orchestrator.handle_partial_failure(entity_id, error)
        
        # Assert
        self.state_manager.log_processing_error.assert_called_once_with(
            entity_id, load_id, error, None
        )
        
        # Check entity status updated
        entity_result = self.orchestrator.processing_results['entities'][0]
        assert entity_result['status'] == 'partial'
    
    def test_calculate_processing_metrics_includes_orchestrator_specific_metrics(self):
        """
        Test that processing metrics include orchestrator-specific information
        """
        # Arrange
        processing_results = {
            'load_id': 'test_load_123',
            'data_source': 'scopus_search',
            'entities': []
        }
        
        # Mock manifest generator response
        mock_stats = {
            'total_entities': 0,
            'success_rate_percent': 0.0
        }
        self.manifest_generator.calculate_summary_statistics.return_value = mock_stats
        
        # Mock rate limit tracker
        self.rate_limit_tracker.get_current_usage.return_value = 150
        
        # Mock cache manager
        self.cache_manager.is_enabled.return_value = True
        
        # Act
        metrics = self.orchestrator.calculate_processing_metrics(processing_results)
        
        # Assert
        assert 'orchestrator_metrics' in metrics
        orchestrator_metrics = metrics['orchestrator_metrics']
        assert orchestrator_metrics['load_id'] == 'test_load_123'
        assert orchestrator_metrics['data_source'] == 'scopus_search'
        assert orchestrator_metrics['current_api_usage'] == 150
        assert orchestrator_metrics['cache_enabled'] is True
    
    def test_run_complete_processing_with_successful_entities_returns_complete_results(self):
        """
        Test that complete processing workflow returns full results and manifest
        """
        # Arrange
        entity_file_path = Path("test_entities.txt")
        data_source = "scopus_search"
        file_content = "author_001\nauthor_002\n"
        
        # Mock file operations
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                with patch.object(self.orchestrator, 'process_single_entity') as mock_process:
                    
                    # Mock configuration
                    mock_config = Mock()
                    mock_config.api.name = data_source
                    self.config_loader.get_config.return_value = mock_config
                    
                    # Mock state manager
                    self.state_manager.get_incomplete_entities.return_value = []
                    
                    # Mock manifest generation
                    mock_manifest = {
                        'manifest_id': 'test_manifest_123',
                        'load_id': 'test_load_456'
                    }
                    self.manifest_generator.generate_manifest_data.return_value = mock_manifest
                    
                    # Mock metrics calculation
                    mock_metrics = {
                        'success_rate_percent': 100.0,
                        'total_entities': 2
                    }
                    
                    with patch.object(self.orchestrator, 'calculate_processing_metrics', 
                                    return_value=mock_metrics):
                        
                        # Act
                        result = self.orchestrator.run_complete_processing(entity_file_path, data_source)
        
        # Assert
        assert 'processing_results' in result
        assert 'manifest' in result
        assert 'metrics' in result
        assert result['manifest'] == mock_manifest
        assert result['metrics'] == mock_metrics
        
        # Verify process_single_entity was called for each entity
        assert mock_process.call_count == 2
        
        # Verify cleanup methods called
        self.http_client.close_connection.assert_called_once()
        self.database_manager.close_connection.assert_called_once()
        self.cache_manager.release_lock.assert_called_once()
    
    def test_run_complete_processing_with_partial_failures_continues_processing(self):
        """
        Test that partial failures don't stop processing of remaining entities
        """
        # Arrange
        entity_file_path = Path("test_entities.txt")
        data_source = "scopus_search"
        file_content = "author_001\nauthor_002\nauthor_003\n"
        
        # Mock file operations
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                
                # Mock configuration
                mock_config = Mock()
                mock_config.api.name = data_source
                self.config_loader.get_config.return_value = mock_config
                
                # Mock state manager
                self.state_manager.get_incomplete_entities.return_value = []
                
                # Mock process_single_entity to fail on second entity
                def mock_process_side_effect(entity_id, load_id):
                    if entity_id == "author_002":
                        raise Exception("Processing failed")
                
                with patch.object(self.orchestrator, 'process_single_entity', 
                                side_effect=mock_process_side_effect) as mock_process:
                    with patch.object(self.orchestrator, 'handle_partial_failure') as mock_handle_failure:
                        
                        # Mock manifest and metrics
                        self.manifest_generator.generate_manifest_data.return_value = {}
                        
                        with patch.object(self.orchestrator, 'calculate_processing_metrics', 
                                        return_value={}):
                            
                            # Act
                            result = self.orchestrator.run_complete_processing(entity_file_path, data_source)
        
        # Assert
        # All three entities should be attempted
        assert mock_process.call_count == 3
        
        # handle_partial_failure should be called once for the failed entity
        mock_handle_failure.assert_called_once()
        
        # Processing should complete and return results
        assert 'processing_results' in result
        assert 'manifest' in result
        assert 'metrics' in result
    
    def test_run_complete_processing_with_incomplete_entities_processes_only_incomplete(self):
        """
        Test that only incomplete entities from previous runs are processed
        """
        # Arrange
        entity_file_path = Path("test_entities.txt")
        data_source = "scopus_search"
        file_content = "author_001\nauthor_002\nauthor_003\n"
        
        # Mock incomplete entities from state manager
        incomplete_entities = ["author_002", "author_003"]
        
        # Mock file operations
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                with patch.object(self.orchestrator, 'process_single_entity') as mock_process:
                    
                    # Mock configuration
                    mock_config = Mock()
                    mock_config.api.name = data_source
                    self.config_loader.get_config.return_value = mock_config
                    
                    # Mock state manager to return incomplete entities
                    self.state_manager.get_incomplete_entities.return_value = incomplete_entities
                    
                    # Mock manifest and metrics
                    self.manifest_generator.generate_manifest_data.return_value = {}
                    
                    with patch.object(self.orchestrator, 'calculate_processing_metrics', 
                                    return_value={}):
                        
                        # Act
                        result = self.orchestrator.run_complete_processing(entity_file_path, data_source)
        
        # Assert
        # Only incomplete entities should be processed
        assert mock_process.call_count == 2
        
        # Verify the correct entities were processed
        processed_entities = [call[0][0] for call in mock_process.call_args_list]
        assert "author_002" in processed_entities
        assert "author_003" in processed_entities
        assert "author_001" not in processed_entities
    
    def test_get_default_entity_file_path_returns_correct_path_for_data_source(self):
        """
        Test that default entity file path is constructed correctly based on data source
        """
        # Arrange
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        self.config_loader.get_config.return_value = mock_config
        
        # Act
        file_path = self.orchestrator.get_default_entity_file_path()
        
        # Assert
        assert file_path.name == "scopus_search_entities.txt"
        assert "data/input" in str(file_path)
    
    def test_create_sample_entity_file_creates_file_with_sample_data(self):
        """
        Test that sample entity file is created with provided sample entities
        """
        # Arrange
        sample_entities = ["author_001", "author_002", "author_003"]
        test_file_path = Path("/tmp/test_entities.txt")
        
        # Clean up any existing test file
        if test_file_path.exists():
            test_file_path.unlink()
        
        # Act
        self.orchestrator.create_sample_entity_file(test_file_path, sample_entities)
        
        # Assert
        assert test_file_path.exists()
        with open(test_file_path, 'r') as f:
            content = f.read().strip().split('\n')
        assert content == sample_entities
        
        # Clean up
        test_file_path.unlink()
    
    def test_run_processing_from_default_location_with_existing_file_processes_entities(self):
        """
        Test that processing from default location works with existing entity file
        """
        # Arrange
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        self.config_loader.get_config.return_value = mock_config
        
        file_content = "author_001\nauthor_002\n"
        
        with patch.object(self.orchestrator, 'get_default_entity_file_path') as mock_get_path:
            mock_file_path = Mock()
            mock_file_path.exists.return_value = True
            mock_get_path.return_value = mock_file_path
            
            with patch("builtins.open", mock_open(read_data=file_content)):
                with patch.object(self.orchestrator, 'run_complete_processing') as mock_run:
                    mock_run.return_value = {"test": "result"}
                    
                    # Act
                    result = self.orchestrator.run_processing_from_default_location()
        
        # Assert
        mock_run.assert_called_once_with(mock_file_path, "scopus_search")
        assert result == {"test": "result"}
    
    def test_run_processing_from_default_location_with_create_sample_creates_file(self):
        """
        Test that processing from default location creates sample file when requested
        """
        # Arrange
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        self.config_loader.get_config.return_value = mock_config
        
        with patch.object(self.orchestrator, 'get_default_entity_file_path') as mock_get_path:
            mock_file_path = Mock()
            mock_file_path.exists.return_value = False
            mock_get_path.return_value = mock_file_path
            
            with patch.object(self.orchestrator, 'create_sample_entity_file') as mock_create:
                with patch.object(self.orchestrator, 'run_complete_processing') as mock_run:
                    mock_run.return_value = {"test": "result"}
                    
                    # Act
                    result = self.orchestrator.run_processing_from_default_location(create_sample=True)
        
        # Assert
        mock_create.assert_called_once()
        # Verify sample entities are default Scopus author IDs
        call_args = mock_create.call_args
        assert call_args[0][0] == mock_file_path  # file path
        assert "7004212771" in call_args[0][1]  # sample entities list
        
        mock_run.assert_called_once_with(mock_file_path, "scopus_search")
    
    def test_run_processing_from_default_location_without_file_raises_error(self):
        """
        Test that processing from default location raises error when file doesn't exist and create_sample=False
        """
        # Arrange
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        self.config_loader.get_config.return_value = mock_config
        
        with patch.object(self.orchestrator, 'get_default_entity_file_path') as mock_get_path:
            mock_file_path = Mock()
            mock_file_path.exists.return_value = False
            mock_get_path.return_value = mock_file_path
            
            # Act & Assert
            with pytest.raises(FileNotFoundError) as exc_info:
                self.orchestrator.run_processing_from_default_location(create_sample=False)
            
            assert "Entity file not found" in str(exc_info.value)
            assert "create_sample=True" in str(exc_info.value)
    
    def test_build_api_request_creates_correct_scopus_query_format(self):
        """
        Test that API requests are built with correct Scopus query format
        """
        # Arrange
        entity_id = "7004212771"
        page_params = {"start": 0, "count": 25}
        
        mock_config = Mock()
        mock_config.api.name = "scopus_search"
        mock_config.api.base_url = "https://api.elsevier.com/content/search/scopus"
        mock_config.default_parameters = {"view": "COMPLETE", "httpAccept": "application/json"}
        self.config_loader.get_config.return_value = mock_config
        
        # Act
        request = self.orchestrator._build_api_request(entity_id, page_params)
        
        # Assert
        assert request.url == "https://api.elsevier.com/content/search/scopus"
        assert request.params['query'] == "AU-ID(7004212771)"
        assert request.params['start'] == 0
        assert request.params['count'] == 25
        assert request.params['view'] == "COMPLETE"
        assert request.entity_id == entity_id
    
    def test_build_api_request_creates_generic_query_for_other_apis(self):
        """
        Test that API requests use entity_id directly for non-Scopus APIs
        """
        # Arrange
        entity_id = "institution_12345"
        page_params = {"page": 1, "size": 50}
        
        mock_config = Mock()
        mock_config.api.name = "scival_metrics"
        mock_config.api.base_url = "https://api.scival.com/institutions"
        mock_config.default_parameters = {"format": "json"}
        self.config_loader.get_config.return_value = mock_config
        
        # Act
        request = self.orchestrator._build_api_request(entity_id, page_params)
        
        # Assert
        assert request.params['query'] == "institution_12345"  # Direct entity_id, not wrapped
        assert request.params['page'] == 1
        assert request.params['size'] == 50
        assert request.params['format'] == "json"
    
    def test_should_stop_processing_returns_true_for_critical_errors(self):
        """
        Test that critical errors cause processing to stop
        """
        # Arrange & Act & Assert
        
        # Rate limit errors should stop
        rate_limit_error = Exception("Rate limit exceeded")
        assert self.orchestrator._should_stop_processing(rate_limit_error) is True
        
        # Authentication errors should stop
        auth_error = Exception("Authentication failed")
        assert self.orchestrator._should_stop_processing(auth_error) is True
        
        # 401 errors should stop
        http_401_error = Exception("HTTP 401 Unauthorized")
        assert self.orchestrator._should_stop_processing(http_401_error) is True
    
    def test_should_stop_processing_returns_false_for_transient_errors(self):
        """
        Test that transient errors allow processing to continue
        """
        # Arrange & Act & Assert
        
        # Network timeouts should continue
        timeout_error = Exception("Request timeout")
        assert self.orchestrator._should_stop_processing(timeout_error) is False
        
        # Generic API errors should continue
        api_error = Exception("Server temporarily unavailable")
        assert self.orchestrator._should_stop_processing(api_error) is False
        
        # Validation errors should continue
        validation_error = Exception("Response validation failed")
        assert self.orchestrator._should_stop_processing(validation_error) is False
    
    def test_should_stop_processing_handles_error_type_names(self):
        """
        Test that specific error type names are handled correctly
        """
        # Arrange
        class RateLimitError(Exception):
            pass
        
        class AuthenticationError(Exception):
            pass
        
        class PermanentAPIError(Exception):
            pass
        
        class TemporaryError(Exception):
            pass
        
        # Act & Assert
        assert self.orchestrator._should_stop_processing(RateLimitError("test")) is True
        assert self.orchestrator._should_stop_processing(AuthenticationError("test")) is True
        assert self.orchestrator._should_stop_processing(PermanentAPIError("test")) is True
        assert self.orchestrator._should_stop_processing(TemporaryError("test")) is False
    
    def test_process_single_entity_tracks_processed_items_in_state(self):
        """
        Test that individual items (DOIs) are tracked in state management
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        # Mock state manager
        self.state_manager.load_state.return_value = None
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 0, 'count': 25},
                None
            ]
            mock_strategy.extract_total_results.return_value = 50
            mock_factory.create_strategy.return_value = mock_strategy
            
            # Mock HTTP response with DOI data
            mock_response = Mock()
            mock_response.data = {
                'search-results': {
                    'entry': [
                        {'prism:doi': '10.1000/test1', 'dc:title': 'Test Paper 1'},
                        {'prism:doi': '10.1000/test2', 'dc:title': 'Test Paper 2'},
                        {'dc:identifier': 'SCOPUS_ID:123', 'dc:title': 'Test Paper 3'}  # No DOI, has identifier
                    ]
                }
            }
            mock_response.timestamp = self.base_time
            mock_response.metadata = {}
            mock_response.log_data = {}
            mock_response.content_hash = 'test_hash'
            self.http_client.make_request.return_value = mock_response
            
            self.payload_validator.validate_response.return_value = True
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        # Verify processed items were tracked
        expected_calls = [
            ((entity_id, load_id, '10.1000/test1'),),
            ((entity_id, load_id, '10.1000/test2'),),
            ((entity_id, load_id, 'SCOPUS_ID:123'),)
        ]
        
        actual_calls = self.state_manager.add_processed_item.call_args_list
        assert len(actual_calls) == 3
        
        # Verify file hash was recorded
        self.state_manager.record_file_hash.assert_called_once()
        hash_call_args = self.state_manager.record_file_hash.call_args[0]
        assert hash_call_args[0] == entity_id
        assert hash_call_args[1] == load_id
        assert "scopus_search_author_author_001_page1" in hash_call_args[2]  # filename pattern
        assert hash_call_args[3] == 'test_hash'
    
    def test_process_single_entity_with_validation_failure_logs_error(self):
        """
        Test that payload validation failures are properly logged and handled
        """
        # Arrange
        entity_id = "author_001"
        load_id = "test_load_123"
        
        # Mock configuration
        mock_config = Mock()
        mock_config.pagination.strategy = "offset_based"
        mock_config.payload_validation = {}
        self.config_loader.get_config.return_value = mock_config
        
        self.state_manager.load_state.return_value = None
        self.rate_limit_tracker.check_weekly_limit.return_value = True
        
        # Mock pagination strategy
        with patch('api_adapter.api_orchestrator.PaginationFactory') as mock_factory:
            mock_strategy = Mock()
            mock_strategy.get_next_page_params.side_effect = [
                {'start': 0, 'count': 25},
                None
            ]
            mock_strategy.extract_total_results.return_value = 50
            mock_factory.create_strategy.return_value = mock_strategy
            
            # Mock HTTP response
            mock_response = Mock()
            mock_response.data = {'invalid': 'response'}
            mock_response.timestamp = self.base_time
            mock_response.metadata = {}
            mock_response.log_data = {}
            mock_response.content_hash = 'test_hash'
            self.http_client.make_request.return_value = mock_response
            
            # Mock validation failure
            self.payload_validator.validate_response.return_value = False
            
            # Act
            self.orchestrator.pass_load_id_to_components(load_id)
            self.orchestrator.process_single_entity(entity_id, load_id)
        
        # Assert
        # Verify error was logged in state
        self.state_manager.log_processing_error.assert_called()
        error_call = self.state_manager.log_processing_error.call_args[0]
        assert error_call[0] == entity_id
        assert error_call[1] == load_id
        assert "validation failed" in str(error_call[2]).lower()
        assert error_call[3] == 1  # page number
        
        # Verify entity is marked as having errors
        entity_result = self.orchestrator.processing_results['entities'][0]
        assert len(entity_result['errors']) >= 1
        assert entity_result['status'] == 'completed'  # Still completed despite error
    
    def test_complete_workflow_integration_with_database_operations(self):
        """
        Test the complete workflow integration ensuring all database operations are called
        """
        # Arrange
        entity_file_path = Path("test_entities.txt")
        data_source = "scopus_search"
        file_content = "author_001\n"
        
        # Mock all required components
        mock_config = Mock()
        mock_config.api.name = data_source
        self.config_loader.get_config.return_value = mock_config
        
        # Mock file operations
        with patch("builtins.open", mock_open(read_data=file_content)):
            with patch.object(Path, "exists", return_value=True):
                # Mock state manager
                self.state_manager.get_incomplete_entities.return_value = []
                
                # Mock successful processing
                with patch.object(self.orchestrator, 'process_single_entity') as mock_process:
                    
                    # Mock manifest generation
                    mock_manifest = {
                        'manifest_id': 'test_manifest_123',
                        'load_id': 'test_load_456',
                        'processing_summary': {'total_entities': 1}
                    }
                    self.manifest_generator.generate_manifest_data.return_value = mock_manifest
                    
                    # Mock metrics calculation
                    mock_metrics = {
                        'success_rate_percent': 100.0,
                        'total_entities': 1,
                        'orchestrator_metrics': {
                            'load_id': 'test_load_456',
                            'current_api_usage': 50
                        }
                    }
                    
                    with patch.object(self.orchestrator, 'calculate_processing_metrics', 
                                    return_value=mock_metrics):
                        
                        # Act
                        result = self.orchestrator.run_complete_processing(entity_file_path, data_source)
        
        # Assert all database operations were called
        
        # 1. Verify manifest was stored in database
        self.database_manager.insert_manifest_data.assert_called_once_with(mock_manifest)
        
        # 2. Verify processing was attempted
        mock_process.assert_called_once()
        
        # 3. Verify cleanup was called
        self.http_client.close_connection.assert_called_once()
        self.database_manager.close_connection.assert_called_once()
        self.cache_manager.release_lock.assert_called_once()
        
        # 4. Verify result structure
        assert 'processing_results' in result
        assert 'manifest' in result
        assert 'metrics' in result
        assert result['manifest'] == mock_manifest
        assert result['metrics'] == mock_metrics
        
        # 5. Verify processing results were populated
        processing_results = result['processing_results']
        assert processing_results['data_source'] == data_source
        assert 'start_time' in processing_results
        assert 'end_time' in processing_results
        assert 'entities' in processing_results