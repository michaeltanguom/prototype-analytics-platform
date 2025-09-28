"""
APIOrchestrator module for high-level processing coordination
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
import uuid

from .config_loader import ConfigLoader
from .database_manager import DatabaseManager
from .state_manager import StateManager
from .http_client import HTTPClient
from .payload_validator import PayloadValidator
from .pagination_strategy import PaginationFactory
from .manifest_generator import ManifestGenerator
from .recovery_manager import RecoveryManager
from .rate_limit_tracker import RateLimitTracker
from .cache_manager import CacheManager


class APIOrchestrator:
    """
    High-level coordinator for API data ingestion workflow
    
    Orchestrates the complete process of:
    1. Loading entity lists from files
    2. Processing entities with pagination
    3. Handling failures and recovery
    4. Generating processing manifests
    """
    
    def __init__(
        self,
        config_loader: ConfigLoader,
        database_manager: DatabaseManager,
        state_manager: StateManager,
        http_client: HTTPClient,
        payload_validator: PayloadValidator,
        manifest_generator: ManifestGenerator,
        recovery_manager: RecoveryManager,
        rate_limit_tracker: RateLimitTracker,
        cache_manager: Optional[CacheManager] = None
    ):
        """
        Initialise APIOrchestrator with dependency injection
        
        Args:
            config_loader: Configuration management component
            database_manager: Database operations component
            state_manager: Processing state persistence component
            http_client: HTTP communication component
            payload_validator: Response validation component
            manifest_generator: Processing summary component
            recovery_manager: Failure recovery component
            rate_limit_tracker: API usage tracking component
            cache_manager: Optional development caching component
        """
        self.config_loader = config_loader
        self.database_manager = database_manager
        self.state_manager = state_manager
        self.http_client = http_client
        self.payload_validator = payload_validator
        self.manifest_generator = manifest_generator
        self.recovery_manager = recovery_manager
        self.rate_limit_tracker = rate_limit_tracker
        self.cache_manager = cache_manager
        
        self.logger = logging.getLogger(__name__)
        self.current_load_id: Optional[str] = None
        self.processing_results: Dict[str, Any] = {}
    
    def load_entities_from_file(self, file_path: Path) -> List[str]:
        """
        Load entity IDs from input file
        
        Args:
            file_path: Path to file containing entity IDs (one per line)
            
        Returns:
            List of entity ID strings
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            ValueError: If the file is empty or contains invalid data
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Entity file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                entities = [line.strip() for line in file if line.strip()]
            
            if not entities:
                raise ValueError(f"Entity file is empty: {file_path}")
            
            self.logger.info(f"Loaded {len(entities)} entities from {file_path}")
            return entities
            
        except Exception as e:
            self.logger.error(f"Failed to load entities from {file_path}: {e}")
            raise
    
    def generate_load_id(self, data_source: str) -> str:
        """
        Generate unique load identifier for this processing run
        
        Args:
            data_source: Name of the data source being processed
            
        Returns:
            Unique load identifier combining source and timestamp
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8]
        load_id = f"{data_source}_{timestamp}_{unique_suffix}"
        
        self.logger.info(f"Generated load ID: {load_id}")
        return load_id
    
    def pass_load_id_to_components(self, load_id: str) -> None:
        """
        Pass load ID to all components that need it
        
        Args:
            load_id: Load identifier to distribute to components
        """
        self.current_load_id = load_id
        
        # Initialise processing results tracking
        self.processing_results = {
            'load_id': load_id,
            'data_source': self.config_loader.get_config().api.name,
            'start_time': datetime.now(timezone.utc),
            'end_time': None,
            'entities': []
        }
        
        self.logger.debug(f"Distributed load ID {load_id} to orchestrator components")
    
    def process_single_entity(self, entity_id: str, load_id: str) -> None:
        """
        Process a single entity with complete pagination
        
        Args:
            entity_id: Identifier for the entity to process
            load_id: Load identifier for this processing run
            
        Raises:
            Exception: Re-raises any unexpected processing errors after logging
        """
        entity_start_time = datetime.now(timezone.utc)
        entity_result = {
            'entity_id': entity_id,
            'status': 'processing',
            'pages_processed': 0,
            'total_results': 0,
            'errors': [],
            'start_time': entity_start_time,
            'end_time': None
        }
        
        try:
            self.logger.info(f"Starting processing for entity: {entity_id}")
            
            # Check if we need to resume from previous processing
            existing_state = self.state_manager.load_state(entity_id, load_id)
            if existing_state and not existing_state.completed:
                self.logger.info(f"Resuming processing for entity {entity_id} from page {existing_state.last_page + 1}")
                entity_result['pages_processed'] = existing_state.pages_processed
            
            # Check weekly rate limits before processing
            if not self.rate_limit_tracker.check_weekly_limit(self.processing_results['data_source']):
                raise Exception("Weekly API rate limit would be exceeded")
            
            # Get pagination strategy from configuration
            config = self.config_loader.get_config()
            pagination_strategy = PaginationFactory.create_strategy(
                config.pagination.strategy,
                config.pagination
            )
            
            page_num = existing_state.last_page + 1 if existing_state else 1
            total_results = existing_state.total_results if existing_state else None
            
            # Process pages until completion
            while True:
                try:
                    # Apply rate limiting
                    self.http_client.apply_rate_limit()
                    
                    # Get pagination parameters
                    page_params = pagination_strategy.get_next_page_params(
                        page_num, total_results
                    )
                    
                    if page_params is None:
                        self.logger.info(f"No more pages for entity {entity_id}")
                        break
                    
                    # Build API request
                    api_request = self._build_api_request(entity_id, page_params)
                    
                    # Check cache if enabled
                    response = None
                    if self.cache_manager and self.cache_manager.is_enabled():
                        cache_key = self.cache_manager.generate_cache_key(api_request)
                        cached_response = self.cache_manager.get_cached_response(cache_key)
                        if cached_response:
                            self.logger.debug(f"Using cached response for entity {entity_id}, page {page_num}")
                            response = cached_response
                        else:
                            response = self.http_client.make_request(api_request)
                            self.cache_manager.store_response(cache_key, response)
                    else:
                        response = self.http_client.make_request(api_request)
                    
                    # Track the API request
                    self.rate_limit_tracker.track_request(
                        self.processing_results['data_source'],
                        datetime.now(timezone.utc)
                    )
                    
                    # Validate response
                    if not self.payload_validator.validate_response(response, config.payload_validation):
                        raise Exception(f"Response validation failed for entity {entity_id}, page {page_num}")
                    
                    # Extract total results on first page
                    if total_results is None:
                        total_results = pagination_strategy.extract_total_results(response.data)
                        entity_result['total_results'] = total_results or 0
                    
                    # Store response in api_ingestion.db → api_responses table
                    self.database_manager.upsert_api_response({
                        'load_id': load_id,
                        'data_source': self.processing_results['data_source'],
                        'entity_id': entity_id,
                        'page_number': page_num,
                        'request_timestamp': api_request.timestamp,
                        'response_timestamp': response.timestamp,
                        'raw_response': response.data,
                        'technical_metadata': response.metadata,
                        'log_entry': response.log_data,
                        'file_hash': response.content_hash
                    })
                    
                    # Update processing state in api_state_management.db → processing_state table
                    self.state_manager.update_page_progress(entity_id, load_id, page_num)
                    
                    # Extract and track processed items for state management
                    items = self._extract_items_from_response(response.data)
                    for item in items:
                        item_id = item.get('prism:doi') or item.get('dc:identifier', '')
                        if item_id:
                            self.state_manager.add_processed_item(entity_id, load_id, item_id)
                    
                    # Record file hash for deduplication in state
                    filename = f"{self.processing_results['data_source']}_author_{entity_id}_page{page_num}_{datetime.now().strftime('%Y%m%d')}.json"
                    self.state_manager.record_file_hash(entity_id, load_id, filename, response.content_hash)
                    entity_result['pages_processed'] += 1
                    
                    self.logger.info(f"Processed page {page_num} for entity {entity_id}")
                    page_num += 1
                    
                except Exception as page_error:
                    # Log page-level error and add to entity errors
                    error_entry = {
                        'type': type(page_error).__name__,
                        'message': str(page_error),
                        'page_num': page_num
                    }
                    entity_result['errors'].append(error_entry)
                    
                    self.state_manager.log_processing_error(
                        entity_id, load_id, page_error, page_num
                    )
                    
                    self.logger.error(f"Error processing page {page_num} for entity {entity_id}: {page_error}")
                    
                    # Determine if we should continue or stop
                    if self._should_stop_processing(page_error):
                        entity_result['status'] = 'failed'
                        break
                    else:
                        # Skip this page and continue
                        page_num += 1
                        continue
            
            # Mark entity as completed if no critical failures
            if entity_result['status'] != 'failed':
                self.state_manager.mark_completed(entity_id, load_id)
                entity_result['status'] = 'completed'
                self.logger.info(f"Successfully completed processing for entity {entity_id}")
            
        except Exception as entity_error:
            entity_result['status'] = 'failed'
            entity_result['errors'].append({
                'type': type(entity_error).__name__,
                'message': str(entity_error),
                'page_num': None
            })
            self.logger.error(f"Entity processing failed for {entity_id}: {entity_error}")
            raise
        
        finally:
            entity_result['end_time'] = datetime.now(timezone.utc)
            self.processing_results['entities'].append(entity_result)
    
    def _extract_items_from_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Safely extract items from response data
        
        Args:
            response_data: Raw response data from API
            
        Returns:
            List of items from the response
        """
        try:
            # Handle nested structure safely
            search_results = response_data.get('search-results', {})
            if hasattr(search_results, 'get'):
                return search_results.get('entry', [])
            else:
                # If search_results is not a dict, return empty list
                return []
        except (AttributeError, TypeError):
            # Handle cases where response_data structure is unexpected
            return []
    
    def handle_partial_failure(self, entity_id: str, error: Exception) -> None:
        """
        Handle partial failure of entity processing
        
        Args:
            entity_id: Identifier of the failed entity
            error: Exception that caused the failure
        """
        self.logger.warning(f"Handling partial failure for entity {entity_id}: {error}")
        
        # Record failure for recovery
        if self.current_load_id:
            self.state_manager.log_processing_error(
                entity_id, self.current_load_id, error, None
            )
        
        # Update processing results to reflect partial failure
        for entity_result in self.processing_results.get('entities', []):
            if entity_result['entity_id'] == entity_id:
                entity_result['status'] = 'partial'
                break
        
        self.logger.info(f"Marked entity {entity_id} as partial failure - processing will continue")
    
    def calculate_processing_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate and return processing metrics
        
        Args:
            results: Processing results dictionary
            
        Returns:
            Dictionary containing calculated metrics
        """
        metrics = self.manifest_generator.calculate_summary_statistics(results)
        
        # Add orchestrator-specific metrics
        metrics['orchestrator_metrics'] = {
            'load_id': results.get('load_id'),
            'data_source': results.get('data_source'),
            'current_api_usage': self.rate_limit_tracker.get_current_usage(
                results.get('data_source', '')
            ),
            'cache_enabled': self.cache_manager.is_enabled() if self.cache_manager else False
        }
        
        return metrics
    
    def run_complete_processing(self, entity_file_path: Path, data_source: str) -> Dict[str, Any]:
        """
        Run complete processing workflow for all entities
        
        Args:
            entity_file_path: Path to file containing entity IDs
            data_source: Name of the data source being processed
            
        Returns:
            Complete processing results and manifest
        """
        try:
            # Load entities and generate load ID
            entities = self.load_entities_from_file(entity_file_path)
            load_id = self.generate_load_id(data_source)
            self.pass_load_id_to_components(load_id)
            
            self.logger.info(f"Starting processing run {load_id} for {len(entities)} entities")
            
            # Check for incomplete entities from previous runs
            incomplete_entities = self.state_manager.get_incomplete_entities(entities, load_id)
            if incomplete_entities:
                self.logger.info(f"Found {len(incomplete_entities)} incomplete entities from previous runs")
                entities = incomplete_entities
            
            # Process each entity
            for entity_id in entities:
                try:
                    self.process_single_entity(entity_id, load_id)
                except Exception as e:
                    self.handle_partial_failure(entity_id, e)
                    # Continue with next entity
                    continue
            
            # Finalise processing
            self.processing_results['end_time'] = datetime.now(timezone.utc)
            
            # Generate manifest and store in api_ingestion.db → processing_manifests table
            manifest_data = self.manifest_generator.generate_manifest_data(self.processing_results)
            self.database_manager.insert_manifest_data(manifest_data)
            
            # Calculate final metrics
            final_metrics = self.calculate_processing_metrics(self.processing_results)
            
            self.logger.info(f"Processing completed for load {load_id}")
            
            # Safely access success_rate_percent
            success_rate = final_metrics.get('success_rate_percent', 0.0)
            self.logger.info(f"Final metrics: {success_rate:.1f}% success rate")
            
            return {
                'processing_results': self.processing_results,
                'manifest': manifest_data,
                'metrics': final_metrics
            }
            
        except Exception as e:
            self.logger.error(f"Processing run failed: {e}")
            raise
        finally:
            # Cleanup resources
            self.http_client.close_connection()
            self.database_manager.close_connection()
            if self.cache_manager:
                self.cache_manager.release_lock()
    
    def _build_api_request(self, entity_id: str, page_params: Dict[str, Any]) -> 'APIRequest':
        """
        Build API request object with entity and pagination parameters
        
        Args:
            entity_id: Author/entity identifier
            page_params: Pagination parameters from strategy
            
        Returns:
            APIRequest object with complete request details
        """
        config = self.config_loader.get_config()
        
        # Build query based on data source
        if config.api.name == "scopus_search":
            search_query = f"AU-ID({entity_id})"
        else:
            # For other APIs, use entity_id directly
            search_query = entity_id
        
        # Build URL with query and pagination
        base_url = config.api.base_url
        
        # Safely handle default_parameters
        default_params = {}
        if hasattr(config, 'default_parameters') and config.default_parameters:
            try:
                # Handle case where default_parameters is a real dict
                if hasattr(config.default_parameters, 'items'):
                    default_params = dict(config.default_parameters.items())
                elif hasattr(config.default_parameters, '__iter__'):
                    # Handle other iterable cases safely
                    default_params = dict(config.default_parameters)
            except (TypeError, AttributeError):
                # If default_parameters is a Mock or non-iterable, use empty dict
                default_params = {}
        
        params = {
            'query': search_query,
            **default_params,
            **page_params
        }
        
        # Create APIRequest object (simplified structure)
        from types import SimpleNamespace
        request = SimpleNamespace()
        request.url = base_url
        request.params = params
        request.headers = {}
        request.timestamp = datetime.now(timezone.utc)
        request.entity_id = entity_id
        request.page_params = page_params
        
        return request
    
    def _should_stop_processing(self, error: Exception) -> bool:
        """
        Determine if processing should stop based on error type
        
        Args:
            error: Exception that occurred during processing
            
        Returns:
            bool: True if processing should stop, False to continue
        """
        # Stop on critical errors that indicate systemic issues
        critical_error_types = [
            'RateLimitError', 
            'AuthenticationError', 
            'PermanentAPIError',
            'ConfigurationError'
        ]
        
        error_type = type(error).__name__
        
        # Check for rate limit exceeded message
        if "rate limit" in str(error).lower():
            return True
            
        # Check for authentication failures
        if "auth" in str(error).lower() or "401" in str(error):
            return True
            
        return error_type in critical_error_types
    
    def get_default_entity_file_path(self) -> Path:
        """
        Get the default path for entity input file based on data source
        
        Returns:
            Path: Default file path for entity list
        """
        # Entity files are still file-based as they are input to the system
        # But processed data goes to databases
        config = self.config_loader.get_config()
        data_source = config.api.name
        
        # Get the api directory (parent of src)
        api_root = Path(__file__).parent.parent.parent
        input_dir = api_root / "data" / "input"
        
        # Create input directory if it doesn't exist
        input_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename based on data source
        filename = f"{data_source}_entities.txt"
        return input_dir / filename
    
    def create_sample_entity_file(self, file_path: Path, sample_entities: List[str]) -> None:
        """
        Create a sample entity file for testing
        
        Args:
            file_path: Path where to create the file
            sample_entities: List of sample entity IDs
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for entity in sample_entities:
                    f.write(f"{entity}\n")
            
            self.logger.info(f"Created sample entity file with {len(sample_entities)} entities at {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create sample entity file: {e}")
            raise
    
    def run_processing_from_default_location(self, create_sample: bool = False) -> Dict[str, Any]:
        """
        Run processing using default file locations
        
        Args:
            create_sample: If True, creates a sample entity file if none exists
            
        Returns:
            Complete processing results and manifest
        """
        config = self.config_loader.get_config()
        data_source = config.api.name
        
        # Get default file path
        entity_file_path = self.get_default_entity_file_path()
        
        # Create sample file if requested and file doesn't exist
        if create_sample and not entity_file_path.exists():
            sample_entities = ["7004212771", "7005400538", "24726138500"]  # Sample Scopus author IDs
            self.create_sample_entity_file(entity_file_path, sample_entities)
        
        # Check if file exists
        if not entity_file_path.exists():
            raise FileNotFoundError(
                f"Entity file not found at {entity_file_path}. "
                f"Please create this file with entity IDs (one per line) or use create_sample=True"
            )
        
        return self.run_complete_processing(entity_file_path, data_source)
