"""
Prefect Orchestration for Generic REST API Adapter
Converts the APIOrchestrator into Prefect tasks and flows for simplified execution

python prefect_api_orchestrator.py --config configs/scopus_search.toml --entities data/input/scopus_search_entities.txt --run
"""

import sys
import time
import uuid
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import traceback

# Prefect imports
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# Add current directory to path for proper imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from api_adapter.config_loader import ConfigLoader, ConfigurationError, EnvironmentError
from api_adapter.database_manager import DatabaseManager, DatabaseConnectionError
from api_adapter.state_manager import StateManager, ProcessingState
from api_adapter.manifest_generator import ManifestGenerator
from api_adapter.rate_limit_tracker import RateLimitTracker
from api_adapter.file_hasher import FileHasher
from api_adapter.http_client import HTTPClient, APIRequest, APIResponse, PermanentAPIError
from api_adapter.payload_validator import PayloadValidator
from api_adapter.pagination_strategy import PaginationFactory
from api_adapter.cache_manager import CacheManager

# ===================================================================
# PREFECT TASKS - Individual components converted to tasks
# ===================================================================

@task(
    name="validate_configuration_and_environment",
    description="Validate TOML configuration and environment variables",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    retries=0  # Configuration validation should not retry
)
def validate_configuration_and_environment(config_path: str) -> Dict[str, Any]:
    """
    Validate TOML configuration and environment variables
    
    Args:
        config_path: Path to TOML configuration file
        
    Returns:
        Validation results and config summary
    """
    logger = get_run_logger()
    logger.info(f"Validating configuration: {config_path}")
    
    try:
        # Load and validate TOML configuration
        config = ConfigLoader.load_toml_config(Path(config_path))
        
        # Validate environment variables
        ConfigLoader.validate_environment_variables(config)
        
        logger.info("Configuration and environment validation passed")
        return {
            'status': 'valid',
            'config_summary': {
                'api_name': config.name,
                'base_url': config.base_url,
                'pagination_strategy': config.pagination.get('strategy', 'unknown'),
                'rate_limit': config.rate_limits.get('requests_per_second', 1.0)
            }
        }
        
    except (ConfigurationError, EnvironmentError) as e:
        logger.error(f"Configuration validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected configuration error: {e}")
        raise ConfigurationError(f"Configuration validation failed: {e}")


@task(
    name="initialise_database_connections",
    description="Create and initialise database connections with schema",
    retries=1,
    retry_delay_seconds=10
)
def initialise_database_connections(config_path: str) -> Dict[str, Any]:
    """
    Create database connections and initialise schema
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Database initialisation results
    """
    logger = get_run_logger()
    logger.info("Initialising database connections")
    
    try:
        config = ConfigLoader.load_toml_config(Path(config_path))
        
        # Create database manager and connections
        db_manager = DatabaseManager()
        
        # Create connections for both databases
        api_data_db = Path(f"data/databases/{config.name}_api_ingestion.db")
        state_db = Path(f"data/databases/api_state_management.db")
        
        # Ensure database directories exist
        api_data_db.parent.mkdir(parents=True, exist_ok=True)
        state_db.parent.mkdir(parents=True, exist_ok=True)
        
        # Create main data connection and schema
        data_connection = db_manager.create_connection(api_data_db)
        db_manager.create_tables()
        db_manager.close_connection()
        
        # Create state connection and schema  
        state_connection = db_manager.create_connection(state_db)
        db_manager.create_tables()
        db_manager.close_connection()
        
        logger.info(f"Database connections initialised successfully")
        logger.info(f"API data database: {api_data_db}")
        logger.info(f"State management database: {state_db}")
        
        return {
            'status': 'initialised',
            'api_data_db_path': str(api_data_db),
            'state_db_path': str(state_db),
            'data_source': config.name
        }
        
    except DatabaseConnectionError as e:
        logger.error(f"Database initialisation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected database error: {e}")
        raise DatabaseConnectionError(f"Database initialisation failed: {e}")


@task(
    name="load_entity_list",
    description="Load entity IDs from input file",
    retries=0  # File loading should not retry
)
def load_entity_list(entity_file_path: str) -> List[str]:
    """
    Load entity IDs from input file
    
    Args:
        entity_file_path: Path to file containing entity IDs (one per line)
        
    Returns:
        List of entity ID strings
    """
    logger = get_run_logger()
    logger.info(f"Loading entities from: {entity_file_path}")
    
    file_path = Path(entity_file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Entity file not found: {entity_file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            entities = [line.strip() for line in file if line.strip()]
        
        if not entities:
            raise ValueError(f"Entity file is empty: {entity_file_path}")
        
        logger.info(f"Loaded {len(entities)} entities")
        return entities
        
    except Exception as e:
        logger.error(f"Failed to load entities: {e}")
        raise


@task(
    name="generate_load_identifier",
    description="Generate unique load identifier for this processing run",
    retries=0
)
def generate_load_identifier(data_source: str) -> str:
    """
    Generate unique load identifier for this processing run
    
    Args:
        data_source: Name of the data source being processed
        
    Returns:
        Unique load identifier
    """
    logger = get_run_logger()
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    load_id = f"{data_source}_{timestamp}_{unique_suffix}"
    
    logger.info(f"Generated load ID: {load_id}")
    return load_id


@task(
    name="check_incomplete_entities",
    description="Identify entities that need processing based on state",
    retries=1,
    retry_delay_seconds=5
)
def check_incomplete_entities(entities: List[str], load_id: str, 
                            state_db_path: str) -> List[str]:
    """
    Check which entities need processing (incomplete or failed)
    
    Args:
        entities: List of all entity IDs
        load_id: Load identifier
        state_db_path: Path to state database
        
    Returns:
        List of entities that need processing
    """
    logger = get_run_logger()
    logger.info(f"Checking for incomplete entities from previous runs")
    
    try:
        # Create database manager and state manager
        db_manager = DatabaseManager()
        db_manager.create_connection(Path(state_db_path))
        db_manager.create_tables()  # Ensure tables exist
        
        state_manager = StateManager(db_manager)
        
        # Get incomplete entities
        incomplete_entities = state_manager.get_incomplete_entities(entities, load_id)
        
        db_manager.close_connection()
        
        if incomplete_entities:
            logger.info(f"Found {len(incomplete_entities)} incomplete entities from previous runs")
            logger.info(f"Will process: {incomplete_entities[:5]}..." if len(incomplete_entities) > 5 else f"Will process: {incomplete_entities}")
            return incomplete_entities
        else:
            logger.info("No incomplete entities found, processing all entities")
            return entities
        
    except Exception as e:
        logger.error(f"Error checking incomplete entities: {e}")
        # On error, process all entities to be safe
        return entities


@task(
    name="process_single_entity",
    description="Process a single entity with complete pagination and state management",
    retries=2,
    retry_delay_seconds=60  # Wait longer for entity-level retries
)
def process_single_entity(entity_id: str, config_path: str, load_id: str,
                        api_data_db_path: str, state_db_path: str,
                        enable_cache: bool = False) -> Dict[str, Any]:
    """
    Process a single entity with complete pagination
    
    Args:
        entity_id: Identifier for the entity to process
        config_path: Path to configuration file
        load_id: Load identifier
        api_data_db_path: Path to API data database
        state_db_path: Path to state database
        enable_cache: Whether to enable caching (development only)
        
    Returns:
        Processing results for this entity
    """
    logger = get_run_logger()
    entity_start_time = datetime.now(timezone.utc)
    logger.info(f"Processing entity: {entity_id}")
    
    entity_result = {
        'entity_id': entity_id,
        'status': 'processing',
        'pages_processed': 0,
        'total_results': 0,
        'errors': [],
        'start_time': entity_start_time,
        'end_time': None
    }
    
    # Initialise all components
    try:
        config = ConfigLoader.load_toml_config(Path(config_path))
        
        # Database managers
        api_db_manager = DatabaseManager()
        api_db_manager.create_connection(Path(api_data_db_path))
        api_db_manager.create_tables()
        
        state_db_manager = DatabaseManager()
        state_db_manager.create_connection(Path(state_db_path))
        state_db_manager.create_tables()
        
        # Core components
        state_manager = StateManager(state_db_manager)
        http_client = HTTPClient(
            max_retries=config.retries['max_attempts'],
            backoff_factor=config.retries.get('backoff_factor', 2.0),
            requests_per_second=config.rate_limits['requests_per_second']
        )
        payload_validator = PayloadValidator(config.payload_validation)
        
        # Authentication
        credentials = {}
        if config.authentication['type'] == 'api_key_and_token':
            credentials = {
                'type': 'api_key_and_token',
                'api_key': ConfigLoader.get_environment_value(config.authentication['api_key_env']),
                'inst_token': ConfigLoader.get_environment_value(config.authentication['inst_token_env'])
            }
        elif config.authentication['type'] == 'api_key':
            credentials = {
                'type': 'api_key',
                'api_key': ConfigLoader.get_environment_value(config.authentication['api_key_env'])
            }
        
        http_client.authenticate(credentials)
        
        # Optional caching
        cache_manager = None
        if enable_cache and config.cache.get('enabled', False):
            cache_dir = Path(config.cache.get('directory', 'cache'))
            cache_manager = CacheManager(
                cache_dir=cache_dir,
                expiration_seconds=config.cache.get('expiration_seconds', 3600),
                enabled=True
            )
            if cache_manager.acquire_lock():
                logger.info("Cache enabled for development")
            else:
                cache_manager = None
                logger.warning("Could not acquire cache lock, proceeding without cache")
        
        # Rate limiting
        rate_limit_file = Path(f"data/rate_limits/{config.name}_usage.json")
        rate_limit_file.parent.mkdir(parents=True, exist_ok=True)
        rate_limit_tracker = RateLimitTracker(
            tracking_file=rate_limit_file,
            weekly_limits=config.rate_limits.get('weekly_limits', {})
        )
        
        # Check weekly rate limits
        if not rate_limit_tracker.check_weekly_limit(config.name):
            raise Exception("Weekly API rate limit would be exceeded")
        
        # Check for existing state
        existing_state = state_manager.load_state(entity_id, load_id)
        if existing_state and not existing_state.completed:
            logger.info(f"Resuming processing for entity {entity_id} from page {existing_state.last_page + 1}")
            entity_result['pages_processed'] = existing_state.pages_processed
        
        # Get pagination strategy
        pagination_strategy = PaginationFactory.create_strategy(config.pagination)
        
        page_num = existing_state.last_page + 1 if existing_state else 1
        total_results = existing_state.total_results if existing_state else None
        
        # Process pages until completion
        while True:
            try:
                # Apply rate limiting
                http_client.apply_rate_limit()
                
                # Get pagination parameters
                page_params = pagination_strategy.get_next_page_params(
                    current_params={},
                    response=None,
                    page_num=page_num
                )
                
                if page_params is None:
                    logger.info(f"No more pages for entity {entity_id}")
                    break
                
                # Build API request
                api_request = _build_api_request(entity_id, page_params, config)
                
                # Check cache if enabled
                response = None
                cache_key = None
                if cache_manager:
                    cache_key = cache_manager.generate_cache_key(api_request)
                    cached_response = cache_manager.get_cached_response(cache_key)
                    if cached_response:
                        logger.debug(f"Using cached response for entity {entity_id}, page {page_num}")
                        response = cached_response
                
                if response is None:
                    response = http_client.make_request(api_request)
                    if cache_manager and cache_key:
                        cache_manager.store_response(cache_key, response)
                
                # Track the API request
                rate_limit_tracker.track_request(config.name, datetime.now(timezone.utc))
                
                # Validate response
                if not payload_validator.validate_response(response, config.payload_validation):
                    raise Exception(f"Response validation failed for entity {entity_id}, page {page_num}")
                
                # Extract total results on first page
                if total_results is None:
                    total_results = pagination_strategy.extract_total_results(response)
                    entity_result['total_results'] = total_results or 0
                
                # Store response in database
                api_db_manager.upsert_api_response({
                    'load_id': load_id,
                    'data_source': config.name,
                    'entity_id': entity_id,
                    'page_number': page_num,
                    'request_timestamp': api_request.timestamp,
                    'response_timestamp': response.request_timestamp,
                    'raw_response': response.raw_data,
                    'technical_metadata': response.metadata,
                    'log_entry': {'page_num': page_num, 'entity_id': entity_id},
                    'file_hash': FileHasher.generate_content_hash(response.raw_data)
                })
                
                # Update processing state
                state_manager.update_page_progress(entity_id, load_id, page_num)
                
                # Extract and track processed items
                items = _extract_items_from_response(response.raw_data)
                for item in items:
                    item_id = item.get('prism:doi') or item.get('dc:identifier', '')
                    if item_id:
                        state_manager.add_processed_item(entity_id, load_id, item_id)
                
                # Record file hash
                filename = f"{config.name}_author_{entity_id}_page{page_num}_{datetime.now().strftime('%Y%m%d')}.json"
                file_hash = FileHasher.generate_content_hash(response.raw_data)
                state_manager.record_file_hash(entity_id, load_id, filename, file_hash)
                
                entity_result['pages_processed'] += 1
                logger.info(f"Processed page {page_num} for entity {entity_id}")
                page_num += 1
                
            except Exception as page_error:
                # Log page-level error
                error_entry = {
                    'type': type(page_error).__name__,
                    'message': str(page_error),
                    'page_num': page_num
                }
                entity_result['errors'].append(error_entry)
                
                state_manager.log_processing_error(entity_id, load_id, page_error, page_num)
                logger.error(f"Error processing page {page_num} for entity {entity_id}: {page_error}")
                
                # Determine if we should continue or stop
                if _should_stop_processing(page_error):
                    entity_result['status'] = 'failed'
                    break
                else:
                    # Skip this page and continue
                    page_num += 1
                    continue
        
        # Mark entity as completed if no critical failures
        if entity_result['status'] != 'failed':
            state_manager.mark_completed(entity_id, load_id)
            entity_result['status'] = 'completed'
            logger.info(f"Successfully completed processing for entity {entity_id}")
        
    except Exception as entity_error:
        entity_result['status'] = 'failed'
        entity_result['errors'].append({
            'type': type(entity_error).__name__,
            'message': str(entity_error),
            'page_num': None
        })
        logger.error(f"Entity processing failed for {entity_id}: {entity_error}")
        # Don't re-raise - let the flow continue with other entities
    
    finally:
        entity_result['end_time'] = datetime.now(timezone.utc)
        
        # Cleanup connections
        try:
            if 'api_db_manager' in locals():
                api_db_manager.close_connection()
            if 'state_db_manager' in locals():
                state_db_manager.close_connection()
            if 'http_client' in locals():
                http_client.close_connection()
            if 'cache_manager' in locals() and cache_manager:
                cache_manager.release_lock()
        except:
            pass  # Ignore cleanup errors
    
    return entity_result


@task(
    name="generate_processing_manifest",
    description="Generate comprehensive processing manifest and statistics",
    retries=0
)
def generate_processing_manifest(processing_results: List[Dict[str, Any]], 
                               load_id: str, data_source: str,
                               api_data_db_path: str) -> Dict[str, Any]:
    """
    Generate processing manifest and store in database
    
    Args:
        processing_results: List of entity processing results
        load_id: Load identifier
        data_source: Data source name
        api_data_db_path: Path to API data database
        
    Returns:
        Complete manifest data
    """
    logger = get_run_logger()
    logger.info("Generating processing manifest")
    
    try:
        # Create manifest generator
        manifest_generator = ManifestGenerator()
        
        # Format results for manifest generator
        formatted_results = {
            'load_id': load_id,
            'data_source': data_source,
            'start_time': min(r['start_time'] for r in processing_results) if processing_results else datetime.now(timezone.utc),
            'end_time': max(r['end_time'] for r in processing_results) if processing_results else datetime.now(timezone.utc),
            'entities': processing_results
        }
        
        # Generate manifest
        manifest_data = manifest_generator.generate_manifest_data(formatted_results)
        
        # Store manifest in database
        db_manager = DatabaseManager()
        db_manager.create_connection(Path(api_data_db_path))
        
        db_manager.insert_manifest_data({
            'load_id': load_id,
            'data_source': data_source,
            'manifest_data': manifest_data,
            'processing_summary': manifest_data['processing_summary'],
            'total_entities': len(processing_results),
            'total_responses': sum(r.get('pages_processed', 0) for r in processing_results)
        })
        
        db_manager.close_connection()
        
        # Log summary statistics
        summary = manifest_data['processing_summary']
        logger.info(f"Processing completed: {summary['successful_entities']}/{summary['total_entities']} entities successful")
        logger.info(f"Total API responses: {summary['total_api_responses']}")
        logger.info(f"Success rate: {summary['success_rate_percent']:.1f}%")
        
        return manifest_data
        
    except Exception as e:
        logger.error(f"Manifest generation failed: {e}")
        raise


# ===================================================================
# PREFECT FLOWS - Main workflow orchestration
# ===================================================================

@flow(
    name="api-ingestion-pipeline",
    description="Complete REST API ingestion pipeline with state management",
    version="1.0.0",
    timeout_seconds=7200,
    log_prints=True
)
def api_ingestion_flow(config_path: str, entity_file_path: str, 
                      enable_cache: bool = False) -> Dict[str, Any]:
    """
    Complete API ingestion workflow using Prefect orchestration
    
    Args:
        config_path: Path to TOML configuration file
        entity_file_path: Path to entity list file
        enable_cache: Enable development caching
        
    Returns:
        Complete processing results and manifest
    """
    logger = get_run_logger()
    logger.info("Starting API ingestion pipeline")
    logger.info(f"Configuration: {config_path}")
    logger.info(f"Entity file: {entity_file_path}")
    logger.info(f"Caching: {'Enabled' if enable_cache else 'Disabled'}")
    
    try:
        # Step 1: Validate configuration and environment
        validation_results = validate_configuration_and_environment(config_path)
        data_source = validation_results['config_summary']['api_name']
        
        # Step 2: Initialise databases
        db_results = initialise_database_connections(config_path)
        api_data_db_path = db_results['api_data_db_path']
        state_db_path = db_results['state_db_path']
        
        # Step 3: Load entity list
        entities = load_entity_list(entity_file_path)
        
        # Step 4: Generate load identifier
        load_id = generate_load_identifier(data_source)
        
        # Step 5: Check for incomplete entities from previous runs
        entities_to_process = check_incomplete_entities(entities, load_id, state_db_path)
        
        logger.info(f"Processing {len(entities_to_process)} entities")
        
        # Step 6: Process each entity (parallel execution possible)
        entity_results = []
        for entity_id in entities_to_process:
            try:
                result = process_single_entity(
                    entity_id=entity_id,
                    config_path=config_path,
                    load_id=load_id,
                    api_data_db_path=api_data_db_path,
                    state_db_path=state_db_path,
                    enable_cache=enable_cache
                )
                entity_results.append(result)
                
            except Exception as e:
                # Log entity failure but continue with others
                logger.error(f"Failed to process entity {entity_id}: {e}")
                entity_results.append({
                    'entity_id': entity_id,
                    'status': 'failed',
                    'pages_processed': 0,
                    'total_results': 0,
                    'errors': [{'type': type(e).__name__, 'message': str(e), 'page_num': None}],
                    'start_time': datetime.now(timezone.utc),
                    'end_time': datetime.now(timezone.utc)
                })
        
        # Step 7: Generate processing manifest
        manifest_data = generate_processing_manifest(
            processing_results=entity_results,
            load_id=load_id,
            data_source=data_source,
            api_data_db_path=api_data_db_path
        )
        
        # Calculate final statistics
        total_entities = len(entity_results)
        successful_entities = len([r for r in entity_results if r['status'] == 'completed'])
        total_pages = sum(r.get('pages_processed', 0) for r in entity_results)
        
        logger.info("API ingestion pipeline completed successfully")
        logger.info(f"Results: {successful_entities}/{total_entities} entities successful")
        logger.info(f"Total pages processed: {total_pages}")
        
        return {
            'pipeline_status': 'SUCCESS',
            'load_id': load_id,
            'data_source': data_source,
            'total_entities': total_entities,
            'successful_entities': successful_entities,
            'total_pages_processed': total_pages,
            'entity_results': entity_results,
            'manifest': manifest_data,
            'execution_summary': {
                'config_validation': validation_results,
                'database_initialisation': db_results,
                'entities_loaded': len(entities),
                'entities_processed': len(entities_to_process)
            }
        }
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return {
            'pipeline_status': 'FAILED',
            'error': str(e),
            'load_id': None,
            'data_source': None
        }


# ===================================================================
# UTILITY FUNCTIONS
# ===================================================================

def _build_api_request(entity_id: str, page_params: Dict[str, Any], config) -> APIRequest:
    """Build API request object with entity and pagination parameters"""
    # Build query based on data source
    if config.name == "scopus_search":
        search_query = f"AU-ID({entity_id})"
    else:
        search_query = entity_id
    
    # Combine default parameters with pagination
    params = {
        'query': search_query,
        **dict(config.default_parameters),
        **page_params
    }
    
    return APIRequest(
        url=config.base_url,
        parameters=params,
        headers={},
        method="GET"
    )


def _extract_items_from_response(response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Safely extract items from response data"""
    try:
        search_results = response_data.get('search-results', {})
        if isinstance(search_results, dict):
            return search_results.get('entry', [])
        return []
    except (AttributeError, TypeError):
        return []


def _should_stop_processing(error: Exception) -> bool:
    """Determine if processing should stop based on error type"""
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


# ===================================================================
# DEVELOPMENT AND DEPLOYMENT UTILITIES
# ===================================================================

def create_sample_entity_file(file_path: Path, data_source: str) -> None:
    """Create a sample entity file for testing"""
    sample_entities = {
        'scopus_search': ["7004212771", "7005400538", "24726138500"],  # Sample Scopus author IDs
        'scival_metrics': ["60000001", "60000002", "60000003"],  # Sample institution IDs
        'wos_search': ["researcher_001", "researcher_002", "researcher_003"]  # Sample researcher IDs
    }
    
    entities = sample_entities.get(data_source, ["entity_001", "entity_002", "entity_003"])
    
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        for entity in entities:
            f.write(f"{entity}\n")
    
    print(f"Created sample entity file: {file_path}")
    print(f"Contains {len(entities)} sample entities for {data_source}")


def serve_flow():
    """Serve the flow for immediate execution"""
    print("Starting Prefect flow server...")
    print("Dashboard available at: http://127.0.0.1:4200")
    
    api_ingestion_flow.serve(
        name="api-ingestion-service",
        tags=["production", "api-ingestion"],
        description="REST API ingestion pipeline service",
        version="1.0.0"
    )


# ===================================================================
# COMMAND LINE INTERFACE
# ===================================================================

def main():
    """Main execution function with comprehensive CLI"""
    parser = argparse.ArgumentParser(
        description="Prefect REST API Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with configuration and entity file
  python prefect_api_orchestrator.py --config configs/scopus_search.toml --entities data/input/scopus_search_entities.txt --run

  # Run with caching enabled (development)
  python prefect_api_orchestrator.py --config configs/scopus_search.toml --entities data/input/scopus_search_entities.txt --run --cache

  # Create sample entity file
  python prefect_api_orchestrator.py --config configs/scopus_search.toml --create-sample

  # Validate configuration only
  python prefect_api_orchestrator.py --config configs/scopus_search.toml --validate-only

  # Serve flow for development
  python prefect_api_orchestrator.py --serve
        """
    )
    
    parser.add_argument("--config", help="Path to TOML configuration file")
    parser.add_argument("--entities", help="Path to entity list file")
    parser.add_argument("--run", action="store_true", help="Run the complete pipeline")
    parser.add_argument("--cache", action="store_true", help="Enable development caching")
    parser.add_argument("--create-sample", action="store_true", help="Create sample entity file based on data source")
    parser.add_argument("--validate-only", action="store_true", help="Only validate configuration")
    parser.add_argument("--serve", action="store_true", help="Serve flow for development")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Serve flows for development
    if args.serve:
        serve_flow()
        return 0
    
    # Configuration required for most operations
    if not args.config:
        print("--config is required for most operations")
        parser.print_help()
        return 1
    
    try:
        if args.validate_only:
            print("Validating configuration and environment...")
            result = validate_configuration_and_environment(args.config)
            print("Configuration validation passed!")
            print(f"API: {result['config_summary']['api_name']}")
            print(f"Base URL: {result['config_summary']['base_url']}")
            print(f"Pagination: {result['config_summary']['pagination_strategy']}")
            print(f"Rate limit: {result['config_summary']['rate_limit']} req/sec")
            return 0
            
        elif args.create_sample:
            config = ConfigLoader.load_toml_config(Path(args.config))
            data_source = config.name
            
            # Determine entity file path
            entity_file_path = Path(f"data/input/{data_source}_entities.txt")
            create_sample_entity_file(entity_file_path, data_source)
            return 0
            
        elif args.run:
            if not args.entities:
                print("--entities is required when using --run")
                return 1
            
            print("Starting API ingestion pipeline...")
            if args.cache:
                print("Development caching enabled")
            
            # Run the complete pipeline
            result = api_ingestion_flow(
                config_path=args.config,
                entity_file_path=args.entities,
                enable_cache=args.cache
            )
            
            # Print results
            print("\n" + "=" * 70)
            print("PIPELINE EXECUTION SUMMARY")
            print("=" * 70)
            print(f"Status: {result.get('pipeline_status', 'UNKNOWN')}")
            
            if result.get('pipeline_status') == 'SUCCESS':
                print(f"Load ID: {result.get('load_id', 'Unknown')}")
                print(f"Data Source: {result.get('data_source', 'Unknown')}")
                print(f"Entities Processed: {result.get('successful_entities', 0)}/{result.get('total_entities', 0)}")
                print(f"Total Pages: {result.get('total_pages_processed', 0)}")
                
                success_rate = (result.get('successful_entities', 0) / max(result.get('total_entities', 1), 1)) * 100
                print(f"Success Rate: {success_rate:.1f}%")
                
                # Show entity-level details if requested
                if args.verbose and 'entity_results' in result:
                    print("\nEntity Details:")
                    for entity_result in result['entity_results'][:10]:  # Show first 10
                        status_text = "COMPLETED" if entity_result['status'] == 'completed' else "FAILED"
                        print(f"  {status_text} {entity_result['entity_id']}: {entity_result['pages_processed']} pages")
                    
                    if len(result['entity_results']) > 10:
                        print(f"  ... and {len(result['entity_results']) - 10} more entities")
                
                print("\nDatabases:")
                exec_summary = result.get('execution_summary', {})
                db_init = exec_summary.get('database_initialisation', {})
                print(f"  API Data: {db_init.get('api_data_db_path', 'Unknown')}")
                print(f"  State Management: {db_init.get('state_db_path', 'Unknown')}")
                
            else:
                print(f"Pipeline failed: {result.get('error', 'Unknown error')}")
                return 1
            
            print("=" * 70)
            return 0
            
        else:
            parser.print_help()
            return 1
            
    except Exception as e:
        print(f"\nExecution failed: {e}")
        if args.verbose:
            print(f"Traceback: {traceback.format_exc()}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)