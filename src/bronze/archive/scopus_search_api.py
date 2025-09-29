"""
Scopus Search API Ingestion Script
This script ingests bibliometric data from Scopus via the Search API and retrieves the full RAW JSON output.
It processes author IDs, makes paginated API requests, and stores the raw responses in a date-partitioned
directory structure. The script includes manifest file generation for mapping of paginated results and auto-recovery capabilities to resume from failures.
"""

import os
import re
import json
import time
import argparse
import requests
import requests_cache
import logging
import yaml
import hashlib
from datetime import datetime, timedelta

# Define paths
PROJECT_ROOT = '/Users/work/Documents/prototype-analytics-platform'
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'apis', 'scopus.yaml')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')
INPUT_DIR = os.path.join(DATA_DIR, 'raw', 'api_input')
OUTPUT_DIR = os.path.join(DATA_DIR, 'raw', 'elsevier', 'scopus')
MANIFEST_DIR = os.path.join(DATA_DIR, 'raw', 'manifest')
STATE_DIR = os.path.join(DATA_DIR, 'state', 'scopus')

# Configure logging
log_file = os.path.join(LOGS_DIR, "scopus_author_publication_ingestion.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scopus_author_publication_ingestion")

def load_config(config_path):
    """
    Load configuration from YAML file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        dict: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return {}

# Load configuration
config = load_config(CONFIG_PATH)

# Set up cache from config
CACHE_DIR = os.path.join(DATA_DIR, 'cache', 'elsevier', 'scopus')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info(f"Created cache directory: {CACHE_DIR}")

# Set up request caching
if config.get('cache', {}).get('enabled', True):
    cache_file = os.path.join(CACHE_DIR, 'scopus_search_cache')
    requests_cache.install_cache(cache_file, expire_after=config.get('cache', {}).get('expiration', 86400))
    logger.info(f"Request caching enabled with expiration of {config.get('cache', {}).get('expiration', 86400)} seconds")

# Get API credentials and endpoints from config
SCOPUS_API_KEY = os.environ.get('SCOPUS_API_KEY')
SCOPUS_INST_ID = os.environ.get('SCOPUS_INST_ID')
SCOPUS_ENDPOINT = config.get('base_url')

# Get retry and pagination settings from config
MAX_RETRIES = config.get('retries', {}).get('max_attempts')
RETRY_WAIT = config.get('retries', {}).get('backoff_factor')
ITEMS_PER_PAGE = config.get('pagination', {}).get('items_per_page')

def create_directory_structure(base_path, date_format='%Y%m/%d'):
    """
    Creates a directory structure with date partitioning based on current date.
    
    Args:
        base_path (str): Base path where the directory structure should be created
        date_format (str): Format string for the date partition, configured to 'YYMM/DD'
    
    Returns:
        str: Full path to the created directory
    """
    # Create current date partition
    current_date = datetime.now().strftime(date_format)
    full_path = os.path.join(base_path, current_date)
    
    # Create directory if it doesn't exist
    if not os.path.exists(full_path):
        os.makedirs(full_path, exist_ok=True)
        logger.info(f"Created directory: {full_path}")
    else:
        logger.info(f"Directory already exists: {full_path}")
    
    return full_path

def create_directory_if_not_exists(dir_path):
    """
    Create directory if it doesn't exist.
    
    Args:
        dir_path (str): Path to directory
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def get_load_id_path(load_id, state_dir=STATE_DIR):
    """
    Generate a directory path based on load_id.
    
    Args:
        load_id (str): Unique identifier for the processing batch
        state_dir (str): Base directory where state files are saved
    
    Returns:
        str: Full path to the load_id directory
    """
    load_id_dir = os.path.join(state_dir, f"load_id_{load_id}")
    
    if not os.path.exists(load_id_dir):
        os.makedirs(load_id_dir, exist_ok=True)
        logger.info(f"Created directory: {load_id_dir}")
    
    return load_id_dir

def read_author_ids(filename):
    """
    Read author IDs from a file, one ID per line.
    
    Args:
        filename (str): Path to the file containing author IDs
        
    Returns:
        list: List of author IDs
    """
    try:
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error reading author IDs file: {e}")
        return []

# Ensures that when the pipeline re-runs due to a previous failure, result files are not duplicated by using hash to identify duplicates
def generate_file_hash(content):
    """
    Generate a hash for file content to enable idempotent storage.
    
    Args:
        content (dict): Content to hash
        
    Returns:
        str: Hash of the content
    """
    content_str = json.dumps(content, sort_keys=True)
    return hashlib.md5(content_str.encode('utf-8')).hexdigest()

def check_file_exists(output_dir, filename):
    """
    Check if a file already exists to avoid duplicate downloads.
    
    Args:
        output_dir (str): Directory where files are saved
        filename (str): Filename to check
        
    Returns:
        bool: True if file exists, False otherwise
    """
    file_path = os.path.join(output_dir, filename)
    return os.path.exists(file_path)

def validate_saved_file(file_path):
    """
    Validate that a saved JSON file contains valid data.
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Basic validation - check if essential keys exist
            if 'raw_response' in data and 'metadata' in data:
                if 'search-results' in data['raw_response']:
                    return True
        logger.warning(f"File {file_path} exists but contains invalid data")
        return False
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False

def create_or_load_state_file(author_id, state_dir=STATE_DIR, load_id=None):
    """
    Create or load a state file for tracking ingestion progress of an author.
    
    Args:
        author_id (str): The Scopus Author ID
        state_dir (str): Base directory where state files are saved
        load_id (str): Unique identifier for this processing batch
    
    Returns:
        dict: Current state of ingestion for the author
    """
    
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    # Create the state file path within the load_id directory
    state_file = os.path.join(load_id_dir, f"author_{author_id}_state.json")
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded existing state for author {author_id} from load_id directory")
                return state
        except Exception as e:
            logger.error(f"Error loading state file for author {author_id}: {e}")
    
    # Default state for new or failed state files
    return {
        "author_id": author_id,
        "load_id": load_id,
        "last_start_index": 0,
        "pages_processed": 0,
        "total_results": None,
        "dois": [],
        "file_names": [],
        "file_hashes": {},
        "completed": False,
        "last_updated": datetime.now().isoformat(),
        "errors": []
    }

def update_state_file(state, state_dir=STATE_DIR):
    """
    Update the state file with current ingestion progress.
    
    Args:
        state (dict): Current state to save
        state_dir (str): Base directory where state files are saved
    """
    # Get load_id from state
    load_id = state.get("load_id")
    if not load_id:
        logger.error("No load_id in state, cannot update state file")
        return
    
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    # Create the state file path
    state_file = os.path.join(load_id_dir, f"author_{state['author_id']}_state.json")
    
    # Update timestamp
    state["last_updated"] = datetime.now().isoformat()
    
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Updated state file for author {state['author_id']}")
    except Exception as e:
        logger.error(f"Error updating state file: {e}")

def get_incomplete_authors(state_dir, author_ids, load_id):
    """
    Identify which authors need processing or reprocessing for a specific load_id.
    
    Args:
        state_dir (str): Directory where state files are saved
        author_ids (list): List of all author IDs to check
        load_id (str): The load_id to check for
        
    Returns:
        list: List of author IDs that need processing
    """
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    if not os.path.exists(load_id_dir):
        logger.info(f"No state directory exists for load_id {load_id}. All authors need processing.")
        return author_ids
    
    # Track authors with completed state
    completed_authors = set()
    
    # Check each author for a completed state within this load_id
    for au_id in author_ids:
        state_file = os.path.join(load_id_dir, f"author_{au_id}_state.json")
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    # If the state is marked as completed, add to completed authors
                    if state.get("completed", False):
                        completed_authors.add(au_id)
            except Exception as e:
                logger.error(f"Error reading state file for author {au_id}: {e}")
    
    # Authors that are not marked as completed need processing
    incomplete_authors = [au_id for au_id in author_ids if au_id not in completed_authors]
    
    logger.info(f"Found {len(incomplete_authors)} out of {len(author_ids)} authors requiring processing for load_id {load_id}.")
    return incomplete_authors

def validate_author_files(author_id, output_dir, state_dir=STATE_DIR, load_id=None):
    """
    Validate that all files for an author exist and contain valid data.
    
    Args:
        author_id (str): Scopus Author ID
        output_dir (str): Directory where files are saved
        state_dir (str): Directory where state files are saved
        load_id (str): Unique identifier for this processing batch
        
    Returns:
        tuple: (bool, list) - First element indicates if all files are valid,
               second element is list of files that need reprocessing
    """
    state = create_or_load_state_file(author_id, state_dir, load_id)
    
    if not state.get("file_names"):
        logger.info(f"No files found for author {author_id} in state. Full reprocessing required.")
        return False, []
    
    invalid_files = []
    for filename in state.get("file_names", []):
        file_path = os.path.join(output_dir, filename)
        if not os.path.exists(file_path) or not validate_saved_file(file_path):
            invalid_files.append(filename)
    
    if invalid_files:
        logger.warning(f"Found {len(invalid_files)} invalid files for author {author_id}")
        return False, invalid_files
    
    return True, []

def generate_load_id(source_system='scopus'):
    """
    Generate a unique load ID combining source system, date, and timestamp.
    
    Args:
        source_system (str): Identifier for the source system (e.g., 'scopus', 'scival')
        
    Returns:
        str: A unique load ID
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"{source_system}_{timestamp}"

def scopus_lookup_by_author(au_id, output_dir, max_retries=MAX_RETRIES, state_dir=STATE_DIR, load_id=None):
    """
    Query Scopus for publications by a specific author ID with pagination, retry capability,
    and auto-recovery from previous failures.
    
    Args:
        au_id (str): Scopus Author ID
        output_dir (str): Directory to save raw JSON responses
        max_retries (int): Maximum number of retry attempts
        state_dir (str): Directory where state files are saved
        load_id (str): Unique identifier for this processing batch
        
    Returns:
        dict: Dictionary with author processing metadata
    """
    # Record start time
    start_time = datetime.now()
    
    # Load existing state or create new one
    state = create_or_load_state_file(au_id, state_dir, load_id)
    
    # Validate existing files
    all_valid, invalid_files = validate_author_files(au_id, output_dir, state_dir, load_id)
    
    # Check if this author was already completely processed and all files are valid
    if state.get("completed", False) and all_valid:
        logger.info(f"Author {au_id} was already completely processed with valid files. Skipping.")
        return {
            "author_id": au_id,
            "load_id": load_id,
            "start_time": start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "total_results": state.get("total_results", 0),
            "pages_processed": state.get("pages_processed", 0),
            "dois": state.get("dois", []),
            "file_names": state.get("file_names", [])
        }
    
    # If author was marked as completed but has invalid files
    if state.get("completed", False) and not all_valid:
        logger.info(f"Auto-recovering author {au_id} with {len(invalid_files)} invalid files")
        
        # Prepare state for recovery based on invalid files
        if invalid_files:
            # Extract page numbers from invalid files
            page_numbers = []
            for filename in invalid_files:
                try:
                    match = re.search(r'page(\d+)', filename)
                    if match:
                        page_numbers.append(int(match.group(1)))
                except Exception:
                    continue
            
            if page_numbers:
                # Find the earliest page with issues
                earliest_invalid_page = min(page_numbers)
                # Adjust the state to restart from this page
                state["pages_processed"] = earliest_invalid_page - 1
                state["last_start_index"] = (earliest_invalid_page - 1) * ITEMS_PER_PAGE
                state["completed"] = False
                
                # Remove files and DOIs after this point
                valid_files = []
                valid_file_hashes = {}
                valid_dois = []
                
                for filename in state["file_names"]:
                    current_page = None
                    try:
                        match = re.search(r'page(\d+)', filename)
                        if match:
                            current_page = int(match.group(1))
                    except Exception:
                        continue
                    
                    if current_page is not None and current_page < earliest_invalid_page:
                        valid_files.append(filename)
                        if filename in state.get("file_hashes", {}):
                            valid_file_hashes[filename] = state["file_hashes"][filename]
                
                # Only keep DOIs we can verify from valid files
                for file_name in valid_files:
                    file_path = os.path.join(output_dir, file_name)
                    if os.path.exists(file_path):
                        try:
                            with open(file_path, 'r') as f:
                                data = json.load(f)
                                entries = data.get('raw_response', {}).get('search-results', {}).get('entry', [])
                                for entry in entries:
                                    doi = entry.get('prism:doi', '')
                                    if doi:
                                        valid_dois.append(doi)
                        except Exception as e:
                            logger.error(f"Error reading DOIs from file {file_path}: {e}")
                
                state["file_names"] = valid_files
                state["file_hashes"] = valid_file_hashes
                state["dois"] = valid_dois
                logger.info(f"Adjusted state to restart from page {earliest_invalid_page} for author {au_id}")
    
    credentials = '&insttoken=' + SCOPUS_INST_ID + '&apiKey=' + SCOPUS_API_KEY
    search_query = 'AU-ID(' + au_id + ')'
    
    # Get default parameters from config
    params = config.get('default_parameters', {})
    params_str = ''.join([f"&{k}={v}" for k, v in params.items()])
    
    base_url = f"{SCOPUS_ENDPOINT}?query={search_query}{credentials}{params_str}"
    
    # Initialise pagination variables from state
    start_idx = state.get("last_start_index", 0)
    total_results = state.get("total_results")
    page_num = state.get("pages_processed", 0) + 1
    all_dois = state.get("dois", [])
    file_names = state.get("file_names", [])
    file_hashes = state.get("file_hashes", {})
    
    # Process all remaining pages
    while total_results is None or start_idx < total_results:
        # Add pagination parameters
        url = f"{base_url}&start={start_idx}&items_per_page={ITEMS_PER_PAGE}"
        
        retry_count = 0
        success = False
        
        # Implement retry mechanism
        while retry_count < max_retries and not success:
            try:
                logger.info(f"Requesting data for author {au_id} (page {page_num}, attempt {retry_count + 1})")
                response = requests.get(url)
                response.raise_for_status()
                
                # Log API rate limit information
                headers = response.headers
                logger.info(f'API Limit: {headers.get("X-RateLimit-Limit")}')
                logger.info(f'API Remaining: {headers.get("X-RateLimit-Remaining")}')
                
                data = response.json()
                success = True
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                logger.error(f"Error: {e}. Retry {retry_count}/{max_retries}")
                if retry_count < max_retries:
                    wait_time = RETRY_WAIT ** retry_count  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    error_msg = f"Failed to retrieve data for author {au_id} after {max_retries} attempts"
                    logger.error(error_msg)
                    
                    # Update state file with current progress and error
                    state_errors = state.get("errors", [])
                    state_errors.append({
                        "timestamp": datetime.now().isoformat(),
                        "page": page_num,
                        "error": str(e)
                    })
                    
                    state.update({
                        "load_id": load_id,
                        "last_start_index": start_idx,
                        "pages_processed": page_num - 1,
                        "total_results": total_results,
                        "dois": all_dois,
                        "file_names": file_names,
                        "file_hashes": file_hashes,
                        "completed": False,
                        "errors": state_errors
                    })
                    update_state_file(state, state_dir)
                    
                    return {
                        "author_id": au_id,
                        "load_id": load_id,
                        "start_time": start_time.isoformat(),
                        "end_time": datetime.now().isoformat(),
                        "duration_seconds": (datetime.now() - start_time).total_seconds(),
                        "total_results": total_results if total_results is not None else 0,
                        "pages_processed": page_num - 1,
                        "dois": all_dois,
                        "file_names": file_names,
                        "errors": [error_msg]
                    }
        
        # Process response data
        scopus_results = data['search-results']
        
        # Update total_results if not set yet
        if total_results is None:
            total_results = int(scopus_results.get('opensearch:totalResults', 0))
            logger.info(f'Total results for author {au_id}: {total_results}')
            
        # Extract DOIs for monitoring purposes
        entries = scopus_results.get('entry', [])
        page_dois = []
        for entry in entries:
            doi = entry.get('prism:doi', '')
            if doi:
                page_dois.append(doi)
                if doi not in all_dois:
                    all_dois.append(doi)
        
        # Add metadata to the response
        enriched_data = {
            'metadata': {
                'load_id': load_id,
                'data_source': 'scopus_search_api',
                'request_timestamp': datetime.now().isoformat(),
                'query_parameters': {
                    'author_id': au_id,
                    'page': page_num,
                    'start_index': start_idx,
                    'items_per_page': ITEMS_PER_PAGE
                },
                'rate_limit_remaining': response.headers.get('X-RateLimit-Remaining', 'unknown'),
                'total_results': total_results,
                'dois_in_page': page_dois
            },
            'raw_response': data
        }
        
        # Generate content hash
        content_hash = generate_file_hash(enriched_data)
        
        # Save the raw response to the partitioned output directory
        filename = f"scopus_search_author_{au_id}_page{page_num}_{datetime.now().strftime('%Y%m%d')}.json"
        file_path = os.path.join(output_dir, filename)
        
        # Check if file with same hash already exists
        file_exists = False
        for existing_filename, existing_hash in file_hashes.items():
            if existing_hash == content_hash:
                logger.info(f"File with same content already exists: {existing_filename}. Skipping write.")
                file_exists = True
                if filename not in file_names:
                    file_names.append(filename)
                file_hashes[filename] = content_hash
                break
        
        if not file_exists:
            try:
                with open(file_path, 'w') as f:
                    json.dump(enriched_data, f, indent=2)
                logger.info(f"Saved raw data to {file_path}")
                
                if filename not in file_names:
                    file_names.append(filename)
                file_hashes[filename] = content_hash
                
            except Exception as e:
                logger.error(f"Error saving raw data: {e}")
                # Update state in case of file write error
                state_errors = state.get("errors", [])
                state_errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "page": page_num,
                    "error": f"File write error: {str(e)}"
                })
                
                state.update({
                    "load_id": load_id,
                    "last_start_index": start_idx,
                    "pages_processed": page_num - 1,
                    "total_results": total_results,
                    "dois": all_dois,
                    "file_names": file_names,
                    "file_hashes": file_hashes,
                    "completed": False,
                    "errors": state_errors
                })
                update_state_file(state, state_dir)
        
        # Update state after successful page processing
        state.update({
            "load_id": load_id,
            "last_start_index": start_idx + len(entries),
            "pages_processed": page_num,
            "total_results": total_results,
            "dois": all_dois,
            "file_names": file_names,
            "file_hashes": file_hashes,
            "completed": False
        })
        update_state_file(state, state_dir)
        
        # Update for next page
        start_idx += len(entries)
        page_num += 1
        
        # If no more entries found, break the loop
        if not entries:
            break
        
        # Add a delay between pagination calls based on rate limits in config
        rate_delay = 1 / config.get('rate_limits', {}).get('requests_per_second', 2)
        time.sleep(rate_delay)
    
    # Record end time
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    # Validate all files for integrity before marking as complete
    all_valid, invalid_files = validate_author_files(au_id, output_dir, state_dir, load_id)
    
    if all_valid:
        # Mark author as completed in state
        state.update({
            "completed": True,
        })
        update_state_file(state, state_dir)
        
        logger.info(f"Completed API requests for author {au_id}. Retrieved {len(all_dois)} DOIs across {page_num-1} pages")
    else:
        # Some files are still invalid, mark as incomplete
        logger.warning(f"Some files are invalid for author {au_id}. Marking as incomplete.")
        state.update({
            "completed": False,
            "errors": state.get("errors", []) + [
                {
                    "timestamp": datetime.now().isoformat(),
                    "error": f"Validation failed for files: {invalid_files}"
                }
            ]
        })
        update_state_file(state, state_dir)
    
    # Return author processing metadata for manifest creation
    return {
        'load_id': load_id,
        "author_id": au_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "total_results": total_results if total_results is not None else 0,
        "pages_processed": page_num - 1,
        "dois": all_dois,
        "file_names": file_names,
        "completed": state.get("completed", False),
        "errors": state.get("errors", [])
    }

def generate_manifest(author_results, output_dir, manifest_dir=MANIFEST_DIR, include_dois=False, load_id=None):
    """
    Generate and save a manifest file containing metadata about each author processed.
    
    Args:
        author_results: Dictionary mapping author IDs to their processing metadata
        output_dir: Directory where the raw data files are saved
        manifest_dir: Base directory for storing manifest files in date partitions
        include_dois: Whether to include full DOI lists in the manifest (can be large)
        load_id: Unique identifier for this processing batch
        
    Returns:
        str: Path to the generated manifest file
    """
    # Create date-partitioned directory for manifest file with YYYYMM/DD structure
    year_month = datetime.now().strftime('%Y%m')
    day = datetime.now().strftime('%d')
    manifest_date_dir = os.path.join(manifest_dir, year_month, day)
    
    # Create directory if it doesn't exist
    if not os.path.exists(manifest_date_dir):
        os.makedirs(manifest_date_dir, exist_ok=True)
        logger.info(f"Created manifest directory: {manifest_date_dir}")
    
    # Create filename with current date
    manifest_file = os.path.join(manifest_date_dir, f"scopus_manifest_{datetime.now().strftime('%Y%m%d')}.json")
    
    # Calculate overall metrics
    total_publications = sum(author["total_results"] for author in author_results.values())
    
    # Get unique DOIs across all authors
    all_dois_sets = [set(author["dois"]) for author in author_results.values()]
    if all_dois_sets:
        total_unique_dois = len(set.union(*all_dois_sets))
    else:
        total_unique_dois = 0
    
    # Calculate error stats
    authors_with_errors = sum(1 for author in author_results.values() if author.get("errors"))
    
    manifest = {
        "manifest_generated": datetime.now().isoformat(),
        'load_id': load_id,
        "total_authors_processed": len(author_results),
        "authors_with_errors": authors_with_errors,
        "processing_summary": {
            "start_time": min([author["start_time"] for author in author_results.values()], default=None),
            "end_time": max([author["end_time"] for author in author_results.values()], default=None),
            "total_publications": total_publications,
            "total_unique_dois": total_unique_dois
        },
        "authors": []
    }
    
    # Calculate processing duration if we have both start and end times
    if manifest["processing_summary"]["start_time"] and manifest["processing_summary"]["end_time"]:
        start = datetime.fromisoformat(manifest["processing_summary"]["start_time"])
        end = datetime.fromisoformat(manifest["processing_summary"]["end_time"])
        duration = (end - start).total_seconds()
        manifest["processing_summary"]["duration_seconds"] = duration

        # Add author-specific metadata
    for author_id, data in author_results.items():
        author_entry = {
            "author_id": author_id,
            "load_id": data.get("load_id", load_id),
            "timestamp": {
                "start_time": data["start_time"],
                "end_time": data["end_time"],
                "duration_seconds": data["duration_seconds"]
            },
            "results": {
                "total_publications": data["total_results"],
                "pages_processed": data["pages_processed"],
                "dois_count": len(data["dois"]),
                "completed": data.get("completed", False)
            },
            "file_names": data["file_names"]
        }
        
        # Add errors if present
        if data.get("errors"):
            author_entry["errors"] = data["errors"]
        
        # Add DOIs if requested
        if include_dois:
            author_entry["dois"] = data["dois"]
            
        manifest["authors"].append(author_entry)
    
    try:
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifest file generated and saved to {manifest_file}")
        return manifest_file
    except Exception as e:
        logger.error(f"Error saving manifest file: {e}")
        return None

def rebuild_manifest_from_state(output_dir, state_dir=STATE_DIR, manifest_dir=MANIFEST_DIR, load_id=None):
    """
    Rebuild manifest from existing state files, useful for recovery.
    
    Args:
        output_dir (str): Directory where the raw data files are saved
        state_dir (str): Directory where state files are stored
        manifest_dir (str): Directory where manifest files should be saved
        load_id (str): Specific load_id to rebuild
        
    Returns:
        str: Path to the generated manifest file
    """
    if not os.path.exists(state_dir):
        logger.error(f"State directory {state_dir} does not exist. Cannot rebuild manifest.")
        return None
    
    if load_id is None:
        logger.error("No load_id provided. Cannot rebuild manifest.")
        return None 
    
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    if not os.path.exists(load_id_dir):
        logger.error(f"State directory for load_id {load_id} does not exist.")
        return None
    
    # Find all state files for this load_id
    state_files = [f for f in os.listdir(load_id_dir) if f.endswith('_state.json')]
    if not state_files:
        logger.error(f"No state files found for load_id {load_id}. Cannot rebuild manifest.")
        return None
    
    # Load all state files
    author_results = {}
    for state_file in state_files:
        try:
            with open(os.path.join(load_id_dir, state_file), 'r') as f:
                state = json.load(f)
                author_id = state.get("author_id")
                if not author_id:
                    continue
                
                # Validate that the files actually exist
                valid_file_names = []
                for filename in state.get("file_names", []):
                    file_path = os.path.join(output_dir, filename)
                    if os.path.exists(file_path) and validate_saved_file(file_path):
                        valid_file_names.append(filename)
                
                # Create a synthetic author result entry
                author_results[author_id] = {
                    "author_id": author_id,
                    "load_id": load_id,
                    "start_time": state.get("last_updated"),
                    "end_time": state.get("last_updated"),
                    "duration_seconds": 0,
                    "total_results": state.get("total_results", 0),
                    "pages_processed": state.get("pages_processed", 0),
                    "dois": state.get("dois", []),
                    "file_names": valid_file_names,
                    "completed": state.get("completed", False),
                    "errors": state.get("errors", [])
                }
        except Exception as e:
            logger.error(f"Error processing state file {state_file}: {e}")
    
    if not author_results:
        logger.error("No valid author results found in state files. Cannot rebuild manifest.")
        return None
    
    # Generate a new manifest with date-based partitioning
    return generate_manifest(author_results, output_dir, manifest_dir, False, load_id)

def main():
    """Main function to process author IDs and retrieve publication data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Retrieve publication data from Scopus API based on author IDs")
    parser.add_argument("--input", "-i", default=os.path.join(INPUT_DIR, "author_ids.txt"),
                        help=f"File containing Scopus AU-IDs, one per line (default: {os.path.join(INPUT_DIR, 'author_ids.txt')})")
    parser.add_argument("--output-dir", "-o", default=OUTPUT_DIR, 
                        help=f"Base output directory for raw JSON files (default: {OUTPUT_DIR})")
    parser.add_argument("--manifest-dir", "-m", default=MANIFEST_DIR,
                        help=f"Base directory for manifest files (default: {MANIFEST_DIR})")
    parser.add_argument("--state-dir", "-s", default=STATE_DIR,
                        help=f"Directory for state files (default: {STATE_DIR})")
    parser.add_argument("--retries", "-r", type=int, default=MAX_RETRIES, 
                        help=f"Maximum number of retry attempts (default: {MAX_RETRIES})")
    parser.add_argument("--force-reprocess", "-f", action="store_true",
                        help="Force reprocessing of all authors, ignoring existing state")
    parser.add_argument("--rebuild-manifest", action="store_true",
                        help="Rebuild manifest from existing state and files")
    parser.add_argument("--include-dois", action="store_true",
                        help="Include full DOI lists in manifest file (can make manifest large)")
    parser.add_argument("--single-author", type=str,
                        help="Process only a single author ID (useful for testing or targeted recovery)")
    parser.add_argument("--load-id", "-l", type=str, default=None,
                        help="Specify a load_id to use (useful for continuing a previous run)")
    args = parser.parse_args()
    
    # Record script start time
    script_start_time = datetime.now()
    
    # Check if API key is set
    if not SCOPUS_API_KEY:
        logger.error("Error: SCOPUS_API_KEY environment variable not set")
        exit(1)
    
    # Generate load_id or use the one provided via CLI
    load_id = args.load_id if args.load_id else generate_load_id('scopus')
    logger.info(f"Using load ID: {load_id}")
    
    # Handle rebuild manifest only mode
    if args.rebuild_manifest:
        logger.info(f"Rebuilding manifest from existing state files for load_id {load_id}...")
        output_dir = create_directory_structure(args.output_dir)
        manifest_file = rebuild_manifest_from_state(output_dir, args.state_dir, args.manifest_dir, load_id)
        if manifest_file:
            logger.info(f"Manifest rebuilt successfully and saved to {manifest_file}")
        else:
            logger.error("Failed to rebuild manifest")
        return
    
    # Create partitioned output directory
    output_dir = create_directory_structure(args.output_dir)
    logger.info(f"Output directory: {output_dir}")

    # Handle single author mode
    if args.single_author:
        all_author_ids = [args.single_author]
        logger.info(f"Single author mode: processing only {args.single_author}")
    else:
        # Read author IDs from the input file
        all_author_ids = read_author_ids(args.input)
        logger.info(f"Found {len(all_author_ids)} author IDs in input file")
    
    # Determine which authors need processing
    if args.force_reprocess:
        author_ids = all_author_ids
        logger.info("Force reprocessing all authors")
    else:
        # Use the load_id-based incomplete authors check
        author_ids = get_incomplete_authors(args.state_dir, all_author_ids, load_id)
    
    if not author_ids:
        logger.info("No authors need processing. All complete.")
        return
    
    logger.info(f"Will process {len(author_ids)} author(s)")
    
    # Process each author ID and collect metadata for the manifest
    author_results = {}
    all_dois = []
    processing_errors = []
    
    for index, au_id in enumerate(author_ids):
        logger.info(f"Processing author {index+1}/{len(author_ids)}: {au_id}")
        
        try:
            # Process author and get metadata - pass the load_id
            author_data = scopus_lookup_by_author(
                au_id, 
                output_dir, 
                args.retries, 
                args.state_dir, 
                load_id 
            )
            author_results[au_id] = author_data
            all_dois.extend(author_data["dois"])
            
            # Check for errors
            if author_data.get("errors"):
                error_count = len(author_data["errors"])
                logger.warning(f"Author {au_id} processed with {error_count} errors")
                processing_errors.append(f"Author {au_id}: {error_count} errors")
        except Exception as e:
            logger.error(f"Unexpected error processing author {au_id}: {e}")
            processing_errors.append(f"Author {au_id}: Unexpected error: {str(e)}")
            # Create a basic entry for failed authors
            author_results[au_id] = {
                "author_id": au_id,
                "load_id": load_id,
                "start_time": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "duration_seconds": 0,
                "total_results": 0,
                "pages_processed": 0,
                "dois": [],
                "file_names": [],
                "completed": False,
                "errors": [{"timestamp": datetime.now().isoformat(), "error": str(e)}]
            }
        
        # Add a delay between authors based on rate limits in config
        if index < len(author_ids) - 1:
            rate_delay = 1 / config.get('rate_limits', {}).get('requests_per_second', 2)
            time.sleep(rate_delay)
    
    # For a complete manifest, load data for already processed authors
    for au_id in all_author_ids:
        if au_id not in author_results:
            state = create_or_load_state_file(au_id, args.state_dir, load_id)
            if state.get("completed", False):
                author_results[au_id] = {
                    "author_id": au_id,
                    "load_id": load_id,
                    "start_time": state.get("last_updated"),
                    "end_time": state.get("last_updated"),
                    "duration_seconds": 0,
                    "total_results": state.get("total_results", 0),
                    "pages_processed": state.get("pages_processed", 0),
                    "dois": state.get("dois", []),
                    "file_names": state.get("file_names", []),
                    "completed": True,
                    "errors": state.get("errors", [])
                }
                all_dois.extend(state.get("dois", []))
    
    # Generate manifest file
    manifest_file = generate_manifest(author_results, output_dir, args.manifest_dir, args.include_dois, load_id)
    
    # Count unique DOIs for monitoring purposes
    unique_dois = set(all_dois)
    
    # Record script end time
    script_end_time = datetime.now()
    script_duration = (script_end_time - script_start_time).total_seconds()
    
    # Calculate success rate
    completed_authors = sum(1 for data in author_results.values() if data.get("completed", False))
    
    # Print summary
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Total authors processed: {len(author_ids)}")
    logger.info(f"Authors completed successfully: {completed_authors} ({(completed_authors/len(author_results)*100):.1f}%)")
    if processing_errors:
        logger.info(f"Authors with errors: {len(processing_errors)}")
        for error in processing_errors[:5]:  # Show first 5 errors
            logger.info(f"  - {error}")
        if len(processing_errors) > 5:
            logger.info(f"  ... and {len(processing_errors) - 5} more errors")
    
    logger.info(f"Total publications retrieved: {sum(author['total_results'] for author in author_results.values())}")
    logger.info(f"Total DOIs found: {len(all_dois)} ({len(unique_dois)} unique)")
    logger.info(f"Total processing time: {script_duration:.2f} seconds")
    logger.info(f"Load ID used: {load_id}")
    
    # Get manifest directory path for logging
    manifest_date_dir = os.path.join(args.manifest_dir, datetime.now().strftime('%Y%m/%d'))
    logger.info(f"Manifest file generated in: {manifest_date_dir}")

if __name__ == "__main__":
    main()