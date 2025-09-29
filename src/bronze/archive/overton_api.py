"""
Overton API Ingestion Script
This script ingests policy citation data from Overton API for DOIs from a text file.
It processes DOIs from a text file, makes API requests, and stores the raw responses
in a date-partitioned directory structure with a manifest file.
"""

import os
import json
import time
import argparse
import requests
import requests_cache
import logging
import yaml
import hashlib
from datetime import datetime
from typing import List, Dict, Tuple

# Define paths
PROJECT_ROOT = '/Users/work/Documents/prototype-analytics-platform'
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'apis', 'overton.yaml')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')
INPUT_DIR = os.path.join(DATA_DIR, 'raw', 'api_input')
OUTPUT_DIR = os.path.join(DATA_DIR, 'raw', 'overton')
MANIFEST_DIR = os.path.join(DATA_DIR, 'raw', 'manifest')
STATE_DIR = os.path.join(DATA_DIR, 'state', 'overton')

# Configure logging
log_file = os.path.join(LOGS_DIR, "overton_policy_ingestion.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("overton_ingestion")

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
CACHE_DIR = os.path.join(DATA_DIR, 'cache', 'overton')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info(f"Created cache directory: {CACHE_DIR}")

# Set up request caching
if config.get('cache', {}).get('enabled', True):
    cache_file = os.path.join(CACHE_DIR, 'overton_cache')
    requests_cache.install_cache(cache_file, expire_after=config.get('cache', {}).get('expiration', 86400))
    logger.info(f"Request caching enabled with expiration of {config.get('cache', {}).get('expiration', 86400)} seconds")

# Get API credentials and endpoints from config and environment
OVERTON_API_KEY = os.environ.get('OVERTON_API_KEY')
OVERTON_SET_ENDPOINT = config.get('base_url') + 'generate_id_set.php'
OVERTON_DOCS_ENDPOINT = config.get('base_url') + 'documents.php'

# Get retry and pagination settings from config
MAX_RETRIES = config.get('retries', {}).get('max_attempts', 3)
RETRY_WAIT = config.get('retries', {}).get('backoff_factor', 2)
MAX_DOIS_PER_SET = 25  # Overton API limit for DOIs per set

def create_directory_structure(base_path, date_format='%Y%m/%d'):
    """
    Creates a directory structure with date partitioning based on current date.
    
    Args:
        base_path (str): Base path where the directory structure should be created
        date_format (str): Format string for the date partition, default is 'YYMM/DD'
    
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

def get_load_id_path(load_id, state_dir=STATE_DIR):
    """
    Generate a directory path based on load_id.
    
    Args:
        load_id (str): Unique identifier for the processing batch
        state_dir (str): Base directory where state files are saved
    
    Returns:
        str: Full path to the load_id directory
    """
    # Create load_id-based directory path
    load_id_dir = os.path.join(state_dir, f"load_id_{load_id}")
    
    # Create directory if it doesn't exist
    if not os.path.exists(load_id_dir):
        os.makedirs(load_id_dir, exist_ok=True)
        logger.info(f"Created directory: {load_id_dir}")
    else:
        logger.debug(f"Directory already exists: {load_id_dir}")
    
    return load_id_dir

def create_or_load_state_file(batch_num, dois, load_id, state_dir=STATE_DIR):
    """
    Create or load a state file for tracking ingestion progress of a batch.
    
    Args:
        batch_num (int): The batch number
        dois (list): List of DOIs in this batch
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Base directory where state files are saved
    
    Returns:
        dict: Current state of ingestion for the batch
    """
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    # Create the state file path within the load_id directory
    state_file = os.path.join(load_id_dir, f"batch_{batch_num}_state.json")
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded existing state for batch {batch_num} from load_id directory")
                return state
        except Exception as e:
            logger.error(f"Error loading state file for batch {batch_num}: {e}")
    
    # Default state for new or failed state files
    return {
        "batch_num": batch_num,
        "dois": dois,
        "load_id": load_id,
        "set_id": None,
        "policy_document_ids": [],
        "file_name": None,
        "file_hash": None,
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
    state_file = os.path.join(load_id_dir, f"batch_{state['batch_num']}_state.json")
    
    # Update timestamp
    state["last_updated"] = datetime.now().isoformat()
    
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Updated state file for batch {state['batch_num']}")
    except Exception as e:
        logger.error(f"Error updating state file: {e}")

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

def validate_batch_file(batch_num, output_dir, load_id, state_dir=STATE_DIR):
    """
    Validate that the file for a batch exists and contains valid data.
    
    Args:
        batch_num (int): Batch number
        output_dir (str): Directory where files are saved
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Directory where state files are saved
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    state = create_or_load_state_file(batch_num, [], load_id, state_dir)
    
    if not state.get("file_name"):
        logger.info(f"No file found for batch {batch_num} in state. Processing required.")
        return False
    
    file_path = os.path.join(output_dir, state["file_name"])
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} does not exist for batch {batch_num}")
        return False
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Basic validation - check if essential keys exist
            if 'raw_response' in data and 'metadata' in data:
                # If we have a stored hash, verify the content matches
                if state.get("file_hash"):
                    actual_hash = generate_file_hash(data)
                    if actual_hash != state.get("file_hash"):
                        logger.warning(f"File {file_path} hash mismatch. Expected: {state.get('file_hash')}, Got: {actual_hash}")
                        return False
                return True
        logger.warning(f"File {file_path} exists but contains invalid data")
        return False
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False

def get_input_file_path():
    """
    Determine the path to the most recent input file in the date-partitioned directory.
    
    Returns:
        str: Path to the input file
    """
    # Get current year and month for directory
    current_year_month = datetime.now().strftime('%Y%m')
    input_dir_path = os.path.join(INPUT_DIR, current_year_month)
    
    # Get today's date for filename pattern matching
    current_date = datetime.now().strftime('%Y%m%d')
    
    # Find files matching the pattern
    matching_files = []
    try:
        for file in os.listdir(input_dir_path):
            if file.startswith('scopus_generated') and 'dois' in file and current_date in file and file.endswith('.txt'):
                matching_files.append(os.path.join(input_dir_path, file))
    except FileNotFoundError:
        logger.error(f"Directory not found: {input_dir_path}")
        return ""
    
    if not matching_files:
        logger.warning(f"No matching input files found in {input_dir_path}")
        return ""
    
    # If multiple files match, get the most recent one
    latest_file = max(matching_files, key=os.path.getmtime) if matching_files else ""
    
    if not latest_file:
        logger.warning("No input file found for today's date")
    else:
        logger.info(f"Found input file: {latest_file}")
    
    return latest_file

def read_dois_from_file(file_path):
    """
    Read DOIs from a text file, one DOI per line.
    
    Args:
        file_path (str): Path to the file containing DOIs
        
    Returns:
        list: List of DOIs
    """
    try:
        with open(file_path, 'r') as f:
            dois = [line.strip() for line in f if line.strip()]
        
        # Remove duplicates while preserving order
        unique_dois = []
        for doi in dois:
            if doi not in unique_dois:
                unique_dois.append(doi)
                
        logger.info(f"Read {len(unique_dois)} unique DOIs from {file_path}")
        return unique_dois
    except Exception as e:
        logger.error(f"Error reading DOIs file: {e}")
        return []

def chunk_list(input_list, chunk_size):
    """
    Split a list into chunks of specified size.
    
    Args:
        input_list (list): List to be chunked
        chunk_size (int): Maximum size of each chunk
        
    Returns:
        list: List of sublists (chunks)
    """
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]

# When re-factoring code, have source_system inherit from config files
def generate_load_id(source_system='overton'):
    """
    Generate a unique load ID combining source system, date, and timestamp.
    
    Args:
        source_system (str): Identifier for the source system (e.g., 'overton')
        
    Returns:
        str: A unique load ID
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"{source_system}_{timestamp}"

def generate_doi_set(dois: List[str], batch_num: int, load_id: str, max_retries=MAX_RETRIES, state_dir=STATE_DIR) -> Tuple[bool, str]:
    """
    Generate a set ID for a list of DOIs with retry capability and state tracking.
    
    Args:
        dois: List of DOIs to include in the set
        batch_num: The batch number
        load_id: Unique identifier for this processing batch
        max_retries: Maximum number of retry attempts
        state_dir: Directory where state files are saved
        
    Returns:
        Tuple of (success flag, set ID)
    """
    # Get or create state file for this batch
    state = create_or_load_state_file(batch_num, dois, load_id, state_dir)
    
    # Check if we already have a set_id from a previous run
    if state.get("set_id"):
        logger.info(f"Using existing set ID {state.get('set_id')} for batch {batch_num}")
        return True, state.get("set_id")
    
    url = f"{OVERTON_SET_ENDPOINT}?format=json&api_key={OVERTON_API_KEY}"
    dois_data = "\n".join(dois)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            logger.info(f"Generating set for {len(dois)} DOIs (attempt {retry_count + 1})")
            response = requests.post(url, headers=headers, data=f"dois={dois_data}")
            response.raise_for_status()
            
            result = response.json()
            
            if "set" in result:
                set_id = result["set"]
                
                # Update state with the set_id
                state["set_id"] = set_id
                update_state_file(state, state_dir)
                
                return True, set_id
            elif "error" in result:
                logger.error(f"Error from API: {result['error']}")
                retry_count += 1
            else:
                logger.error("Unexpected response format")
                retry_count += 1
                
        except requests.exceptions.RequestException as e:
            retry_count += 1
            logger.error(f"Request failed: {e}. Retry {retry_count}/{max_retries}")
            
        # Wait before retrying with exponential backoff
        if retry_count < max_retries:
            wait_time = RETRY_WAIT ** retry_count
            logger.info(f"Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
    
    # Update state with error
    state_errors = state.get("errors", [])
    state_errors.append({
        "timestamp": datetime.now().isoformat(),
        "error": "Failed to generate set ID after all retry attempts"
    })
    state["errors"] = state_errors
    update_state_file(state, state_dir)
    
    logger.error(f"Failed to generate set after {max_retries} attempts")
    return False, ""

def overton_policy_lookup(dois_batch: List[str], output_dir: str, batch_num: int, load_id: str, 
                          max_retries=MAX_RETRIES, state_dir=STATE_DIR) -> Dict:
    """
    Query Overton for policy citations of specific DOIs with pagination, retry capability,
    and auto-recovery from previous failures.
    
    Args:
        dois_batch (list): List of DOIs to query
        output_dir (str): Directory to save raw JSON responses
        batch_num (int): Current batch number for naming files
        load_id (str): Unique identifier for this processing batch
        max_retries (int): Maximum number of retry attempts
        state_dir (str): Directory where state files are saved
        
    Returns:
        dict: Metadata about the batch processing
    """
    # Record start time
    start_time = datetime.now()
    
    # Load existing state or create new one
    state = create_or_load_state_file(batch_num, dois_batch, load_id, state_dir)
    
    # Validate existing file
    is_valid = validate_batch_file(batch_num, output_dir, load_id, state_dir)
    
    # Check if this batch was already completely processed and file is valid
    if state.get("completed", False) and is_valid:
        logger.info(f"Batch {batch_num} was already completely processed with valid file. Skipping.")
        return {
            "load_id": load_id,
            "batch_num": batch_num,
            "dois": dois_batch,
            "start_time": start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "success": True,
            "policy_document_ids": state.get("policy_document_ids", []),
            "file_name": state.get("file_name"),
            "set_id": state.get("set_id")
        }
    
    # Step 1: Generate a set for this batch using the separate function
    success, set_id = generate_doi_set(dois_batch, batch_num, load_id, max_retries, state_dir)
    if not success:
        logger.error(f"Failed to generate set for batch {batch_num}. Aborting batch processing.")
        
        # Return metadata with error information
        end_time = datetime.now()
        return {
            "load_id": load_id,
            "batch_num": batch_num,
            "dois": dois_batch,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "success": False,
            "error": "Failed to generate set ID",
            "policy_document_ids": [],
            "file_name": None,
            "set_id": None
        }
    
    # Step 2: Get policy info for this set
    all_results = []
    current_page = 1
    policy_document_ids = []
    
    while True:
        # Get default parameters from config
        params = config.get('default_parameters', {}).copy()
        params.update({
            'plain_dois_cited': set_id,
            'api_key': OVERTON_API_KEY,
            'page': current_page
        })
        
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                logger.info(f"Fetching page {current_page} for set {set_id} (attempt {retry_count + 1})")
                response = requests.get(OVERTON_DOCS_ENDPOINT, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data or 'results' not in data:
                    logger.warning("No results found in response")
                    success = True
                    break
                
                # Extract policy document IDs
                for doc in data['results']:
                    if 'id' in doc:
                        policy_document_ids.append(doc['id'])
                
                all_results.extend(data['results'])
                success = True
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                logger.error(f"Request failed: {e}. Retry {retry_count}/{max_retries}")
                
                if retry_count < max_retries:
                    wait_time = RETRY_WAIT ** retry_count  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch page {current_page} after {max_retries} attempts")
                    
                    # Update state with error and what we have so far
                    state_errors = state.get("errors", [])
                    state_errors.append({
                        "timestamp": datetime.now().isoformat(),
                        "error": f"Failed to fetch page {current_page}: {str(e)}"
                    })
                    state["errors"] = state_errors
                    update_state_file(state, state_dir)
                    
                    # Continue with partial results
                    success = True
        
        if not success or not data or 'results' not in data:
            break
            
        # Check if there's a next page
        if ('query' in data and 
            'next_page_url' in data['query'] and 
            data['query']['next_page_url']):
            current_page += 1
            # Respect rate limits between pages
            time.sleep(1 / config.get('rate_limits', {}).get('requests_per_second', 1))
        else:
            # No more pages
            break
    
    # Build the complete response
    response_data = {"set_id": set_id, "results": all_results}
    
    # Add metadata to the response
    enriched_data = {
        'metadata': {
            'load_id': load_id,
            'data_source': 'overton_api',
            'request_timestamp': datetime.now().isoformat(),
            'processing_times': {
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            },
            'query_parameters': {
                'set_id': set_id,
                'batch': batch_num,
                'dois': dois_batch,
                'doi_count': len(dois_batch)
            }
        },
        'raw_response': response_data
    }
    
    # Generate a content hash for the data before saving
    content_hash = generate_file_hash(enriched_data)

    # First determine if we need to save a new file
    file_saved = False
    write_file = True
    filename = None

    # Check if we already have a file with the same content hash
    if state.get("file_hash") == content_hash and state.get("file_name"):
        existing_file_path = os.path.join(output_dir, state.get("file_name"))
        if os.path.exists(existing_file_path):
            try:
                # Verify the existing file is valid by opening it
                with open(existing_file_path, 'r') as f:
                    json.load(f)  # Just try to load it to ensure it's valid JSON
                    
                # If we get here, the file exists and is valid
                logger.info(f"File with identical content already exists: {state.get('file_name')}. Skipping write.")
                filename = state.get("file_name")
                write_file = False
                file_saved = True
            except Exception as e:
                # If there was an error reading the file, we'll need to rewrite it
                logger.warning(f"Existing file with matching hash couldn't be read: {e}. Will rewrite.")
                write_file = True
        else:
            # File doesn't exist, even though we have a hash
            logger.warning(f"Expected file {state.get('file_name')} not found. Will create new file.")

    # If no matching hash or couldn't verify existing file, generate a new filename
    if write_file:
        filename = f"overton_raw_batch_{batch_num}_{datetime.now().strftime('%Y%m%d')}.json"
        file_path = os.path.join(output_dir, filename)
        
        try:
            # Write the file
            with open(file_path, 'w') as f:
                json.dump(enriched_data, f, indent=2)
            logger.info(f"Saved raw data to {file_path}")
            file_saved = True
        except Exception as e:
            logger.error(f"Error saving raw data: {e}")
            file_saved = False
            
            # Update state file with file write error
            state_errors = state.get("errors", [])
            state_errors.append({
                "timestamp": datetime.now().isoformat(),
                "error": f"File write error: {str(e)}"
            })
            
            state.update({
                "completed": False,
                "errors": state_errors
            })
            update_state_file(state, state_dir)

    # Only update the state if file was saved or we verified an existing file
    if file_saved:
        state.update({
            "policy_document_ids": policy_document_ids,
            "file_name": filename,
            "file_hash": content_hash,  # Store the hash for future reference
            "completed": True,
            "errors": []
        })
        update_state_file(state, state_dir)
    
    # Record end time
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    logger.info(f"Retrieved policy data for set {set_id} in batch {batch_num} with {len(policy_document_ids)} documents")
    
    # Return metadata about this batch processing
    return {
        "load_id": load_id,
        "batch_num": batch_num,
        "dois": dois_batch,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "success": file_saved,
        "policy_document_ids": policy_document_ids,
        "file_name": filename,
        "set_id": set_id
    }

def get_incomplete_batches(state_dir, dois, load_id, batch_size=MAX_DOIS_PER_SET):
    """
    Identify which batches need processing or reprocessing.
    
    Args:
        state_dir (str): Directory where state files are saved
        dois (list): List of all DOIs
        load_id (str): Unique identifier for this processing batch
        batch_size (int): Size of each batch
        
    Returns:
        list: List of tuples (batch_num, dois_for_batch) that need processing
    """
    load_id_dir = os.path.join(state_dir, f"load_id_{load_id}")
    
    if not os.path.exists(load_id_dir):
        logger.info(f"No state directory exists for load_id {load_id}. All batches need processing.")
        return [(i+1, batch) for i, batch in enumerate(chunk_list(dois, batch_size))]
    
    # Create batches
    batches = list(chunk_list(dois, batch_size))
    
    # Track batches with completed state
    completed_batches = set()
    
    # Check each batch for a completed state
    for i, _ in enumerate(batches):
        batch_num = i + 1
        state_file = os.path.join(load_id_dir, f"batch_{batch_num}_state.json")
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    # If state is marked as completed, add to completed batches
                    if state.get("completed", False):
                        completed_batches.add(batch_num)
            except Exception as e:
                logger.error(f"Error reading state file for batch {batch_num}: {e}")
    
    # Batches that are not marked as completed need processing
    incomplete_batches = [(i+1, batch) for i, batch in enumerate(batches) if i+1 not in completed_batches]
    
    logger.info(f"Found {len(incomplete_batches)} out of {len(batches)} batches requiring processing.")
    return incomplete_batches

def process_dois_in_batches(dois: List[str], output_dir: str, load_id: str, 
                       max_retries=MAX_RETRIES, state_dir=STATE_DIR, force_reprocess=False) -> List[Dict]:
    """
    Process DOIs in batches of MAX_DOIS_PER_SET with state tracking and idempotent storage.
    
    Args:
        dois: List of DOIs to process
        output_dir: Directory to save output files
        load_id: Unique identifier for this processing batch
        max_retries: Maximum number of retry attempts
        state_dir: Directory where state files are saved
        force_reprocess: If True, reprocess all batches regardless of state
        
    Returns:
        List of batch processing metadata
    """
    # Determine which batches need processing
    if force_reprocess:
        batches_to_process = [(i+1, batch) for i, batch in enumerate(chunk_list(dois, MAX_DOIS_PER_SET))]
        logger.info("Force reprocessing all batches")
    else:
        batches_to_process = get_incomplete_batches(state_dir, dois, load_id, MAX_DOIS_PER_SET)
    
    batch_count = len(batches_to_process)
    logger.info(f"Processing {len(dois)} DOIs in {batch_count} batches")
    
    # Process each batch
    batch_results = []
    
    for i, (batch_num, batch_dois) in enumerate(batches_to_process):
        logger.info(f"\nProcessing batch {batch_num} ({i+1}/{batch_count}, {len(batch_dois)} DOIs)")
        
        # Process batch and collect metadata using our overton_policy_lookup function
        batch_data = overton_policy_lookup(
            batch_dois,
            output_dir,
            batch_num,
            load_id,
            max_retries,
            state_dir
        )
        batch_results.append(batch_data)
        
        # Add a delay between batches based on rate limits in config
        if i < batch_count - 1:
            rate_delay = 1 / config.get('rate_limits', {}).get('requests_per_second', 1)
            logger.info(f"Waiting {rate_delay} seconds before next batch...")
            time.sleep(rate_delay)
    
    # For a complete manifest, load data for already processed batches
    all_batches = list(chunk_list(dois, MAX_DOIS_PER_SET))
    all_batch_nums = set(range(1, len(all_batches) + 1))
    processed_batch_nums = set(data["batch_num"] for data in batch_results)
    
    # Find batches that were already processed before this run
    for batch_num in all_batch_nums - processed_batch_nums:
        state = create_or_load_state_file(batch_num, [], load_id, state_dir)
        if state.get("completed", False) and validate_batch_file(batch_num, output_dir, load_id, state_dir):
            batch_results.append({
                "batch_num": batch_num,
                "dois": state.get("dois", []),
                "start_time": state.get("last_updated", datetime.now().isoformat()),
                "end_time": state.get("last_updated", datetime.now().isoformat()),
                "duration_seconds": 0,
                "success": True,
                "policy_document_ids": state.get("policy_document_ids", []),
                "file_name": state.get("file_name"),
                "set_id": state.get("set_id"),
                "load_id": load_id
            })
    
    return batch_results

def generate_manifest(batch_results, input_file, output_dir: str, manifest_dir: str = MANIFEST_DIR, load_id=None) -> str:
    """
    Generate and save a manifest file containing metadata about each batch processed.
    
    Args:
        batch_results: List of metadata dictionaries from batch processing
        input_file: Path to the input file used
        output_dir: Directory where the raw data files are saved
        manifest_dir: Base directory for storing manifest files in date partitions
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
    manifest_file = os.path.join(manifest_date_dir, f"overton_manifest_{datetime.now().strftime('%Y%m%d')}.json")
    
    # Calculate overall metrics
    successful_batches = [batch for batch in batch_results if batch.get("success", False)]
    total_dois = sum(len(batch["dois"]) for batch in batch_results)
    
    # Count unique policy documents - first from batch_results
    all_policy_document_ids = []
    for batch in batch_results:
        all_policy_document_ids.extend(batch.get("policy_document_ids", []))
    
    # Count DOIs with citations
    dois_with_citations = set()
    
    # Create the manifest dictionary
    manifest = {
        "manifest_generated": datetime.now().isoformat(),
        "load_id": load_id,
        "total_batches_processed": len(batch_results),
        "successful_batches": len(successful_batches),
        "input_file": os.path.basename(input_file),
        "processing_summary": {
            "start_time": min([batch["start_time"] for batch in batch_results], default=None),
            "end_time": max([batch["end_time"] for batch in batch_results], default=None),
            "total_dois": total_dois,
            "total_policy_documents": 0  # Will update this after examining files
        },
        "batches": []
    }
    
    # Calculate processing duration if we have both start and end times
    if manifest["processing_summary"]["start_time"] and manifest["processing_summary"]["end_time"]:
        start = datetime.fromisoformat(manifest["processing_summary"]["start_time"])
        end = datetime.fromisoformat(manifest["processing_summary"]["end_time"])
        duration = (end - start).total_seconds()
        manifest["processing_summary"]["duration_seconds"] = duration
    
    # Add batch-specific metadata and examine files for actual document counts
    total_policy_documents = 0
    for batch in batch_results:
        # Default document count - will be updated from file if possible
        policy_document_count = len(batch.get("policy_document_ids", []))
        
        # Get file content to count policy documents properly
        if batch.get("file_name") and batch.get("success", False):
            file_path = os.path.join(output_dir, batch.get("file_name"))
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        # Count documents in the batch's results
                        if 'raw_response' in data and 'results' in data['raw_response']:
                            policy_document_count = len(data['raw_response']['results'])
                            total_policy_documents += policy_document_count
                            
                            # Count DOIs with citations
                            batch_dois = batch.get("dois", [])
                            if batch_dois:
                                for doc in data['raw_response']['results']:
                                    if "cites" in doc and "scholarly" in doc["cites"]:
                                        for citation in doc["cites"]["scholarly"]:
                                            doi = citation.get("doi")
                                            if doi and doi in batch_dois:
                                                dois_with_citations.add(doi)
                except Exception as e:
                    logger.error(f"Error reading file {file_path} for document count: {e}")
        
        batch_entry = {
            "batch_number": batch["batch_num"],
            "load_id": batch.get("load_id", load_id),
            "set_id": batch.get("set_id"),
            "timestamp": {
                "start_time": batch["start_time"],
                "end_time": batch["end_time"],
                "duration_seconds": batch["duration_seconds"]
            },
            "stats": {
                "doi_count": len(batch["dois"]),
                "policy_document_count": policy_document_count,
                "success": batch.get("success", False)
            },
            "file_name": batch.get("file_name")
        }
        
        if not batch.get("success", False):
            batch_entry["error"] = batch.get("error", "Unknown error")
        
        manifest["batches"].append(batch_entry)
    
    # Update the total policy documents count
    manifest["processing_summary"]["total_policy_documents"] = total_policy_documents
    
    # Add citation statistics to the manifest
    manifest["processing_summary"]["dois_with_citations"] = len(dois_with_citations)
    manifest["processing_summary"]["citation_rate"] = round(len(dois_with_citations) / total_dois * 100, 2) if total_dois > 0 else 0
    
    try:
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifest file generated and saved to {manifest_file}")
        return manifest_file
    except Exception as e:
        logger.error(f"Error saving manifest file: {e}")
        return None

def main():
    """Main function to process DOIs and retrieve policy citation data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Retrieve policy citation data from Overton API for DOIs from a text file")
    parser.add_argument("--input", "-i", default="",
                        help="Path to file containing DOIs (default: auto-detect based on current date)")
    parser.add_argument("--output-dir", "-o", default=OUTPUT_DIR,
                        help=f"Base output directory for raw JSON files (default: {OUTPUT_DIR})")
    parser.add_argument("--manifest-dir", "-m", default=MANIFEST_DIR,
                        help=f"Base directory for manifest files (default: {MANIFEST_DIR})")
    parser.add_argument("--state-dir", "-s", default=STATE_DIR,
                        help=f"Directory for state files (default: {STATE_DIR})")
    parser.add_argument("--retries", "-r", type=int, default=MAX_RETRIES,
                        help=f"Maximum number of retry attempts (default: {MAX_RETRIES})")
    parser.add_argument("--force-reprocess", "-f", action="store_true",
                        help="Force reprocessing of all batches, ignoring existing state")
    parser.add_argument("--load-id", "-l", default=None,
                        help="Load ID to resume processing (if None, a new load_id will be generated)")
    args = parser.parse_args()
    
    # Record script start time
    script_start_time = datetime.now()
    
    # Check if API key is set
    if not OVERTON_API_KEY:
        logger.error("Error: OVERTON_API_KEY environment variable not set")
        exit(1)
    
    # Create partitioned output directory
    output_dir = create_directory_structure(args.output_dir)
    logger.info(f"Output directory: {output_dir}")
    
    # Checks if a load_id was provided as a CLI argument if resuming an existing processing batch
    # If no load_id was provided, generates a new load_id for new processing batch
    load_id = args.load_id if args.load_id else generate_load_id('overton')
    logger.info(f"Using load ID: {load_id}")

    # Determine input file path
    input_file = args.input if args.input else get_input_file_path()
    if not input_file:
        logger.error("No input file found. Exiting.")
        exit(1)
    
    logger.info(f"Input file: {input_file}")
    
    # Read DOIs from the input file
    dois = read_dois_from_file(input_file)
    if not dois:
        logger.error("No DOIs found. Exiting.")
        exit(1)
    
    logger.info(f"Found {len(dois)} DOIs")
    
    # Process DOIs in batches to respect API limits
    # Use the new function with state tracking and idempotent storage
    batch_results = process_dois_in_batches(
        dois, 
        output_dir, 
        load_id,
        args.retries,
        args.state_dir,
        args.force_reprocess
    )
    
    # Generate and save manifest file in the date-partitioned manifest directory
    manifest_file = generate_manifest(
        batch_results, 
        input_file, 
        output_dir, 
        args.manifest_dir, 
        load_id
    )
    
    # Record script end time
    script_end_time = datetime.now()
    script_duration = (script_end_time - script_start_time).total_seconds()
    
    # Calculate summary statistics
    all_policy_document_ids = []
    for batch in batch_results:
        all_policy_document_ids.extend(batch.get("policy_document_ids", []))
    unique_policy_document_ids = set(all_policy_document_ids)
    
    # Print summary
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Total batches processed: {len(batch_results)}")
    logger.info(f"Successful batches: {len([b for b in batch_results if b.get('success', False)])}")
    logger.info(f"Total DOIs processed: {len(dois)}")
    logger.info(f"Total policy documents retrieved: {len(unique_policy_document_ids)}")
    logger.info(f"Total processing time: {script_duration:.2f} seconds")
    
    # Get manifest directory path for logging
    manifest_date_dir = os.path.join(args.manifest_dir, datetime.now().strftime('%Y%m/%d'))
    logger.info(f"Manifest file generated in: {manifest_date_dir}")

if __name__ == "__main__":
    main()