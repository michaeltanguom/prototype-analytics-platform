"""
Altmetric API Ingestion Script
This script ingests altmetric data via the Counts API and retrieves the full RAW JSON output.
It processes DOIs one at a time from a text file, makes API requests, and stores the raw responses
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

# Define paths
PROJECT_ROOT = '/Users/work/Documents/prototype-analytics-platform'
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'apis', 'altmetric.yaml')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')
INPUT_DIR = os.path.join(DATA_DIR, 'raw', 'api_input')
OUTPUT_DIR = os.path.join(DATA_DIR, 'raw', 'altmetric')
MANIFEST_DIR = os.path.join(DATA_DIR, 'raw', 'manifest')
STATE_DIR = os.path.join(DATA_DIR, 'state', 'altmetric')

# Configure logging
log_file = os.path.join(LOGS_DIR, "altmetric_ingestion.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("altmetric_ingestion")

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
CACHE_DIR = os.path.join(DATA_DIR, 'cache', 'altmetric')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info(f"Created cache directory: {CACHE_DIR}")

# Set up request caching
if config.get('cache', {}).get('enabled', True):
    cache_file = os.path.join(CACHE_DIR, 'altmetric_cache')
    requests_cache.install_cache(cache_file, expire_after=config.get('cache', {}).get('expiration', 86400))
    logger.info(f"Request caching enabled with expiration of {config.get('cache', {}).get('expiration', 86400)} seconds")

# Get API credentials and endpoints from config
ALTMETRIC_API_KEY = os.environ.get('ALTMETRIC_API_KEY')
ALTMETRIC_BASE_URL = config.get('base_url', 'https://api.altmetric.com/v1/doi')

# Get retry settings from config
MAX_RETRIES = config.get('retries', {}).get('max_attempts', 3)
RETRY_WAIT = config.get('retries', {}).get('backoff_factor', 2)

# API request rate
REQUESTS_PER_SECOND = 50

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

# When re-factoring code, have source_system inherit from config files
def generate_load_id(source_system='altmetric'):
    """
    Generate a unique load ID combining source system, date, and timestamp.
    
    Args:
        source_system (str): Identifier for the source system (e.g., 'altmetric')
        
    Returns:
        str: A unique load ID
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"{source_system}_{timestamp}"

def create_or_load_state_file(doi, item_num, load_id, state_dir=STATE_DIR):
    """
    Create or load a state file for tracking ingestion progress of a DOI.
    
    Args:
        doi (str): The DOI
        item_num (int): The item number
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Base directory where state files are saved
    
    Returns:
        dict: Current state of ingestion for the DOI
    """
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    # Create a safe filename for the DOI
    safe_doi = doi.replace('/', '_')
    
    # Create the state file path within the load_id directory
    state_file = os.path.join(load_id_dir, f"doi_{safe_doi}_state.json")
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded existing state for DOI {doi} from load_id directory")
                return state
        except Exception as e:
            logger.error(f"Error loading state file for DOI {doi}: {e}")
    
    # Default state for new or failed state files
    return {
        "item_num": item_num,
        "doi": doi,
        "load_id": load_id,
        "bluesky_count": 0,
        "twitter_count": 0,
        "score": 0,
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
    
    # Get DOI from state
    doi = state.get("doi")
    if not doi:
        logger.error("No DOI in state, cannot update state file")
        return
    
    # Create a safe filename for the DOI
    safe_doi = doi.replace('/', '_')
    
    # Create the state file path
    state_file = os.path.join(load_id_dir, f"doi_{safe_doi}_state.json")
    
    # Update timestamp
    state["last_updated"] = datetime.now().isoformat()
    
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Updated state file for DOI {doi}")
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

def validate_doi_file(doi, output_dir, load_id, state_dir=STATE_DIR):
    """
    Validate that the file for a DOI exists and contains valid data.
    
    Args:
        doi (str): DOI
        output_dir (str): Directory where files are saved
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Directory where state files are saved
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    # Create a temp state for validation - we don't need item_num here
    state = create_or_load_state_file(doi, 0, load_id, state_dir)
    
    if not state.get("file_name"):
        logger.info(f"No file found for DOI {doi} in state. Processing required.")
        return False
    
    file_path = os.path.join(output_dir, state["file_name"])
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} does not exist for DOI {doi}")
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
    
    # Get today's date for filename
    current_date = datetime.now().strftime('%Y%m%d')
    file_name = f"scopus_generated_dois_{current_date}.txt"
    
    file_path = os.path.join(input_dir_path, file_name)
    
    if not os.path.exists(file_path):
        logger.warning(f"Input file {file_path} does not exist")
    
    return file_path

def read_dois(file_path):
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
        logger.info(f"Read {len(dois)} DOIs from {file_path}")
        return dois
    except Exception as e:
        logger.error(f"Error reading DOI file: {e}")
        return []

def altmetric_doi_lookup(doi, output_dir, item_num, load_id, max_retries=MAX_RETRIES, state_dir=STATE_DIR):
    """
    Query Altmetric for metrics of a specific DOI with retry capability and state tracking.
    
    Args:
        doi (str): DOI to query
        output_dir (str): Directory to save raw JSON response
        item_num (int): Current item number for naming files
        load_id (str): Unique identifier for this processing batch
        max_retries (int): Maximum number of retry attempts
        state_dir (str): Directory where state files are saved
        
    Returns:
        dict: Metadata about the processing
    """
    # Record start time
    start_time = datetime.now()
    
    # Load existing state or create new one
    state = create_or_load_state_file(doi, item_num, load_id, state_dir)
    
    # Check if this DOI was already completely processed and file is valid
    if state.get("completed", False) and validate_doi_file(doi, output_dir, load_id, state_dir):
        logger.info(f"DOI {doi} was already completely processed with valid file. Skipping.")
        return {
            "load_id": load_id,
            "item_num": item_num,
            "doi": doi,
            "start_time": start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "success": True,
            "bluesky_count": state.get("bluesky_count", 0),
            "twitter_count": state.get("twitter_count", 0),
            "score": state.get("score", 0),
            "file_name": state.get("file_name"),
            "has_metrics": bool(state.get("bluesky_count", 0) or state.get("twitter_count", 0) or state.get("score", 0))
        }
    
    # Construct the URL
    url = f"{ALTMETRIC_BASE_URL}/{doi}"
    if ALTMETRIC_API_KEY:
        url += f"?key={ALTMETRIC_API_KEY}"
    
    retry_count = 0
    success = False
    response_data = None
    error_message = None
    
    # Implement retry mechanism
    while retry_count < max_retries and not success:
        try:
            logger.info(f"Requesting metrics for DOI {doi} (item {item_num}, attempt {retry_count + 1})")
            response = requests.get(url)
            
            # Handle 404s (DOI not found in Altmetric)
            if response.status_code == 404:
                logger.warning(f"DOI {doi} not found in Altmetric (item {item_num})")
                error_message = "DOI not found in Altmetric"
                break
            
            # For other status codes, raise an exception
            response.raise_for_status()
            
            # Parse the data
            response_data = response.json()
            success = True
            
        except requests.exceptions.RequestException as e:
            retry_count += 1
            error_message = str(e)
            logger.error(f"Error: {e}. Retry {retry_count}/{max_retries}")
            if retry_count < max_retries:
                wait_time = RETRY_WAIT ** retry_count  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to retrieve metrics after {max_retries} attempts")
    
    # Record end time
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    # Add metadata to the response
    enriched_data = {
        'metadata': {
            'load_id': load_id,
            'data_source': 'altmetric_api',
            'request_timestamp': datetime.now().isoformat(),
            'query_parameters': {
                'doi': doi,
                'item_num': item_num
            },
            'success': success,
            'error': error_message if not success else None,
            'duration_seconds': duration_seconds
        },
        'raw_response': response_data
    }
    
    # Generate a content hash for the data before saving
    content_hash = generate_file_hash(enriched_data)
    
    # Extract metrics from the response
    bluesky_count = response_data.get('cited_by_bluesky_count', 0) if success else 0
    twitter_count = response_data.get('cited_by_tweeters_count', 0) if success else 0
    score = response_data.get('score', 0) if success else 0
    
    # Save the raw response to the partitioned output directory (only if we got data)
    file_name = None
    file_saved = False
    write_file = True
    
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
                file_name = state.get("file_name")
                write_file = False
                file_saved = True
            except Exception as e:
                # If there was an error reading the file, we'll need to rewrite it
                logger.warning(f"Existing file with matching hash couldn't be read: {e}. Will rewrite.")
                write_file = True
        else:
            # File doesn't exist, even though we have a hash
            logger.warning(f"Expected file {state.get('file_name')} not found. Will create new file.")
    
    # If no matching hash or couldn't verify existing file, generate a new filename and save
    if write_file and success:
        file_name = f"altmetric_doi_{doi.replace('/', '_')}_{datetime.now().strftime('%Y%m%d')}.json"
        file_path = os.path.join(output_dir, file_name)
        
        try:
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
    if success and (file_saved or not write_file):
        state.update({
            "bluesky_count": bluesky_count,
            "twitter_count": twitter_count,
            "score": score,
            "file_name": file_name,
            "file_hash": content_hash,  # Store the hash for future reference
            "completed": True,
            "errors": []
        })
        update_state_file(state, state_dir)
    elif not success:
        # Update state with the error
        state_errors = state.get("errors", [])
        state_errors.append({
            "timestamp": datetime.now().isoformat(),
            "error": error_message
        })
        state["errors"] = state_errors
        update_state_file(state, state_dir)
    
    # Return metadata about this processing
    return {
        "load_id": load_id,
        "item_num": item_num,
        "doi": doi,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "success": success,
        "error": error_message if not success else None,
        "bluesky_count": bluesky_count,
        "twitter_count": twitter_count,
        "score": score,
        "file_name": file_name,
        "has_metrics": bool(bluesky_count or twitter_count or score) if success else False
    }

def get_incomplete_dois(dois, load_id, state_dir=STATE_DIR):
    """
    Identify which DOIs need processing or reprocessing.
    
    Args:
        dois (list): List of all DOIs
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Directory where state files are saved
        
    Returns:
        list: List of DOIs that need processing
    """
    load_id_dir = os.path.join(state_dir, f"load_id_{load_id}")
    
    if not os.path.exists(load_id_dir):
        logger.info(f"No state directory exists for load_id {load_id}. All DOIs need processing.")
        return dois
    
    # Track DOIs with completed state
    completed_dois = set()
    
    # Check each DOI for a completed state
    for doi in dois:
        safe_doi = doi.replace('/', '_')
        state_file = os.path.join(load_id_dir, f"doi_{safe_doi}_state.json")
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    # If state is marked as completed, add to completed DOIs
                    if state.get("completed", False):
                        completed_dois.add(doi)
            except Exception as e:
                logger.error(f"Error reading state file for DOI {doi}: {e}")
    
    # DOIs that are not marked as completed need processing
    incomplete_dois = [doi for doi in dois if doi not in completed_dois]
    
    logger.info(f"Found {len(incomplete_dois)} out of {len(dois)} DOIs requiring processing.")
    return incomplete_dois

def process_dois_sequentially(dois, output_dir, load_id, requests_per_second=REQUESTS_PER_SECOND, 
                              max_retries=MAX_RETRIES, state_dir=STATE_DIR, force_reprocess=False):
    """
    Process DOIs one at a time with controlled request rate, state tracking, and idempotent storage.
    
    Args:
        dois (list): List of DOIs to process
        output_dir (str): Directory to save raw responses
        load_id (str): Unique identifier for this processing batch
        requests_per_second (int): Number of requests to make per second
        max_retries (int): Maximum number of retry attempts
        state_dir (str): Directory where state files are saved
        force_reprocess (bool): If True, reprocess all DOIs regardless of state
        
    Returns:
        list: List of processing results
    """
    # Determine which DOIs need processing
    if force_reprocess:
        dois_to_process = dois
        logger.info("Force reprocessing all DOIs")
    else:
        dois_to_process = get_incomplete_dois(dois, load_id, state_dir)
    
    results = []
    total_dois = len(dois)
    dois_to_process_count = len(dois_to_process)
    
    logger.info(f"Processing {dois_to_process_count} of {total_dois} DOIs sequentially at {requests_per_second} requests per second")
    
    # Calculate delay between requests
    delay = 1.0 / requests_per_second
    
    # Create a DOI to item_num mapping for all DOIs
    doi_to_item_num = {doi: i+1 for i, doi in enumerate(dois)}
    
    # Process DOIs that need processing
    for i, doi in enumerate(dois_to_process):
        item_num = doi_to_item_num[doi]
        start_time = time.time()
        
        # Process the DOI
        result = altmetric_doi_lookup(doi, output_dir, item_num, load_id, max_retries, state_dir)
        results.append(result)
        
        # Calculate how long to wait before the next request
        processing_time = time.time() - start_time
        wait_time = max(0, delay - processing_time)
        
        # Progress reporting
        if i % 100 == 0 or i == dois_to_process_count - 1:
            logger.info(f"Processed {i+1}/{dois_to_process_count} DOIs ({((i+1)/dois_to_process_count*100):.1f}%)")
        
        # Only wait if this is not the last item
        if i < dois_to_process_count - 1 and wait_time > 0:
            time.sleep(wait_time)
    
    # For a complete manifest, load data for DOIs that were already processed
    processed_dois = set(result["doi"] for result in results)
    
    # Find DOIs that were already processed before this run
    for doi in dois:
        if doi not in processed_dois:
            # Create temp state for rehydrating results (we don't need item_num here)
            state = create_or_load_state_file(doi, 0, load_id, state_dir)
            if state.get("completed", False) and validate_doi_file(doi, output_dir, load_id, state_dir):
                item_num = doi_to_item_num[doi]
                results.append({
                    "load_id": load_id,
                    "item_num": item_num,
                    "doi": doi,
                    "start_time": state.get("last_updated", datetime.now().isoformat()),
                    "end_time": state.get("last_updated", datetime.now().isoformat()),
                    "duration_seconds": 0,
                    "success": True,
                    "bluesky_count": state.get("bluesky_count", 0),
                    "twitter_count": state.get("twitter_count", 0),
                    "score": state.get("score", 0),
                    "file_name": state.get("file_name"),
                    "has_metrics": bool(state.get("bluesky_count", 0) or state.get("twitter_count", 0) or state.get("score", 0))
                })
    
    # Sort results by item_num to maintain original order
    results.sort(key=lambda x: x["item_num"])
    
    return results

def generate_manifest(results, input_file, output_dir, manifest_dir=MANIFEST_DIR, load_id=None):
    """
    Generate and save a manifest file containing metadata about each DOI processed.
    
    Args:
        results: List of metadata dictionaries from DOI processing
        input_file: Path to the input file used
        output_dir: Directory where the raw data files are saved
        manifest_dir: Base directory for storing manifest files in date partitions
        load_id: Unique identifier for this processing batch
        
    Returns:
        str: Path to the generated manifest file
    """
    # Create date-partitioned directory for manifest file
    current_year_month = datetime.now().strftime('%Y%m')
    current_day = datetime.now().strftime('%d')
    manifest_date_dir = os.path.join(manifest_dir, current_year_month, current_day)
    
    # Create directory if it doesn't exist
    if not os.path.exists(manifest_date_dir):
        os.makedirs(manifest_date_dir, exist_ok=True)
        logger.info(f"Created manifest directory: {manifest_date_dir}")
    
    # Create filename with current date
    manifest_file = os.path.join(manifest_date_dir, f"altmetric_manifest_{datetime.now().strftime('%Y%m%d')}.json")
    
    # Calculate overall metrics
    successful_requests = [r for r in results if r["success"]]
    total_dois = len(results)
    dois_with_metrics = sum(1 for r in results if r.get("has_metrics", False))
    total_bluesky_mentions = sum(r.get("bluesky_count", 0) for r in results)
    total_twitter_mentions = sum(r.get("twitter_count", 0) for r in results)
    
    manifest = {
        "manifest_generated": datetime.now().isoformat(),
        "load_id": load_id,
        "total_dois_processed": total_dois,
        "successful_requests": len(successful_requests),
        "input_file": os.path.basename(input_file),
        "processing_summary": {
            "start_time": min([r["start_time"] for r in results], default=None),
            "end_time": max([r["end_time"] for r in results], default=None),
            "total_dois": total_dois,
            "dois_with_metrics": dois_with_metrics,
            "coverage_rate": round(dois_with_metrics / total_dois * 100, 2) if total_dois > 0 else 0,
            "total_bluesky_mentions": total_bluesky_mentions,
            "total_twitter_mentions": total_twitter_mentions
        },
        "items": []
    }
    
    # Calculate processing duration if we have both start and end times
    if manifest["processing_summary"]["start_time"] and manifest["processing_summary"]["end_time"]:
        start = datetime.fromisoformat(manifest["processing_summary"]["start_time"])
        end = datetime.fromisoformat(manifest["processing_summary"]["end_time"])
        duration = (end - start).total_seconds()
        manifest["processing_summary"]["duration_seconds"] = duration
    
    # Add item-specific metadata
    for result in results:
        item_entry = {
            "item_number": result["item_num"],
            "doi": result["doi"],
            "timestamp": {
                "start_time": result["start_time"],
                "end_time": result["end_time"],
                "duration_seconds": result["duration_seconds"]
            },
            "results": {
                "success": result["success"],
                "has_metrics": result.get("has_metrics", False),
                "bluesky_count": result.get("bluesky_count", 0),
                "twitter_count": result.get("twitter_count", 0),
                "score": result.get("score", 0)
            },
            "file_name": result["file_name"]
        }
        
        if not result["success"]:
            item_entry["error"] = result.get("error", "Unknown error")
        
        manifest["items"].append(item_entry)
    
    try:
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifest file generated and saved to {manifest_file}")
        return manifest_file
    except Exception as e:
        logger.error(f"Error saving manifest file: {e}")
        return None

def main():
    """Main function to process DOIs and retrieve altmetric data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Retrieve altmetric data based on DOIs")
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
    parser.add_argument("--rate", type=int, default=REQUESTS_PER_SECOND,
                        help=f"Requests per second (default: {REQUESTS_PER_SECOND})")
    parser.add_argument("--force-reprocess", "-f", action="store_true",
                        help="Force reprocessing of all DOIs, ignoring existing state")
    parser.add_argument("--load-id", "-l", default=None,
                        help="Load ID to resume processing (if None, a new load_id will be generated)")
    args = parser.parse_args()
    
    # Record script start time
    script_start_time = datetime.now()
    
    # Check if API key is set
    if not ALTMETRIC_API_KEY:
        logger.error("Error: ALTMETRIC_API_KEY environment variable not set")
        exit(1)
    
    # Create partitioned output directory
    output_dir = create_directory_structure(args.output_dir)
    logger.info(f"Output directory: {output_dir}")
    
    # Checks if a load_id was provided as a CLI argument if resuming an existing processing batch
    # If no load_id was provided, generates a new load_id for new processing batch
    load_id = args.load_id if args.load_id else generate_load_id('altmetric')
    logger.info(f"Using load ID: {load_id}")
    
    # Determine input file path
    input_file = args.input if args.input else get_input_file_path()
    logger.info(f"Input file: {input_file}")
    
    # Read DOIs from the input file
    dois = read_dois(input_file)
    if not dois:
        logger.error("No DOIs found. Exiting.")
        exit(1)
    
    logger.info(f"Found {len(dois)} DOIs")
    
    # Process DOIs sequentially with rate limiting, state tracking, and idempotent storage
    results = process_dois_sequentially(
        dois, 
        output_dir, 
        load_id,
        requests_per_second=args.rate, 
        max_retries=args.retries,
        state_dir=args.state_dir,
        force_reprocess=args.force_reprocess
    )
    
    # Generate manifest file
    manifest_file = generate_manifest(
        results, 
        input_file, 
        output_dir, 
        args.manifest_dir,
        load_id
    )
    
    # Record script end time
    script_end_time = datetime.now()
    script_duration = (script_end_time - script_start_time).total_seconds()
    
    # Calculate summary statistics
    successful_requests = len([r for r in results if r["success"]])
    dois_with_metrics = sum(1 for r in results if r.get("has_metrics", False))
    
    # Print summary
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Total DOIs processed: {len(dois)}")
    logger.info(f"Successful requests: {successful_requests} ({round(successful_requests/len(dois)*100, 2)}%)")
    logger.info(f"DOIs with metrics: {dois_with_metrics} ({round(dois_with_metrics/len(dois)*100, 2)}%)")
    logger.info(f"Total processing time: {script_duration:.2f} seconds")
    
    # Get manifest directory path for logging
    manifest_date_dir = os.path.join(args.manifest_dir, datetime.now().strftime('%Y%m'), datetime.now().strftime('%d'))
    logger.info(f"Manifest file generated in: {manifest_date_dir}")

if __name__ == "__main__":
    main()