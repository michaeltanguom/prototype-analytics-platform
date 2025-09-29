"""
SciVal Publication Metrics API Ingestion Script
This script ingests bibliometric data from SciVal via the Metrics API and retrieves the full RAW JSON output.
It processes publication IDs from a text file, makes paginated API requests, and stores the raw responses
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
from datetime import datetime, timedelta
import hashlib

# Define paths
PROJECT_ROOT = '/Users/work/Documents/prototype-analytics-platform'
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'apis', 'scival.yaml')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')
INPUT_DIR = os.path.join(DATA_DIR, 'raw', 'api_input')
OUTPUT_DIR = os.path.join(DATA_DIR, 'raw', 'elsevier', 'scival')
MANIFEST_DIR = os.path.join(DATA_DIR, 'raw', 'manifest')
STATE_DIR = os.path.join(DATA_DIR, 'state', 'scival')

# Configure logging
log_file = os.path.join(LOGS_DIR, "scival_publication_metrics_ingestion.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scival_publication_metrics_ingestion")

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
CACHE_DIR = os.path.join(DATA_DIR, 'cache', 'elsevier', 'scival')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info(f"Created cache directory: {CACHE_DIR}")

# Set up request caching
if config.get('cache', {}).get('enabled', True):
    cache_file = os.path.join(CACHE_DIR, 'scival_metrics_cache')
    requests_cache.install_cache(cache_file, expire_after=config.get('cache', {}).get('expiration', 86400))
    logger.info(f"Request caching enabled with expiration of {config.get('cache', {}).get('expiration', 86400)} seconds")

# Get API credentials and endpoints from config
SCOPUS_API_KEY = os.environ.get('SCOPUS_API_KEY')
SCOPUS_INST_ID = os.environ.get('SCOPUS_INST_ID')
SCIVAL_ENDPOINT = config.get('metrics_url', 'https://api.elsevier.com/analytics/scival/publication/metrics')

# Get retry and pagination settings from config
MAX_RETRIES = config.get('retries', {}).get('max_attempts')
RETRY_WAIT = config.get('retries', {}).get('backoff_factor')
MAX_PUBID_PER_REQUEST = 25  # SciVal API limit for publication IDs per request

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
    file_name = f"scopus_publication_ids_{current_date}.txt"
    
    file_path = os.path.join(input_dir_path, file_name)
    
    if not os.path.exists(file_path):
        logger.warning(f"Input file {file_path} does not exist")
    
    return file_path

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

def read_publication_ids(file_path):
    """
    Read publication IDs from a text file, one ID per line.
    
    Args:
        file_path (str): Path to the file containing publication IDs
        
    Returns:
        list: List of publication IDs
    """
    try:
        with open(file_path, 'r') as f:
            ids = [line.strip() for line in f if line.strip()]
        logger.info(f"Read {len(ids)} publication IDs from {file_path}")
        return ids
    except Exception as e:
        logger.error(f"Error reading publication IDs file: {e}")
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

def create_directory_if_not_exists(dir_path):
    """
    Create directory if it doesn't exist.
    
    Args:
        dir_path (str): Path to directory
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def create_or_load_state_file(chunk_num, publication_ids, load_id, state_dir=STATE_DIR):
    """
    Create or load a state file for tracking ingestion progress of a chunk.
    
    Args:
        chunk_num (int): The chunk number
        publication_ids (list): List of publication IDs in this chunk
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Base directory where state files are saved
    
    Returns:
        dict: Current state of ingestion for the chunk
    """
    # Get the load_id directory path
    load_id_dir = get_load_id_path(load_id, state_dir)
    
    # Create the state file path within the load_id directory
    state_file = os.path.join(load_id_dir, f"chunk_{chunk_num}_state.json")
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded existing state for chunk {chunk_num} from load_id directory")
                return state
        except Exception as e:
            logger.error(f"Error loading state file for chunk {chunk_num}: {e}")
    
    # Default state for new or failed state files
    return {
        "chunk_num": chunk_num,
        "publication_ids": publication_ids,
        "load_id": load_id,
        "result_ids": [],
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
    state_file = os.path.join(load_id_dir, f"chunk_{state['chunk_num']}_state.json")
    
    # Update timestamp
    state["last_updated"] = datetime.now().isoformat()
    
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Updated state file for chunk {state['chunk_num']}")
    except Exception as e:
        logger.error(f"Error updating state file: {e}")

def get_incomplete_chunks(state_dir, publication_ids, load_id, chunk_size=MAX_PUBID_PER_REQUEST):
    """
    Identify which chunks need processing or reprocessing.
    
    Args:
        state_dir (str): Directory where state files are saved
        publication_ids (list): List of all publication IDs
        load_id (str): Unique identifier for this processing batch
        chunk_size (int): Size of each chunk
        
    Returns:
        list: List of tuples (chunk_num, publication_ids_for_chunk) that need processing
    """
    load_id_dir = os.path.join(state_dir, load_id)
    
    if not os.path.exists(load_id_dir):
        logger.info(f"No state directory exists for load_id {load_id}. All chunks need processing.")
        return [(i+1, chunk) for i, chunk in enumerate(chunk_list(publication_ids, chunk_size))]
    
    # Create chunks
    chunks = list(chunk_list(publication_ids, chunk_size))
    
    # Track chunks with completed state
    completed_chunks = set()
    
    # Check each chunk for a completed state
    for i, _ in enumerate(chunks):
        chunk_num = i + 1
        state_file = os.path.join(load_id_dir, f"chunk_{chunk_num}_state.json")
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    # If state is marked as completed, add to completed chunks
                    if state.get("completed", False):
                        completed_chunks.add(chunk_num)
            except Exception as e:
                logger.error(f"Error reading state file for chunk {chunk_num}: {e}")
    
    # Chunks that are not marked as completed need processing
    incomplete_chunks = [(i+1, chunk) for i, chunk in enumerate(chunks) if i+1 not in completed_chunks]
    
    logger.info(f"Found {len(incomplete_chunks)} out of {len(chunks)} chunks requiring processing.")
    return incomplete_chunks

def validate_chunk_file(chunk_num, output_dir, load_id, state_dir=STATE_DIR):
    """
    Validate that the file for a chunk exists and contains valid data.
    
    Args:
        chunk_num (int): Chunk number
        output_dir (str): Directory where files are saved
        load_id (str): Unique identifier for this processing batch
        state_dir (str): Directory where state files are saved
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    state = create_or_load_state_file(chunk_num, [], load_id, state_dir)
    
    if not state.get("file_name"):
        logger.info(f"No file found for chunk {chunk_num} in state. Processing required.")
        return False
    
    file_path = os.path.join(output_dir, state["file_name"])
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} does not exist for chunk {chunk_num}")
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

def generate_load_id(source_system='scival'):
    """
    Generate a unique load ID combining source system, date, and timestamp.
    
    Args:
        source_system (str): Identifier for the source system (e.g., 'scopus', 'scival')
        
    Returns:
        str: A unique load ID
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"{source_system}_{timestamp}"

def scival_metrics_lookup(publication_ids_chunk, metrics, output_dir, chunk_num, load_id, max_retries=MAX_RETRIES, state_dir=STATE_DIR):
    """
    Query SciVal for metrics of specific publication IDs with pagination, retry capability,
    and auto-recovery from previous failures.
    
    Args:
        publication_ids_chunk (list): List of publication IDs to query
        metrics (list): List of metrics to retrieve
        output_dir (str): Directory to save raw JSON responses
        chunk_num (int): Current chunk number for naming files
        load_id (str): Unique identifier for this processing batch
        max_retries (int): Maximum number of retry attempts
        state_dir (str): Directory where state files are saved
        
    Returns:
        dict: Metadata about the chunk processing
    """
    # Record start time
    start_time = datetime.now()
    
    # Load existing state or create new one
    state = create_or_load_state_file(chunk_num, publication_ids_chunk, load_id, state_dir)
    
    # Validate existing file
    is_valid = validate_chunk_file(chunk_num, output_dir, load_id, state_dir)
    
    # Check if this chunk was already completely processed and file is valid
    if state.get("completed", False) and is_valid:
        logger.info(f"Chunk {chunk_num} was already completely processed with valid file. Skipping.")
        return {
            "load_id": load_id,
            "chunk_num": chunk_num,
            "publication_ids": publication_ids_chunk,
            "start_time": start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "success": True,
            "result_ids": state.get("result_ids", []),
            "file_name": state.get("file_name"),
            "rate_limit_remaining": "unknown",
        }
    
    credentials = '&insttoken=' + SCOPUS_INST_ID + '&apiKey=' + SCOPUS_API_KEY
    
    # Format metrics and publication IDs for API request
    metrics_param = ','.join(metrics)
    publication_ids_query = ','.join(publication_ids_chunk)
    
    # Get default parameters from config
    params = config.get('default_parameters', {})
    params_str = ''.join([f"&{k}={v}" for k, v in params.items()])
    
    # Construct the URL
    base_url = f"{SCIVAL_ENDPOINT}?publicationIds={publication_ids_query}&byYear=false&yearRange=5yrsAndCurrentAndFuture&metricTypes={metrics_param}{credentials}{params_str}"
    
    retry_count = 0
    success = False
    rate_limit_remaining = "unknown"
    
    # Implement retry mechanism
    while retry_count < max_retries and not success:
        try:
            logger.info(f"Requesting metrics for {len(publication_ids_chunk)} publication IDs (chunk {chunk_num}, attempt {retry_count + 1})")
            response = requests.get(base_url)
            response.raise_for_status()
            
            # Log API rate limit information
            headers = response.headers
            if "X-RateLimit-Limit" in headers and "X-RateLimit-Remaining" in headers:
                logger.info(f'API Limit: {headers.get("X-RateLimit-Limit")}')
                logger.info(f'API Remaining: {headers.get("X-RateLimit-Remaining")}')
                rate_limit_remaining = headers.get("X-RateLimit-Remaining")
            
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
                logger.error(f"Failed to retrieve metrics after {max_retries} attempts")
                
                # Update state file with error
                state_errors = state.get("errors", [])
                state_errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e)
                })
                
                state.update({
                    "completed": False,
                    "errors": state_errors
                })
                update_state_file(state, state_dir)
                
                # Return metadata with error information
                end_time = datetime.now()
                return {
                    "load_id": load_id,
                    "chunk_num": chunk_num,
                    "publication_ids": publication_ids_chunk,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": (end_time - start_time).total_seconds(),
                    "success": False,
                    "error": str(e),
                    "result_ids": [],
                    "file_name": None,
                    "rate_limit_remaining": rate_limit_remaining,
                }
    
    # Extract publication IDs from the response for monitoring
    result_ids = []
    if 'results' in data:
        for item in data['results']:
            publication_info = item.get('publication', {})
            publication_id = str(publication_info.get('id', ''))
            if publication_id:
                result_ids.append(publication_id)
    
    # Add metadata to the response
    enriched_data = {
        'metadata': {
            'load_id': load_id,
            'data_source': 'scival_publication_metrics_api',
            'request_timestamp': datetime.now().isoformat(),
            'query_parameters': {
                'publication_ids': publication_ids_chunk,
                'metrics': metrics,
                'chunk': chunk_num
            },
            'rate_limit_remaining': rate_limit_remaining,
            'publication_count': len(publication_ids_chunk),
            'results_count': len(result_ids)
        },
        'raw_response': data
    }
    
    # Generate a content hash for the data before saving
    content_hash = generate_file_hash(enriched_data)

    # First determine if we need to save a new file
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
        filename = f"scival_metrics_chunk{chunk_num}_{datetime.now().strftime('%Y%m%d')}.json"
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
            "result_ids": result_ids,
            "file_name": filename,
            "file_hash": content_hash,  # Store the hash for future reference
            "completed": True,
            "errors": []
        })
        update_state_file(state, state_dir)
    
    # Record end time
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    logger.info(f"Retrieved metrics for {len(result_ids)} publications in chunk {chunk_num}")
    
    # Return metadata about this chunk processing
    return {
        "load_id": load_id,
        "chunk_num": chunk_num,
        "publication_ids": publication_ids_chunk,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "success": success,
        "result_ids": result_ids,
        "file_name": filename if file_saved else None,
        "rate_limit_remaining": rate_limit_remaining,
    }

def generate_manifest(chunk_results, input_file, metrics, output_dir, manifest_dir=MANIFEST_DIR, load_id=None):
    """
    Generate and save a manifest file containing metadata about each chunk processed.
    
    Args:
        chunk_results: List of metadata dictionaries from chunk processing
        input_file: Path to the input file used
        metrics: List of metrics that were requested
        output_dir: Directory where the raw data files are saved
        manifest_dir: Base directory for storing manifest files in date partitions
        load_id: Unique identifier for this processing batch
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
    manifest_file = os.path.join(manifest_date_dir, f"scival_manifest_{datetime.now().strftime('%Y%m%d')}.json")
    
    # Calculate overall metrics
    successful_chunks = [chunk for chunk in chunk_results if chunk["success"]]
    total_publication_ids = sum(len(chunk["publication_ids"]) for chunk in chunk_results)
    total_results = sum(len(chunk["result_ids"]) for chunk in chunk_results)
    all_result_ids = []
    for chunk in chunk_results:
        all_result_ids.extend(chunk["result_ids"])
    unique_result_ids = len(set(all_result_ids))
    
    manifest = {
        "manifest_generated": datetime.now().isoformat(),
        'load_id': load_id,
        "total_chunks_processed": len(chunk_results),
        "successful_chunks": len(successful_chunks),
        "input_file": os.path.basename(input_file),
        "metrics_requested": metrics,
        "processing_summary": {
            "start_time": min([chunk["start_time"] for chunk in chunk_results], default=None),
            "end_time": max([chunk["end_time"] for chunk in chunk_results], default=None),
            "total_publication_ids": total_publication_ids,
            "total_results": total_results,
            "unique_results": unique_result_ids,
            "coverage_rate": round(unique_result_ids / total_publication_ids * 100, 2) if total_publication_ids > 0 else 0
        },
        "chunks": []
    }
    
    # Calculate processing duration if we have both start and end times
    if manifest["processing_summary"]["start_time"] and manifest["processing_summary"]["end_time"]:
        start = datetime.fromisoformat(manifest["processing_summary"]["start_time"])
        end = datetime.fromisoformat(manifest["processing_summary"]["end_time"])
        duration = (end - start).total_seconds()
        manifest["processing_summary"]["duration_seconds"] = duration
    
    # Add chunk-specific metadata
    for chunk in chunk_results:
        chunk_entry = {
            "chunk_number": chunk["chunk_num"],
            "load_id": chunk.get("load_id", load_id),
            "timestamp": {
                "start_time": chunk["start_time"],
                "end_time": chunk["end_time"],
                "duration_seconds": chunk["duration_seconds"]
            },
            "results": {
                "publication_ids_count": len(chunk["publication_ids"]),
                "results_count": len(chunk["result_ids"]),
                "success": chunk["success"]
            },
            "file_name": chunk["file_name"]
        }
        
        if not chunk["success"]:
            chunk_entry["error"] = chunk.get("error", "Unknown error")
        
        manifest["chunks"].append(chunk_entry)
    
    try:
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifest file generated and saved to {manifest_file}")
        return manifest_file
    except Exception as e:
        logger.error(f"Error saving manifest file: {e}")
        return None

def main():
    """Main function to process publication IDs and retrieve metrics data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Retrieve publication metrics from SciVal API based on publication IDs")
    parser.add_argument("--input", "-i", default="",
                        help="Path to file containing Scopus publication IDs (default: auto-detect based on current date)")
    parser.add_argument("--output-dir", "-o", default=OUTPUT_DIR,
                        help=f"Base output directory for raw JSON files (default: {OUTPUT_DIR})")
    parser.add_argument("--manifest-dir", "-m", default=MANIFEST_DIR,
                        help=f"Base directory for manifest files (default: {MANIFEST_DIR})")
    parser.add_argument("--state-dir", "-s", default=STATE_DIR,
                        help=f"Directory for state files (default: {STATE_DIR})")
    parser.add_argument("--retries", "-r", type=int, default=MAX_RETRIES,
                        help=f"Maximum number of retry attempts (default: {MAX_RETRIES})")
    parser.add_argument("--force-reprocess", "-f", action="store_true",
                        help="Force reprocessing of all chunks, ignoring existing state")
    parser.add_argument("--load-id", "-l", default=None,
                    help="Load ID to resume processing (if None, a new load_id will be generated)")
    args = parser.parse_args()
    
    # Record script start time
    script_start_time = datetime.now()
    
    # Check if API key is set
    if not SCOPUS_API_KEY:
        logger.error("Error: SCOPUS_API_KEY environment variable not set")
        exit(1)
    
    # Create partitioned output directory
    output_dir = create_directory_structure(args.output_dir)
    logger.info(f"Output directory: {output_dir}")
    
    # Checks if a load_id was provided as a CLI argument if resuming an existing processing batch
    # If no load_id was provided, generates a new load_id for new processing batch
    load_id = args.load_id if args.load_id else generate_load_id('scival')
    logger.info(f"Using load ID: {load_id}")

    # Determine input file path
    input_file = args.input if args.input else get_input_file_path()
    logger.info(f"Input file: {input_file}")
    
    # Read publication IDs from the input file
    publication_ids = read_publication_ids(input_file)
    if not publication_ids:
        logger.error("No publication IDs found. Exiting.")
        exit(1)
    
    logger.info(f"Found {len(publication_ids)} publication IDs")
    
    # Get metrics to retrieve from config
    metrics = config.get('metrics')
    logger.info(f"Requesting the following metrics: {', '.join(metrics)}")
    
    # Process publication IDs in chunks to respect API limits
    # Determine which chunks need processing
    if args.force_reprocess:
        chunks_to_process = [(i+1, chunk) for i, chunk in enumerate(chunk_list(publication_ids, load_id, MAX_PUBID_PER_REQUEST))]
        logger.info("Force reprocessing all chunks")
    else:
        chunks_to_process = get_incomplete_chunks(args.state_dir, publication_ids, load_id, MAX_PUBID_PER_REQUEST)
    
    chunk_count = len(chunks_to_process)
    logger.info(f"Processing {len(publication_ids)} publication IDs in {chunk_count} chunks")
    
    # Process each chunk
    chunk_results = []
    
    for i, (chunk_num, id_chunk) in enumerate(chunks_to_process):
        logger.info(f"Processing chunk {chunk_num} ({i+1}/{chunk_count}, {len(id_chunk)} publication IDs)")
        
        # Process chunk and collect metadata
        chunk_data = scival_metrics_lookup(
            id_chunk, 
            metrics, 
            output_dir, 
            chunk_num, 
            load_id,
            args.retries,
            args.state_dir,
        )
        chunk_results.append(chunk_data)
        
        # Add a delay between chunks based on rate limits in config
        if i < chunk_count - 1:
            rate_delay = 1 / config.get('rate_limits', {}).get('requests_per_second', 1)
            logger.info(f"Waiting {rate_delay} seconds before next chunk...")
            time.sleep(rate_delay)
    
    # For a complete manifest, load data for already processed chunks
    all_chunks = list(chunk_list(publication_ids, MAX_PUBID_PER_REQUEST))
    all_chunk_nums = set(range(1, len(all_chunks) + 1))
    processed_chunk_nums = set(data["chunk_num"] for data in chunk_results)
    
    # Find chunks that were already processed before this run
    for chunk_num in all_chunk_nums - processed_chunk_nums:
        state = create_or_load_state_file(chunk_num, [], load_id, args.state_dir)
        if state.get("completed", False) and validate_chunk_file(chunk_num, output_dir, load_id, args.state_dir):
            chunk_results.append({
                "chunk_num": chunk_num,
                "publication_ids": state.get("publication_ids", []),
                "start_time": state.get("last_updated", datetime.now().isoformat()),
                "end_time": state.get("last_updated", datetime.now().isoformat()),
                "duration_seconds": 0,
                "success": True,
                "result_ids": state.get("result_ids", []),
                "file_name": state.get("file_name"),
                "rate_limit_remaining": "unknown",
                "load_id": load_id
            })
    
    # Generate manifest file
    manifest_file = generate_manifest(chunk_results, input_file, metrics, output_dir, args.manifest_dir, load_id)
    
    # Calculate summary statistics
    all_result_ids = []
    for chunk in chunk_results:
        all_result_ids.extend(chunk["result_ids"])
    unique_result_ids = set(all_result_ids)
    
    # Record script end time
    script_end_time = datetime.now()
    script_duration = (script_end_time - script_start_time).total_seconds()
    
    # Print summary
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Total chunks processed: {len(chunk_results)}")
    logger.info(f"Total chunks in manifest: {len(chunk_results)}")
    logger.info(f"Total publication IDs processed: {len(publication_ids)}")
    logger.info(f"Total metrics retrieved: {len(all_result_ids)}")
    logger.info(f"Unique publications with metrics: {len(unique_result_ids)} ({round(len(unique_result_ids)/len(publication_ids)*100, 2)}% coverage)")
    logger.info(f"Total processing time: {script_duration:.2f} seconds")
    
    # Get manifest directory path for logging
    manifest_date_dir = os.path.join(args.manifest_dir, datetime.now().strftime('%Y%m/%d'))
    logger.info(f"Manifest file generated in: {manifest_date_dir}")

if __name__ == "__main__":
    main()