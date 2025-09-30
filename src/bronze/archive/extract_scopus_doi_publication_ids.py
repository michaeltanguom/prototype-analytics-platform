"""
Scopus DOI and Publication ID Extraction Script
This script extracts unique DOIs and EIDs from Scopus Search API JSON responses,
transforms EIDs to Scopus Publication IDs, and writes them to separate files in
a date-partitioned directory structure so that they can be passed on as input for
the Overton and Scival Publication Metrics Retrieval APIs.
"""

import os
import json
import glob
import logging
import argparse
from datetime import datetime

# Define paths consistent with the existing codebase
PROJECT_ROOT = '/Users/work/Documents/prototype-analytics-platform'
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw', 'elsevier', 'scopus')
OUTPUT_DIR = os.path.join(DATA_DIR, 'raw', 'api_input')

# Configure logging
log_file = os.path.join(LOGS_DIR, "scopus_doi_and_pubid_extraction.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scopus_doi_and_pubid_extraction")

def find_json_files(directory, date=None):
    """
    Find all JSON files matching the pattern in the specified YYYYMM/DD directory structure.
    
    Args:
        directory (str): Base directory containing the JSON files in YYYYMM/DD structure
        date (datetime, optional): Specific date to process, defaults to today
        
    Returns:
        list: List of matching file paths
    """
    if date is None:
        date = datetime.now()
    
    date_string = date.strftime('%Y%m%d')
    
    # Search directly in the provided directory which should be YYYYMM/DD
    pattern = os.path.join(directory, f"scopus_search_*_{date_string}.json")
    files = glob.glob(pattern)
    
    logger.info(f"Found {len(files)} JSON files matching the pattern for date {date_string} in {directory}")
    return files

def extract_identifiers(files):
    """
    Extract unique DOIs and EIDs from the JSON files.
    
    Args:
        files (list): List of file paths to process
        
    Returns:
        tuple: Sets of unique DOIs and EIDs
    """
    unique_dois = set()
    unique_eids = set()
    processed_files = 0
    
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
                # Based on the sample JSON provided, navigate the structure
                # First try the structure with metadata wrapper
                if 'raw_response' in data and 'search-results' in data['raw_response'] and 'entry' in data['raw_response']['search-results']:
                    entries = data['raw_response']['search-results']['entry']
                # Fallback to direct structure without metadata wrapper
                elif 'search-results' in data and 'entry' in data['search-results']:
                    entries = data['search-results']['entry']
                else:
                    logger.warning(f"Could not find entry data in file {file_path}")
                    continue
                
                # Process each entry in the data
                for entry in entries:
                    # Extract DOI (using the key found in the sample: 'prism:doi')
                    doi = entry.get('prism:doi')
                    if doi:
                        unique_dois.add(doi)
                    
                    # Extract EID (using the key found in the sample: 'eid')
                    eid = entry.get('eid')
                    if eid:
                        unique_eids.add(eid)
            
            processed_files += 1
            if processed_files % 10 == 0:  # Log every 10 files for larger datasets
                logger.info(f"Processed {processed_files}/{len(files)} files")
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    
    logger.info(f"Successfully processed {processed_files} files")
    logger.info(f"Extracted {len(unique_dois)} unique DOIs and {len(unique_eids)} unique EIDs")
    return unique_dois, unique_eids

def transform_eids(eids):
    """
    Transform EIDs by removing the '2-s2.0-' prefix.
    
    Args:
        eids (set): Set of EIDs to transform
        
    Returns:
        set: Set of transformed Scopus Publication IDs
    """
    scopus_publication_ids = set()
    prefix_count = 0
    
    for eid in eids:
        if eid.startswith('2-s2.0-'):
            scopus_publication_ids.add(eid[7:])  # Remove the '2-s2.0-' prefix
            prefix_count += 1
        else:
            # If the EID doesn't have the expected prefix, keep it as is
            scopus_publication_ids.add(eid)
    
    logger.info(f"Transformed {len(eids)} EIDs to Scopus Publication IDs ({prefix_count} with '2-s2.0-' prefix)")
    return scopus_publication_ids

def create_date_partitioned_directory(base_path):
    """
    Create a date-partitioned directory with format YYYYMM.
    
    Args:
        base_path (str): Base path where the directory structure should be created
        
    Returns:
        str: Full path to the created directory
    """
    # Create current date partition with YYYYMMDD format
    date_folder = datetime.now().strftime('%Y%m')
    
    full_path = os.path.join(base_path, date_folder)
    
    # Create directory if it doesn't exist
    if not os.path.exists(full_path):
        os.makedirs(full_path, exist_ok=True)
        logger.info(f"Created directory: {full_path}")
    else:
        logger.info(f"Directory already exists: {full_path}")
    
    return full_path

def write_to_file(items, file_path):
    """
    Write a set of items to a text file, one item per line.
    
    Args:
        items (set): Set of items to write
        file_path (str): Path to the output file
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            for item in sorted(items):
                file.write(f"{item}\n")
        logger.info(f"Successfully wrote {len(items)} items to {file_path}")
    except Exception as e:
        logger.error(f"Error writing to file {file_path}: {e}")

def main():
    """Main function to extract and transform Scopus identifiers."""
    # Parse command line arguments
    today = datetime.now()
    year_month = today.strftime('%Y%m')
    day = today.strftime('%d')
    default_input_dir = os.path.join(RAW_DATA_DIR, year_month, day)
    
    parser = argparse.ArgumentParser(description="Extract and transform Scopus identifiers from JSON files")
    parser.add_argument("--input-dir", "-i", default=default_input_dir,
                        help=f"Directory containing the JSON files (default: {default_input_dir})")
    parser.add_argument("--output-dir", "-o", default=OUTPUT_DIR,
                        help=f"Base output directory for extracted identifiers (default: {OUTPUT_DIR})")
    parser.add_argument("--date", "-d", 
                        help="Optional specific date to process in YYYYMMDD format (default: today)")
    args = parser.parse_args()
    
    # Process date argument if provided
    process_date = today
    if args.date:
        try:
            process_date = datetime.strptime(args.date, '%Y%m%d')
            year_month = process_date.strftime('%Y%m')
            day = process_date.strftime('%d')
            args.input_dir = os.path.join(RAW_DATA_DIR, year_month, day)
            logger.info(f"Using specified date: {args.date}, input directory: {args.input_dir}")
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Using default: {default_input_dir}")
    
    logger.info("Starting Scopus identifier extraction")
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output base directory: {args.output_dir}")
    
    # Find all matching JSON files
    json_files = find_json_files(args.input_dir, process_date)
    
    if not json_files:
        logger.warning(f"No matching JSON files found in {args.input_dir}. Exiting.")
        return
    
    # Extract unique DOIs and EIDs
    unique_dois, unique_eids = extract_identifiers(json_files)
    
    # Transform EIDs to Scopus Publication IDs
    scopus_publication_ids = transform_eids(unique_eids)
    
    # Create the date-partitioned output directory
    output_directory = create_date_partitioned_directory(args.output_dir)
    
    # Generate output file paths
    current_date = datetime.now().strftime('%Y%m%d')
    scopus_ids_file = os.path.join(output_directory, f"scopus_publication_ids_{current_date}.txt")
    dois_file = os.path.join(output_directory, f"scopus_generated_dois_{current_date}.txt")
    
    # Write the results to files
    write_to_file(scopus_publication_ids, scopus_ids_file)
    write_to_file(unique_dois, dois_file)
    
    logger.info("Scopus identifier extraction completed successfully")

if __name__ == "__main__":
    main()