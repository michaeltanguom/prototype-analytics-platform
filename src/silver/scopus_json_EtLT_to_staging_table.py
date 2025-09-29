"""
Scopus EtLT Pipeline - takes JSON files and performs transformations, dumping tabular data into staging table
Run soda_dq.py afterwards for health check on dumped data
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import duckdb
import pandas as pd
import math
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScopusEtLTConfig:
    """Configuration for the Scopus EtLT pipeline"""
    
    def __init__(self):
        self.source_path = Path("/Users/work/Documents/prototype-analytics-platform/src/silver/POC")
        self.db_path = Path("scopus_test.db")
        self.schema_path = Path("stg_schema.sql")

# ========================================
# PURE TRANSFORMATION FUNCTIONS
# ========================================

def remove_fa_keys_recursive(obj):
    """Recursively remove @_fa keys from nested structures"""
    if isinstance(obj, dict):
        return {k: remove_fa_keys_recursive(v) for k, v in obj.items() if k != '@_fa'}
    elif isinstance(obj, list):
        return [remove_fa_keys_recursive(item) for item in obj]
    else:
        return obj

def transform_array_structure(obj, field_name):
    """Transform complex structures to simple arrays"""
    if field_name == 'scopus_affiliation_id' and isinstance(obj, list):
        result = []
        for item in obj:
            if isinstance(item, dict) and '$' in item:
                result.append(item['$'])
            else:
                result.append(item)
        return result
    
    elif field_name == 'open_access_status' and isinstance(obj, dict):
        if 'value' in obj and isinstance(obj['value'], list):
            result = []
            for item in obj['value']:
                if isinstance(item, dict) and '$' in item:
                    result.append(item['$'])
                else:
                    result.append(item)
            return result
        else:
            return obj
    
    elif field_name == 'authkeywords' and isinstance(obj, str):
        stripped_value = obj.strip()
        if stripped_value == '' or stripped_value.lower() == 'undefined':
            return 'null'
        
        keywords = [keyword.strip() for keyword in obj.split('|')]
        keywords = [keyword for keyword in keywords if keyword]
        
        return keywords if keywords else 'null'
    
    else:
        return obj

def transform_field_keys(obj, parent_key=None):
    """Recursively transform field keys with context-aware mappings"""
    base_field_mapping = {
        'affiliation-url': 'scopus_affiliation_url',
        'afid': 'scopus_affiliation_id',
        'affilname': 'scopus_affiliation_name', 
        'affiliation-city': 'scopus_affiliation_city',
        'affiliation-country': 'scopus_affiliation_country',
        '@seq': 'author_list_position',
        'author-url': 'scopus_author_page_url',
        'authid': 'scopus_author_id',
        'authkeywords': 'publication_keywords',
        'freetoreadLabel': 'open_access_status'
    }
    
    context_sensitive_mappings = {
        'author-count': {
            '@limit': 'scopus_exported_authors_limit',
            '@total': 'scopus_total_authors_count',
            '$': 'scopus_exported_authors_count'
        }
    }
    
    if isinstance(obj, dict):
        transformed_dict = {}
        for key, value in obj.items():
            new_key = base_field_mapping.get(key, key)
            
            if parent_key in context_sensitive_mappings:
                context_mapping = context_sensitive_mappings[parent_key]
                new_key = context_mapping.get(key, new_key)
            
            transformed_value = transform_field_keys(value, new_key)
            
            if new_key == 'scopus_affiliation_id' and parent_key == 'author':
                transformed_dict[new_key] = transform_array_structure(transformed_value, 'scopus_affiliation_id')
            elif new_key == 'open_access_status':
                transformed_dict[new_key] = transform_array_structure(transformed_value, 'open_access_status')
            elif key == 'authkeywords':
                transformed_dict[new_key] = transform_array_structure(value, 'authkeywords')
            else:
                transformed_dict[new_key] = transformed_value
        
        return transformed_dict
    elif isinstance(obj, list):
        return [transform_field_keys(item, parent_key) for item in obj]
    else:
        return obj

def clean_and_transform_entry(entry):
    """Apply cleaning and transformation operations to an entry"""
    cleaned_entry = remove_fa_keys_recursive(entry)
    transformed_entry = transform_field_keys(cleaned_entry)
    return transformed_entry

def transform_single_author_name(author: Dict) -> Dict:
    """Transform a single author by adding author_name field"""
    if not isinstance(author, dict):
        return author
    
    given_name = author.get('given-name', '') or ''
    surname = author.get('surname', '') or ''
    
    transformed_author = author.copy()
    transformed_author['author_name'] = f"{given_name} {surname}".strip()
    
    return transformed_author

def transform_authors_list(authors: List[Dict]) -> List[Dict]:
    """Transform a list of authors by adding author_name to each"""
    if not isinstance(authors, list):
        return authors
    
    return [transform_single_author_name(author) for author in authors]

def transform_authors_json_string(authors_json: str) -> str:
    """Transform authors from JSON string to JSON string with author_name added"""
    try:
        authors = json.loads(authors_json) if isinstance(authors_json, str) else authors_json
        transformed_authors = transform_authors_list(authors)
        return json.dumps(transformed_authors)
    except (json.JSONDecodeError, TypeError):
        return authors_json

def process_author_records(records: List[Tuple]) -> List[Dict]:
    """Process author records by transforming the authors field"""
    updates = []
    
    for load_id, eid, authors_json in records:
        try:
            transformed_authors_json = transform_authors_json_string(authors_json)
            
            updates.append({
                'load_id': load_id,
                'eid': eid,
                'transformed_authors': transformed_authors_json
            })
            
        except Exception as e:
            logger.warning(f"Error transforming authors for eid {eid}: {e}")
            continue
    
    return updates

def get_null_normalisation_updates(columns: List[str], table_name: str = "silver_stg_scopus", 
                                  null_values: List[str] = None) -> List[str]:
    """Generate SQL statements for null normalisation"""
    if null_values is None:
        null_values = ['None', 'null', 'undefined', '', '[]', '{}']
    
    null_value_list = ', '.join([f"'{val}'" for val in null_values])
    
    return [
        f"""UPDATE {table_name} 
            SET {column_name} = NULL 
            WHERE {column_name} IS NULL 
               OR TRIM(CAST({column_name} AS VARCHAR)) IN ({null_value_list})"""
        for column_name in columns
        if column_name not in ['load_id', 'data_source', 'eid']
    ]

def prepare_scopus_data_for_loading(scopus_data: List[Dict]) -> List[Dict]:
    """Pre-process scopus data for database loading"""
    processed_data = []
    for record in scopus_data:
        processed_record = record.copy()
        for field in ['author', 'affiliation', 'author-count', 'freetoread']:
            if field in processed_record and processed_record[field] is not None:
                processed_record[field] = json.dumps(processed_record[field])
        processed_data.append(processed_record)
    
    return processed_data

def prepare_audit_data_for_loading(audit_data: List[Dict]) -> List[Dict]:
    """Pre-process audit data for database loading"""
    processed_audit_data = []
    for record in audit_data:
        processed_record = record.copy()
        
        for field in ['query_parameters', 'payload_files', 'payload_results']:
            if field in processed_record and processed_record[field] is not None:
                processed_record[field] = json.dumps(processed_record[field])
        
        processed_audit_data.append(processed_record)
    
    return processed_audit_data

def build_scopus_load_sql(table_name: str = "silver_stg_scopus", temp_table_name: str = "temp_scopus_json") -> str:
    """Build SQL for loading scopus data"""
    return f"""
    INSERT INTO {table_name} (
        load_id,
        data_source,
        eid,
        title,
        first_author,
        journal_name,
        issn,
        eissn,
        journal_volume,
        journal_issue,
        journal_page_range,
        publication_date,
        doi,
        abstract,
        citations,
        affiliations,
        pubmed_id,
        publishing_format,
        document_type,
        author_count,
        authors,
        publication_keywords,
        funder_source_id,
        funder_acronym,
        funder_number,
        funder_name,
        open_access_flag,
        open_access_status
    )
    SELECT 
        CAST(load_id AS STRING),
        CAST(data_source AS STRING),
        CAST(eid AS STRING),
        CAST("dc:title" AS STRING) as title,
        CAST("dc:creator" AS STRING) as first_author,
        CAST("prism:publicationName" AS STRING) as journal_name,
        CAST("prism:issn" AS STRING) as issn,
        CAST("prism:eIssn" AS STRING) as eissn,
        TRY_CAST("prism:volume" AS INTEGER) as journal_volume,
        CAST("prism:issueIdentifier" AS STRING) as journal_issue,
        CAST("prism:pageRange" AS STRING) as journal_page_range,
        TRY_CAST("prism:coverDate" AS DATE) as publication_date,
        CAST("prism:doi" AS STRING) as doi,
        CAST("dc:description" AS STRING) as abstract,
        TRY_CAST("citedby-count" AS INTEGER) as citations,
        CAST(affiliation AS JSON) as affiliations,
        TRY_CAST("pubmed-id" AS BIGINT) as pubmed_id,
        CAST("prism:aggregationType" AS STRING) as publishing_format,
        CAST(subtypeDescription AS STRING) as document_type,
        CAST("author-count" AS JSON) as author_count,
        CAST(author AS JSON) as authors,
        CAST(publication_keywords AS JSON),
        CAST("source-id" AS STRING) as funder_source_id,
        CAST("fund-acr" AS STRING) as funder_acronym,
        CAST("fund-no" AS STRING) as funder_number,
        CAST("fund-sponsor" AS STRING) as funder_name,
        CAST(openaccessFlag AS BOOLEAN) as open_access_flag,
        CAST(open_access_status AS JSON)
    FROM {temp_table_name}
    """

def build_audit_load_sql(table_name: str = "silver_temp_audit", temp_table_name: str = "temp_audit_json") -> str:
    """Build SQL for loading audit data to silver_temp_audit"""
    return f"""
    INSERT INTO {table_name} (
        load_id,
        data_source,
        ingest_timestamp,
        query_parameters,
        audit_schema_version,
        ingestion_time,
        payload_files,
        payload_results,
        audit_schema_hash_value
    )
    SELECT 
        load_id,
        CAST(data_source AS STRING),
        CAST(ingest_timestamp AS TIMESTAMP) as ingest_timestamp,
        CAST(query_parameters AS JSON) as query_parameters,
        audit_schema_version,
        ingestion_time,
        CAST(payload_files AS JSON) as payload_files,
        CAST(payload_results AS JSON) as payload_results,
        audit_schema_hash_value
    FROM {temp_table_name}
    """

def build_author_update_sql(table_name: str = "silver_stg_scopus", temp_table_name: str = "temp_author_updates") -> str:
    """Build SQL for updating author transformations"""
    return f"""
    UPDATE {table_name} 
    SET authors = CAST({temp_table_name}.transformed_authors AS JSON)
    FROM {temp_table_name}
    WHERE {table_name}.load_id = {temp_table_name}.load_id 
    AND {table_name}.eid = {temp_table_name}.eid
    """

def build_truncate_table_sql(table_name: str) -> str:
    """Build SQL statement for truncating any table"""
    return f"TRUNCATE TABLE {table_name}"

def get_unique_load_ids(data_list: List[Dict]) -> List[str]:
    """Extract unique load_ids from data"""
    load_ids = {item.get('load_id') for item in data_list if item.get('load_id')}
    return list(load_ids)

# ========================================
# FILE OPERATIONS
# ========================================

def create_database_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Create and return DuckDB connection"""
    try:
        conn = duckdb.connect(str(db_path))
        logger.info(f"Connected to DuckDB at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def load_json_file(file_path: Path) -> Optional[Dict]:
    """Load and parse a JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded {file_path}")
        return data
    except json.JSONDecodeError as e:
        logger.warning(f"Malformed JSON in {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None

def find_json_files(source_path: Path) -> List[Path]:
    """Find all JSON files excluding manifest files"""
    try:
        all_json_files = list(source_path.glob("*.json"))
        json_files = [f for f in all_json_files if "_manifest_" not in f.name]
        logger.info(f"Found {len(json_files)} JSON files (excluding manifests) in {source_path}")
        return json_files
    except Exception as e:
        logger.error(f"Error finding JSON files: {e}")
        return []

def find_manifest_files(source_path: Path) -> List[Path]:
    """Find all manifest files following pattern: datasourcename_manifest_YYYYMMDD.json"""
    try:
        manifest_files = list(source_path.glob("*_manifest_*.json"))
        logger.info(f"Found {len(manifest_files)} manifest files in {source_path}")
        return manifest_files
    except Exception as e:
        logger.error(f"Error finding manifest files: {e}")
        return []

def get_file_size_kb(base_directory: Path, file_name: str) -> Optional[int]:
    """Get file size in KB for a given file name in the base directory, rounded up to nearest integer"""
    try:
        file_path = base_directory / file_name
        if file_path.exists() and file_path.is_file():
            size_bytes = file_path.stat().st_size
            size_kb = size_bytes / 1024
            size_kb_rounded = math.ceil(size_kb)
            return size_kb_rounded
        else:
            logger.warning(f"File not found: {file_path}")
            return None
    except Exception as e:
        logger.error(f"Error getting file size for {file_name}: {e}")
        return None

# ========================================
# DATABASE OPERATIONS
# ========================================

def execute_schema_ddl(conn: duckdb.DuckDBPyConnection, schema_path: Path) -> None:
    """Execute DDL statements to create tables"""
    try:
        with open(schema_path, 'r') as f:
            ddl_statements = f.read()
        
        statements = [stmt.strip() for stmt in ddl_statements.split(';') if stmt.strip()]
        
        for statement in statements:
            conn.execute(statement)
            logger.info("Executed DDL statement")
        
        logger.info("Schema creation completed")
        
    except Exception as e:
        logger.error(f"Failed to execute schema DDL: {e}")
        raise

def fetch_author_records(conn: duckdb.DuckDBPyConnection) -> List[Tuple]:
    """Fetch records that need author transformation"""
    return conn.execute("""
        SELECT load_id, eid, authors 
        FROM silver_stg_scopus 
        WHERE authors IS NOT NULL
    """).fetchall()

def fetch_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    """Fetch column names for a table"""
    schema_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return [row[0] for row in schema_result]

def apply_author_updates(conn: duckdb.DuckDBPyConnection, updates: List[Dict]) -> None:
    """Apply author transformations to the database"""
    if not updates:
        logger.info("No author updates to apply")
        return
    
    update_df = pd.DataFrame(updates)
    conn.register('temp_author_updates', update_df)
    
    update_sql = build_author_update_sql()
    conn.execute(update_sql)
    
    logger.info(f"Successfully applied {len(updates)} author transformations")

def truncate_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Truncate the specified table"""
    try:
        truncate_sql = build_truncate_table_sql(table_name)
        conn.execute(truncate_sql)
        logger.info(f"Successfully truncated {table_name}")
        
    except Exception as e:
        logger.error(f"Error truncating {table_name}: {e}")
        raise

def load_scopus_data_to_duckdb(conn: duckdb.DuckDBPyConnection, scopus_data: List[Dict]) -> None:
    """Load Scopus data into DuckDB"""
    if not scopus_data:
        logger.info("No Scopus data to load")
        return
    
    try:
        processed_data = prepare_scopus_data_for_loading(scopus_data)
        
        df = pd.DataFrame(processed_data)
        conn.register('temp_scopus_json', df)
        
        load_sql = build_scopus_load_sql()
        conn.execute(load_sql)
        
        logger.info(f"Successfully loaded {len(scopus_data)} records to silver_stg_scopus")
        
    except Exception as e:
        logger.error(f"Error loading Scopus data: {e}")
        raise

def load_audit_data_to_duckdb(conn: duckdb.DuckDBPyConnection, audit_data: List[Dict]) -> None:
    """Load audit data into DuckDB to silver_temp_audit table"""
    if not audit_data:
        logger.info("No audit data to load")
        return
    
    try:
        processed_audit_data = prepare_audit_data_for_loading(audit_data)
        
        df = pd.DataFrame(processed_audit_data)
        conn.register('temp_audit_json', df)
        
        load_sql = build_audit_load_sql()
        conn.execute(load_sql)
        
        logger.info(f"Successfully loaded {len(audit_data)} audit records to silver_temp_audit")
        
        # Verification query
        verification_query = """
        SELECT 
            load_id,
            data_source,
            JSON_ARRAY_LENGTH(payload_files) as file_count,
            JSON_ARRAY_LENGTH(payload_results) as results_count
        FROM silver_temp_audit
        WHERE load_id IN (SELECT DISTINCT load_id FROM temp_audit_json)
        """
        
        verification_results = conn.execute(verification_query).fetchall()
        for load_id, data_source, file_count, results_count in verification_results:
            logger.info(f"Loaded audit record {load_id}: {file_count} files, {results_count} results")
        
    except Exception as e:
        logger.error(f"Error loading audit data: {e}")
        raise

def execute_null_normalisation(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Execute null normalisation using prepared SQL statements"""
    try:
        columns = fetch_table_columns(conn, table_name)
        update_statements = get_null_normalisation_updates(columns, table_name)
        
        for statement in update_statements:
            conn.execute(statement)
        
        logger.info(f"Completed null normalisation for {len(update_statements)} columns")
        
    except Exception as e:
        logger.error(f"Error in null normalisation: {e}")
        raise

# ========================================
# DATA PROCESSING FUNCTIONS
# ========================================

def process_single_scopus_file(file_path: Path) -> Tuple[List[Dict], Optional[Dict]]:
    """Process a single Scopus JSON file and return entries and metadata"""
    data = load_json_file(file_path)
    if not data:
        return [], None
    
    try:
        metadata = data.get('metadata', {})
        load_id = metadata.get('load_id')
        data_source = metadata.get('data_source')
        
        if not load_id:
            logger.warning(f"No load_id found in {file_path}")
            return [], None
        
        entries = data.get('raw_response', {}).get('search-results', {}).get('entry', [])
        
        processed_entries = []
        for entry in entries:
            entry_with_metadata = {
                'load_id': load_id,
                'data_source': data_source,
                **entry
            }
            
            transformed_entry = clean_and_transform_entry(entry_with_metadata)
            processed_entries.append(transformed_entry)
        
        logger.info(f"Processed {len(processed_entries)} entries from {file_path}")
        return processed_entries, metadata
        
    except Exception as e:
        logger.warning(f"Error processing {file_path}: {e}")
        return [], None

def process_single_manifest_file(file_path: Path, metadata_by_load_id: Dict[str, Dict]) -> Optional[Dict]:
    """Process a single manifest file and return audit data"""
    try:
        data = load_json_file(file_path)
        if not data:
            logger.warning(f"No data loaded from manifest {file_path}")
            return None
        
        load_id = data.get('load_id')
        if not load_id:
            logger.warning(f"No load_id found in manifest {file_path}")
            return None
        
        data_source = 'scopus_search_api'
        
        all_query_parameters = []
        if load_id in metadata_by_load_id:
            metadata = metadata_by_load_id[load_id]
            data_source = metadata.get('data_source', data_source)
            
            if 'all_query_parameters' in metadata:
                all_query_parameters = metadata['all_query_parameters']
            elif 'query_parameters' in metadata:
                all_query_parameters = [metadata['query_parameters']]
        
        processing_summary = data.get('processing_summary', {})
        authors_list = data.get('authors', [])
        
        all_file_metadata = []
        all_results = []
        
        for author in authors_list:
            if not isinstance(author, dict):
                logger.warning(f"Author entry is not a dictionary: {type(author)}")
                continue
            
            file_names = author.get('file_names', [])
            if isinstance(file_names, list) and file_names:
                for file_name in file_names:
                    file_size_kb = get_file_size_kb(file_path.parent, file_name)
                    file_metadata = {
                        'file_name': file_name,
                        'file_size_kb': file_size_kb
                    }
                    all_file_metadata.append(file_metadata)
            
            results = author.get('results', {})
            if isinstance(results, dict) and results:
                results_with_author = {
                    'author_id': author.get('author_id'),
                    **results
                }
                all_results.append(results_with_author)
        
        audit_data = {
            'load_id': load_id,
            'data_source': data_source,
            'ingest_timestamp': processing_summary.get('start_time'),
            'query_parameters': all_query_parameters,
            'audit_schema_version': 1,
            'ingestion_time': processing_summary.get('duration_seconds'),
            'payload_files': all_file_metadata,
            'payload_results': all_results,
            'audit_schema_hash_value': None
        }
        
        logger.info(f"Successfully extracted audit data from {file_path}")
        return audit_data
        
    except Exception as e:
        logger.error(f"Error processing manifest {file_path}: {e}")
        return None

def extract_scopus_data(json_files: List[Path]) -> Tuple[List[Dict], Dict[str, Dict]]:
    """Extract data from Scopus JSON files and return both entries and metadata"""
    all_entries = []
    metadata_by_load_id = {}
    
    for file_path in json_files:
        entries, metadata = process_single_scopus_file(file_path)
        all_entries.extend(entries)
        
        if metadata and metadata.get('load_id'):
            load_id = metadata['load_id']
            
            if load_id in metadata_by_load_id:
                existing_metadata = metadata_by_load_id[load_id]
                
                if 'all_query_parameters' not in existing_metadata:
                    existing_metadata['all_query_parameters'] = []
                    if 'query_parameters' in existing_metadata:
                        existing_metadata['all_query_parameters'].append(existing_metadata['query_parameters'])
                
                if 'query_parameters' in metadata:
                    existing_metadata['all_query_parameters'].append(metadata['query_parameters'])
            else:
                metadata_by_load_id[load_id] = metadata
    
    logger.info(f"Total processed entries: {len(all_entries)}")
    logger.info(f"Collected metadata for {len(metadata_by_load_id)} load_ids")
    return all_entries, metadata_by_load_id

def extract_manifest_data(manifest_files: List[Path], metadata_by_load_id: Dict[str, Dict]) -> List[Dict]:
    """Extract audit data from manifest files and combine with query parameters from metadata"""
    audit_entries = []
    for file_path in manifest_files:
        logger.info(f"Processing manifest file: {file_path}")
        entry = process_single_manifest_file(file_path, metadata_by_load_id)
        if entry:
            audit_entries.append(entry)
    
    logger.info(f"Successfully processed {len(audit_entries)} out of {len(manifest_files)} manifest files")
    return audit_entries

# ========================================
# PIPELINE ORCHESTRATION FUNCTIONS
# ========================================

def transform_author_names_pipeline(conn: duckdb.DuckDBPyConnection) -> None:
    """Orchestrate the author name transformation process"""
    logger.info("Starting author name transformation")
    
    try:
        records = fetch_author_records(conn)
        
        if not records:
            logger.info("No records with authors found for transformation")
            return
        
        logger.info(f"Processing {len(records)} records for author transformation")
        
        updates = process_author_records(records)
        
        if not updates:
            logger.warning("No successful author transformations completed")
            return
        
        apply_author_updates(conn, updates)
        
        logger.info("Author name transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in author transformation pipeline: {e}")
        raise

def normalise_null_values_pipeline(conn: duckdb.DuckDBPyConnection) -> None:
    """Orchestrate the null normalisation process"""
    logger.info("Starting null normalisation")
    
    try:
        execute_null_normalisation(conn, "silver_stg_scopus")
        logger.info("Null normalisation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in null normalisation pipeline: {e}")
        raise

# ========================================
# MAIN PIPELINE FUNCTION
# ========================================

def run_etlt_pipeline() -> None:
    """Main pipeline execution function"""
    logger.info("Starting Scopus EtLT Pipeline")
    
    try:
        config = ScopusEtLTConfig()
        
        conn = create_database_connection(config.db_path)
        
        execute_schema_ddl(conn, config.schema_path)
        
        # Extract phase
        logger.info("Starting extraction phase")
        json_files = find_json_files(config.source_path)
        manifest_files = find_manifest_files(config.source_path)
        
        scopus_data, metadata_by_load_id = extract_scopus_data(json_files)
        audit_data = extract_manifest_data(manifest_files, metadata_by_load_id)
        
        if not scopus_data and not audit_data:
            logger.warning("No data extracted, pipeline terminating")
            return
        
        processed_load_ids = get_unique_load_ids(scopus_data + audit_data)
        
        # Load phase - Truncate & reload for BOTH staging tables
        logger.info("Starting load phase - truncate & reload strategy for both staging tables")
        
        if scopus_data:
            logger.info("Truncating silver_stg_scopus for complete refresh")
            truncate_table(conn, "silver_stg_scopus")
            load_scopus_data_to_duckdb(conn, scopus_data)
        
        if audit_data:
            logger.info("Truncating silver_temp_audit for complete refresh")
            truncate_table(conn, "silver_temp_audit")
            load_audit_data_to_duckdb(conn, audit_data)
        
        # Transformations (only if we loaded scopus data)
        if scopus_data:
            logger.info("Starting transformation phase")
            
            logger.info("Adding scopus_author_name fields")
            transform_author_names_pipeline(conn)

            logger.info("Performing post-load null normalisation")
            normalise_null_values_pipeline(conn)

            logger.info("Finished transformation phase")
        
        # Verify load
        scopus_count = conn.execute("SELECT COUNT(*) FROM silver_stg_scopus").fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM silver_temp_audit").fetchone()[0]
        
        logger.info(f"Pipeline completed successfully")
        logger.info(f"Records loaded - Scopus: {scopus_count}, Audit: {audit_count}")
        logger.info(f"Load IDs processed: {processed_load_ids}")
        logger.info("Data is ready for external DQ orchestration")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    run_etlt_pipeline()