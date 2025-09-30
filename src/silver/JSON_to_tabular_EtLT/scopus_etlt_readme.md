# Scopus JSON to Tabular EtLT Pipeline

A comprehensive Extract, transform, Load, and Transform (EtLT) pipeline that processes Scopus API JSON responses into structured tabular data ready for integration into a cumulative table design (silver layer within medallion architecture). The pipeline handles complex nested JSON structures, performs data quality transformations, and prepares audit metadata for downstream validation.

## Overview

This pipeline extracts publication data from Scopus API JSON files and their associated manifest files, transforms the nested structures into flat tabular formats, loads them into DuckDB staging tables, and applies post-load transformations. The resulting tables are ready for data quality validation, checksum verification/integration, and integration into a historical record system (cumulative table design).

## Features

- **JSON Structure Flattening**: Converts deeply nested JSON structures to tabular format
- **Field Mapping**: Intelligent mapping of Scopus API fields to database columns
- **Context-Aware Transformations**: Applies different transformations based on field context
- **Array Handling**: Transforms string-delimited arrays to proper JSON arrays
- **Metadata Preservation**: Maintains complete audit trail with load identifiers
- **Author Name Construction**: Generates full author names from given name and surname
- **Null Normalisation**: Standardises various null representations to database NULL
- **Manifest Processing**: Extracts audit metadata from processing manifest files
- **File Size Calculation**: Automatically calculates and records payload file sizes
- **Truncate and Reload**: Implements full refresh strategy for staging tables
- **Dual Table Loading**: Processes both publication data and audit metadata
- **Comprehensive Logging**: Detailed logging throughout the pipeline execution

## Use Cases

### Research Publication Management
Process Scopus API responses to build a comprehensive publication database:
```python
# Run the pipeline to process all JSON files
python scopus_json_EtLT_to_staging_table.py
```

### Medallion Architecture Integration
Prepare data for silver layer integration in medallion architecture:
- **Bronze Layer**: Raw JSON files from Scopus API
- **Silver Staging**: Transformed tabular data (this pipeline)
- **Silver Production**: Cumulative historical records, Data Vault 2.0 scalable modelling
- **Gold Layer**: Analytics-ready aggregated data

### Research Analytics
Build analytics datasets for publication metrics, citation analysis, and author collaboration networks.

### Data Quality Workflows
Prepare standardised data for downstream quality validation and checksum verification.

## Installation

### Prerequisites

- Python 3.8 or higher
- DuckDB
- Pandas

### Dependencies

```bash
pip install duckdb pandas
```

### Project Structure

```
src/silver/JSON_to_tabular_EtLT/
├── scopus_json_EtLT_to_staging_table.py    # Main pipeline script
├── stg_schema.sql                           # DDL for staging tables
└── POC/                                     # Source JSON files directory
    ├── scopus_response_*.json               # Scopus API responses
    └── scopus_manifest_*.json               # Processing manifests
```

## Configuration

### Pipeline Configuration

The pipeline uses the `ScopusEtLTConfig` class for configuration:

```python
class ScopusEtLTConfig:
    def __init__(self):
        self.source_path = Path("/path/to/json/files")
        self.db_path = Path("scopus_test.db")
        self.schema_path = Path("stg_schema.sql")
```

**Configuration Parameters:**
- `source_path`: Directory containing JSON files and manifests
- `db_path`: Path to DuckDB database file
- `schema_path`: Path to SQL schema definition file

### Modifying Configuration

Edit the configuration directly in the script:

```python
config = ScopusEtLTConfig()
config.source_path = Path("/custom/path")
config.db_path = Path("custom_database.db")
```

## Database Schema

### Silver Staging Table (silver_stg_scopus)

Stores transformed publication data:

```sql
CREATE TABLE silver_stg_scopus (
    load_id STRING,
    data_source STRING,
    eid STRING PRIMARY KEY,
    title STRING,
    first_author STRING,
    journal_name STRING,
    issn STRING,
    eissn STRING,
    journal_volume INTEGER,
    journal_issue STRING,
    journal_page_range STRING,
    publication_date DATE,
    doi STRING,
    abstract STRING,
    citations INTEGER,
    affiliations JSON,
    pubmed_id BIGINT,
    publishing_format STRING,
    document_type STRING,
    author_count JSON,
    authors JSON,
    publication_keywords JSON,
    funder_source_id STRING,
    funder_acronym STRING,
    funder_number STRING,
    funder_name STRING,
    open_access_flag BOOLEAN,
    open_access_status JSON,
    -- Quality assurance fields
    scopus_schema_version INTEGER,
    scopus_schema_hash_value STRING,
    scopus_schema_report JSON,
    checksum_status STRING,
    checksum_timestamp TIMESTAMP,
    checksum_report JSON,
    checksum_value STRING
)
```

### Audit Table (silver_temp_audit)

Stores processing metadata and audit information:

```sql
CREATE TABLE silver_temp_audit (
    load_id STRING PRIMARY KEY,
    data_source STRING,
    ingest_timestamp TIMESTAMP,
    query_parameters JSON,
    audit_schema_version INTEGER,
    audit_schema_hash_value STRING,
    schema_validation_report JSON,
    schema_validation_status BOOLEAN,
    checksum_status STRING,
    checksum_timestamp TIMESTAMP,
    checksum_report JSON,
    checksum_value STRING,
    ingestion_time DOUBLE,
    validation_status STRING,
    validation_errors JSON,
    validation_fails JSON,
    validation_summary STRING,
    dq_report JSON,
    payload_files JSON,
    payload_results JSON
)
```

## Field Mappings

The pipeline performs intelligent field mapping from Scopus API JSON to database columns:

### Basic Field Mappings

| Scopus API Field | Database Column | Data Type |
|------------------|-----------------|-----------|
| dc:title | title | STRING |
| dc:creator | first_author | STRING |
| dc:description | abstract | STRING |
| prism:publicationName | journal_name | STRING |
| prism:issn | issn | STRING |
| prism:eIssn | eissn | STRING |
| prism:volume | journal_volume | INTEGER |
| prism:issueIdentifier | journal_issue | STRING |
| prism:pageRange | journal_page_range | STRING |
| prism:coverDate | publication_date | DATE |
| prism:doi | doi | STRING |
| citedby-count | citations | INTEGER |
| pubmed-id | pubmed_id | BIGINT |
| prism:aggregationType | publishing_format | STRING |
| subtypeDescription | document_type | STRING |
| openaccessFlag | open_access_flag | BOOLEAN |

### Complex Field Mappings

| Scopus API Field | Database Column | Transformation |
|------------------|-----------------|----------------|
| affiliation | affiliations | Array to JSON |
| author-count | author_count | Object to JSON with context-aware keys |
| author | authors | Array to JSON with author_name generation |
| authkeywords | publication_keywords | Pipe-delimited string to JSON array |
| freetoreadLabel | open_access_status | Nested structure to JSON array |

### Context-Sensitive Mappings

Within the `author-count` structure:
- `@limit` → `scopus_exported_authors_limit`
- `@total` → `scopus_total_authors_count`
- `$` → `scopus_exported_authors_count`

Within the `affiliation` structure:
- `affiliation-url` → `scopus_affiliation_url`
- `afid` → `scopus_affiliation_id`
- `affilname` → `scopus_affiliation_name`
- `affiliation-city` → `scopus_affiliation_city`
- `affiliation-country` → `scopus_affiliation_country`

Within the `author` structure:
- `@seq` → `author_list_position`
- `author-url` → `scopus_author_page_url`
- `authid` → `scopus_author_id`

## Transformation Logic

### Key Transformations

#### 1. @_fa Key Removal
Recursively removes Scopus-specific `@_fa` keys from all nested structures:

```python
def remove_fa_keys_recursive(obj):
    """Recursively remove @_fa keys from nested structures"""
    if isinstance(obj, dict):
        return {k: remove_fa_keys_recursive(v) 
                for k, v in obj.items() if k != '@_fa'}
    elif isinstance(obj, list):
        return [remove_fa_keys_recursive(item) for item in obj]
    else:
        return obj
```

#### 2. Author Name Generation
Constructs full author names from given name and surname:

```python
def transform_single_author_name(author: Dict) -> Dict:
    """Transform a single author by adding author_name field"""
    given_name = author.get('given-name', '') or ''
    surname = author.get('surname', '') or ''
    
    transformed_author = author.copy()
    transformed_author['author_name'] = f"{given_name} {surname}".strip()
    
    return transformed_author
```

**Example:**
```json
// Input
{"given-name": "John", "surname": "Smith"}

// Output
{"given-name": "John", "surname": "Smith", "author_name": "John Smith"}
```

#### 3. Keyword Array Transformation
Converts pipe-delimited keyword strings to JSON arrays:

```python
def transform_array_structure(obj, field_name):
    """Transform complex structures to simple arrays"""
    if field_name == 'authkeywords' and isinstance(obj, str):
        keywords = [keyword.strip() for keyword in obj.split('|')]
        keywords = [keyword for keyword in keywords if keyword]
        return keywords if keywords else 'null'
```

**Example:**
```json
// Input
"machine learning | artificial intelligence | neural networks"

// Output
["machine learning", "artificial intelligence", "neural networks"]
```

#### 4. Null Normalisation
Standardises various null representations:

```python
null_values = ['None', 'null', 'undefined', '', '[]', '{}']
```

All these values are converted to database NULL for consistency.

#### 5. Affiliation ID Array Transformation
Extracts values from complex affiliation ID structures:

```python
# Input structure
[{"$": "60071065"}, {"$": "60013520"}]

# Output structure
["60071065", "60013520"]
```

#### 6. Open Access Status Transformation
Flattens nested open access label structures:

```python
# Input structure
{"value": [{"$": "Gold"}, {"$": "Green"}]}

# Output structure
["Gold", "Green"]
```

## Usage

### Basic Execution

Run the complete pipeline:

```bash
python scopus_json_EtLT_to_staging_table.py
```

### Programmatic Usage

```python
from scopus_json_EtLT_to_staging_table import run_etlt_pipeline

# Run the complete pipeline
run_etlt_pipeline()
```

### Custom Configuration

```python
from scopus_json_EtLT_to_staging_table import (
    ScopusEtLTConfig,
    create_database_connection,
    execute_schema_ddl,
    find_json_files,
    extract_scopus_data,
    load_scopus_data_to_duckdb
)

# Custom configuration
config = ScopusEtLTConfig()
config.source_path = Path("/custom/path")
config.db_path = Path("custom.db")

# Create connection and execute pipeline steps
conn = create_database_connection(config.db_path)
execute_schema_ddl(conn, config.schema_path)

# Process files
json_files = find_json_files(config.source_path)
scopus_data, metadata = extract_scopus_data(json_files)
load_scopus_data_to_duckdb(conn, scopus_data)

conn.close()
```

## Pipeline Workflow

### Complete Pipeline Flow

```
1. INITIALISATION
   ├── Load configuration
   ├── Create database connection
   └── Execute schema DDL

2. EXTRACTION PHASE
   ├── Find JSON files (excluding manifests)
   ├── Find manifest files
   ├── Process JSON files
   │   ├── Load JSON data
   │   ├── Extract metadata (load_id, data_source)
   │   ├── Extract search results entries
   │   ├── Apply transformations
   │   └── Store with metadata
   └── Process manifest files
       ├── Load manifest data
       ├── Extract processing summary
       ├── Extract file metadata with sizes
       ├── Extract result details
       └── Combine with query parameters

3. LOAD PHASE
   ├── Truncate silver_stg_scopus
   ├── Load transformed publication data
   ├── Truncate silver_temp_audit
   └── Load audit metadata

4. TRANSFORMATION PHASE
   ├── Author name transformation
   │   ├── Fetch author records
   │   ├── Generate author_name fields
   │   └── Update database
   └── Null normalisation
       ├── Fetch table columns
       ├── Generate UPDATE statements
       └── Execute normalisation

5. VERIFICATION
   ├── Count loaded records
   ├── Log load IDs processed
   └── Report completion status
```

### Detailed Process Flow

#### Extraction Phase

**JSON File Processing:**
1. Locate all `*.json` files excluding manifests
2. For each file:
   - Load JSON content
   - Extract metadata (load_id, data_source)
   - Extract search results entries
   - Add metadata to each entry
   - Apply cleaning transformations
   - Apply field mapping transformations
   - Collect transformed entries

**Manifest File Processing:**
1. Locate all `*_manifest_*.json` files
2. For each manifest:
   - Load manifest content
   - Extract load_id
   - Match with metadata from JSON files
   - Extract processing summary (timing, status)
   - Extract file metadata (names, sizes)
   - Extract result details (per author)
   - Combine into audit record

#### Load Phase

**Staging Table Loading:**
1. Prepare data for loading (serialize JSON fields)
2. Truncate existing staging table
3. Create temporary DataFrame
4. Register as DuckDB temporary table
5. Execute INSERT statement with type casting
6. Verify row count

**Audit Table Loading:**
1. Prepare audit data (serialize JSON fields)
2. Truncate existing audit table
3. Create temporary DataFrame
4. Register as DuckDB temporary table
5. Execute INSERT statement
6. Verify with diagnostic queries

#### Transformation Phase

**Author Name Transformation:**
1. Fetch all records with non-null authors
2. Parse JSON authors field
3. For each author in each record:
   - Extract given-name and surname
   - Construct author_name field
   - Add to author object
4. Serialize updated authors back to JSON
5. Update database via temporary table join

**Null Normalisation:**
1. Fetch all column names from table
2. Exclude critical columns (load_id, data_source, eid)
3. For each column:
   - Generate UPDATE statement
   - Convert null-like values to NULL
4. Execute all UPDATE statements

## Input File Formats

### JSON Response File Format

```json
{
  "metadata": {
    "load_id": "LOAD_20250115_143000",
    "data_source": "scopus_search_api",
    "query_parameters": {
      "query": "AUTHID(123456)",
      "field": "dc:title,prism:doi,citedby-count",
      "date": "2020-2025"
    }
  },
  "raw_response": {
    "search-results": {
      "entry": [
        {
          "eid": "2-s2.0-85012345678",
          "dc:title": "Example Publication Title",
          "dc:creator": "Smith, John",
          "prism:publicationName": "Journal of Examples",
          "prism:doi": "10.1016/j.example.2024.01.001",
          "citedby-count": "42",
          "author": [
            {
              "authid": "123456",
              "given-name": "John",
              "surname": "Smith",
              "afid": [{"$": "60071065"}]
            }
          ],
          "affiliation": [
            {
              "afid": "60071065",
              "affilname": "University of Example",
              "affiliation-city": "London",
              "affiliation-country": "United Kingdom"
            }
          ],
          "authkeywords": "machine learning | data science | analytics"
        }
      ]
    }
  }
}
```

### Manifest File Format

```json
{
  "load_id": "LOAD_20250115_143000",
  "data_source": "scopus_search_api",
  "processing_summary": {
    "start_time": "2025-01-15T14:30:00.000000",
    "end_time": "2025-01-15T14:35:00.000000",
    "duration_seconds": 300.5,
    "total_files": 25,
    "total_results": 1250
  },
  "authors": [
    {
      "author_id": "123456",
      "file_names": [
        "scopus_response_123456_20250115.json"
      ],
      "results": {
        "entries_found": 50,
        "entries_returned": 50,
        "start_index": 0
      }
    }
  ]
}
```

## Output Data Examples

### Silver Staging Table (silver_stg_scopus)

```
| load_id              | eid              | title                | first_author | citations |
|---------------------|------------------|---------------------|--------------|-----------|
| LOAD_20250115_14300 | 2-s2.0-850123456 | Machine Learning... | Smith, John  | 42        |
| LOAD_20250115_14300 | 2-s2.0-850123457 | Data Analytics...   | Jones, Mary  | 31        |
```

**JSON Fields Example:**
```json
// authors field
[
  {
    "scopus_author_id": "123456",
    "given-name": "John",
    "surname": "Smith",
    "author_name": "John Smith",
    "scopus_affiliation_id": ["60071065"],
    "author_list_position": "1"
  }
]

// publication_keywords field
["machine learning", "data science", "analytics"]

// author_count field
{
  "scopus_exported_authors_limit": "100",
  "scopus_total_authors_count": "3",
  "scopus_exported_authors_count": "3"
}
```

### Audit Table (silver_temp_audit)

```
| load_id              | data_source         | ingest_timestamp       | ingestion_time |
|---------------------|---------------------|------------------------|----------------|
| LOAD_20250115_14300 | scopus_search_api   | 2025-01-15 14:30:00   | 300.5          |
```

**JSON Fields Example:**
```json
// payload_files field
[
  {
    "file_name": "scopus_response_123456_20250115.json",
    "file_size_kb": 245
  },
  {
    "file_name": "scopus_response_123457_20250115.json",
    "file_size_kb": 198
  }
]

// payload_results field
[
  {
    "author_id": "123456",
    "entries_found": 50,
    "entries_returned": 50,
    "start_index": 0
  }
]

// query_parameters field (array of all queries)
[
  {
    "query": "AUTHID(123456)",
    "field": "dc:title,prism:doi",
    "date": "2020-2025"
  }
]
```

## Integration with Validation Tools

### Schema Registry Integration

After running the EtLT pipeline, validate schema:

```bash
# Validate the staging table schema
python src/utils/schema_registry/run_schema_registry.py validate \
    scopus silver_stg_scopus \
    --load-id LOAD_20250115_143000 \
    --data-db scopus_test.db
```

### Checksum Validation Integration

After loading data, verify integrity:

```bash
# Compare staging to production checksums
python src/utils/checksum_tool/checksum_utility.py \
    scopus_test.db silver_stg_scopus \
    --compare production.db silver_scopus \
    --load-id LOAD_20250115_143000
```

### Data Quality Integration

After transformations, run quality checks:

```bash
# Run Soda DQ checks
python soda_dq.py --table silver_stg_scopus --load-id LOAD_20250115_143000
```

### Complete Validation Workflow

```python
from scopus_json_EtLT_to_staging_table import run_etlt_pipeline
from schema_registry import SchemaRegistry
from checksum_utility import ChecksumUtility

# 1. Run EtLT pipeline
run_etlt_pipeline()

# 2. Validate schema
registry = SchemaRegistry(
    db_path="schema_registry.db",
    data_db_path="scopus_test.db"
)
schema_validation = registry.validate_and_report(
    data_source="scopus",
    table_name="silver_stg_scopus",
    load_id="LOAD_20250115_143000"
)

if schema_validation['validation_results']['should_halt_pipeline']:
    raise Exception("Schema validation failed")

# 3. Verify checksums
checksum_util = ChecksumUtility()
checksum_result = checksum_util.compare_checksums(
    source_db="scopus_test.db",
    source_table="silver_temp_audit",
    dest_db="silver_metadata.db",
    dest_table="silver_staging_audit",
    load_id="LOAD_20250115_143000"
)

if not checksum_result.get("checksums_match"):
    raise Exception("Checksum validation failed")

# 4. Run data quality checks
# ... (DQ checks here)
```

## Error Handling

### Common Errors and Solutions

#### Malformed JSON File
**Error:** `Malformed JSON in file: Expecting property name`

**Solution:**
- Verify JSON syntax using online validator
- Check for trailing commas or missing brackets
- Review file encoding (should be UTF-8)
- File will be logged and skipped; pipeline continues

#### Missing Metadata
**Error:** `No load_id found in file.json`

**Solution:**
- Ensure all JSON files include metadata section
- Verify metadata structure matches expected format
- Check that load_id is not null or empty

#### Schema Execution Error
**Error:** `Failed to execute schema DDL`

**Solution:**
- Verify stg_schema.sql file exists
- Check SQL syntax in schema file
- Ensure database has write permissions
- Confirm DuckDB version compatibility

#### Author Transformation Failure
**Error:** `Error transforming authors for eid: Invalid JSON`

**Solution:**
- Check authors field in source data
- Verify JSON structure is valid array
- Review transformation logic for edge cases
- Transformation will be logged and skipped for that record

### Pipeline Failure Recovery

If pipeline fails mid-execution:

1. **Check Logs**: Review error messages for specific failure point
2. **Database State**: Staging tables use truncate-and-reload, so partial loads are not an issue
3. **Restart Pipeline**: Simply re-run the pipeline
4. **Verify Source Files**: Ensure all JSON files are intact and accessible

## Performance Considerations

### Optimization Strategies

**File Processing:**
- Pipeline processes files sequentially
- Large files (>100MB) may require additional memory
- Consider splitting very large result sets

**Database Operations:**
- Truncate-and-reload strategy ensures clean state
- Bulk loading via DataFrames is efficient
- Post-load transformations use SQL for performance

**Memory Management:**
- JSON files loaded one at a time
- DataFrames created per table, not per file
- Connection closed properly in finally block

### Scaling Recommendations

For large datasets:
1. **Batch Processing**: Process files in batches if memory limited
2. **Parallel Processing**: Consider parallel file processing (with coordination)
3. **Database Tuning**: Increase DuckDB memory settings
4. **Incremental Loading**: Modify to support incremental loads vs full refresh

## Best Practices

### Data Source Management
1. **Consistent Naming**: Use consistent load_id patterns
2. **File Organization**: Keep JSON and manifest files together
3. **Backup Source Files**: Maintain backups before processing
4. **Version Control**: Track schema changes in version control

### Pipeline Execution
1. **Pre-Flight Checks**: Verify source files exist before running
2. **Monitor Logs**: Review logs for warnings and errors
3. **Validate Outputs**: Always run post-load validation
4. **Document Load IDs**: Maintain registry of processed load_ids

### Schema Evolution
1. **Version Schemas**: Track schema versions in audit table
2. **Backward Compatibility**: Test changes with old data
3. **Document Mappings**: Maintain field mapping documentation
4. **Coordinate Changes**: Align schema changes with source systems

### Maintenance
1. **Regular Cleanup**: Archive old staging data periodically
2. **Log Rotation**: Implement log rotation for long-running systems
3. **Database Vacuum**: Periodically vacuum DuckDB for performance
4. **Monitor Disk Space**: Ensure adequate space for staging tables

## Troubleshooting

### Debugging Techniques

**Enable Debug Logging:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Inspect Intermediate Data:**
```python
# After extraction phase
print(f"Loaded {len(scopus_data)} records")
print(f"Sample record: {scopus_data[0] if scopus_data else 'None'}")

# Check metadata collection
print(f"Metadata for load_ids: {list(metadata_by_load_id.keys())}")
```

**Query Staging Tables:**
```sql
-- Check record counts by load_id
SELECT load_id, COUNT(*) 
FROM silver_stg_scopus 
GROUP BY load_id;

-- Inspect specific records
SELECT * FROM silver_stg_scopus WHERE eid = '2-s2.0-85012345678';

-- Check JSON field structure
SELECT authors FROM silver_stg_scopus LIMIT 1;

-- Verify null normalisation
SELECT COUNT(*) FROM silver_stg_scopus WHERE title = 'null';
```

**Verify File Processing:**
```python
from pathlib import Path
source_path = Path("/path/to/files")
json_files = list(source_path.glob("*.json"))
manifest_files = list(source_path.glob("*_manifest_*.json"))

print(f"JSON files: {len(json_files)}")
print(f"Manifest files: {len(manifest_files)}")
```

### Data Quality Issues

**Missing Authors:**
- Check if authors field exists in source JSON
- Verify transformation logic handles empty arrays
- Review null normalisation settings

**Incorrect Field Values:**
- Inspect field mapping dictionary
- Check for typos in source field names
- Verify data type casting in SQL

**Duplicate Records:**
- Verify EID uniqueness in source data
- Check for multiple files with same data
- Review truncate logic execution

## API Reference

### Core Functions

#### `run_etlt_pipeline()`
Main pipeline execution function.

**Returns:** None

**Raises:** Exception on pipeline failure

#### `extract_scopus_data(json_files: List[Path])`
Extract data from Scopus JSON files.

**Parameters:**
- `json_files` (List[Path]): List of JSON file paths to process

**Returns:** Tuple of (List[Dict], Dict[str, Dict])
- List of transformed entries
- Dictionary of metadata by load_id

#### `extract_manifest_data(manifest_files: List[Path], metadata_by_load_id: Dict[str, Dict])`
Extract audit data from manifest files.

**Parameters:**
- `manifest_files` (List[Path]): List of manifest file paths
- `metadata_by_load_id` (Dict): Metadata from JSON files

**Returns:** List[Dict] - Audit data entries

#### `load_scopus_data_to_duckdb(conn: DuckDBPyConnection, scopus_data: List[Dict])`
Load Scopus data into staging table.

**Parameters:**
- `conn` (DuckDBPyConnection): Database connection
- `scopus_data` (List[Dict]): Transformed publication data

**Returns:** None

#### `load_audit_data_to_duckdb(conn: DuckDBPyConnection, audit_data: List[Dict])`
Load audit data into temporary audit table.

**Parameters:**
- `conn` (DuckDBPyConnection): Database connection
- `audit_data` (List[Dict]): Audit metadata

**Returns:** None

### Transformation Functions

#### `clean_and_transform_entry(entry: Dict)`
Apply cleaning and transformation operations.

**Parameters:**
- `entry` (Dict): Raw JSON entry

**Returns:** Dict - Transformed entry

#### `transform_authors_json_string(authors_json: str)`
Transform authors JSON to add author_name.

**Parameters:**
- `authors_json` (str): JSON string of authors array

**Returns:** str - Transformed JSON string

#### `transform_array_structure(obj, field_name: str)`
Transform complex structures to simple arrays.

**Parameters:**
- `obj`: Object to transform
- `field_name` (str): Name of field being transformed

**Returns:** Transformed object

### Utility Functions

#### `find_json_files(source_path: Path)`
Find all JSON files excluding manifests.

**Parameters:**
- `source_path` (Path): Directory to search

**Returns:** List[Path] - List of JSON file paths

#### `find_manifest_files(source_path: Path)`
Find all manifest files.

**Parameters:**
- `source_path` (Path): Directory to search

**Returns:** List[Path] - List of manifest file paths

#### `get_file_size_kb(base_directory: Path, file_name: str)`
Get file size in KB, rounded up.

**Parameters:**
- `base_directory` (Path): Base directory
- `file_name` (str): Name of file

**Returns:** Optional[int] - File size in KB or None