# Checksum Utility for Data Integrity Validation

A comprehensive utility for generating and comparing checksums of DuckDB tables to validate data integrity during ETL processes. The tool uses DuckDB's native hash function with automatic type casting to ensure reliable data validation across pipeline stages.

## Overview

The Checksum Utility provides a robust method for verifying that data remains unchanged as it moves through your data pipeline. It generates deterministic checksums by hashing table contents and comparing them between source and destination tables, making it ideal for validating ETL operations, data migrations, and ensuring data integrity in medallion architectures.

## Features

- **Deterministic Checksums**: Uses DuckDB's native hash function with consistent column ordering
- **Flexible Column Selection**: Generate checksums for all columns or specify a subset
- **Automatic Type Casting**: Handles different data types seamlessly with automatic string conversion
- **Critical Column Validation**: Checks for NULL values in key columns (e.g., primary keys)
- **Empty Table Detection**: Prevents silent failures by validating table state
- **Comparison Mode**: Compare checksums between source and destination tables
- **Configuration Files**: Support for YAML configuration for repeatable validation
- **Report Generation**: Automatically saves JSON reports with configurable filenames
- **Load ID Extraction**: Automatically extracts load identifiers from source tables
- **Command-Line Interface**: Flexible CLI for both manual and automated operations

## Use Cases

### ETL Validation
Verify that data transferred between staging and production tables remains unchanged:
```bash
python checksum_utility.py staging.db temp_audit \
    --compare production.db staging_audit
```

### Medallion Architecture
Validate data integrity between bronze, silver, and gold layers:
```bash
python checksum_utility.py silver.db silver_scopus \
    --compare gold.db gold_scopus \
    --columns "id,data,timestamp"
```

### Data Migration
Ensure complete and accurate data migration between systems:
```bash
python checksum_utility.py old_system.db users \
    --compare new_system.db users
```

### Audit Trail Validation
Verify audit table consistency after ETL operations:
```bash
python checksum_utility.py --config scopus_checksum.yml
```

## Installation

### Prerequisites

- Python 3.7 or higher
- DuckDB Python library
- PyYAML (for configuration file support)

### Dependencies

```bash
pip install duckdb pyyaml
```

### Project Structure

```
src/utils/checksum_tool/
├── checksum_utility.py        # Main utility script
├── scopus_checksum.yml        # Example configuration file
└── checksum_reports/          # Output directory for reports
```

## Usage

### Basic Checksum Generation

Generate a checksum for a single table:

```bash
python checksum_utility.py database.db table_name
```

**Example:**
```bash
python checksum_utility.py scopus_test.db silver_temp_audit
```

**Output:**
```json
{
  "database": "scopus_test.db",
  "table": "silver_temp_audit",
  "row_count": 1250,
  "column_count": 8,
  "columns": ["column1", "column2", "column3"],
  "checksum": "1234567890",
  "timestamp": "2025-01-15T14:30:00",
  "status": "success"
}
```

### Checksum with Specific Columns

Generate a checksum using only specified columns:

```bash
python checksum_utility.py database.db table_name \
    --columns "load_id,ingest_timestamp,payload_files"
```

**Example:**
```bash
python checksum_utility.py scopus_test.db silver_temp_audit \
    --columns "load_id,query_parameters,ingest_timestamp"
```

### Checksum Comparison

Compare checksums between two tables:

```bash
python checksum_utility.py source.db source_table \
    --compare dest.db dest_table
```

**Example:**
```bash
python checksum_utility.py scopus_test.db silver_temp_audit \
    --compare silver_audit.db silver_staging_audit
```

**Success Output:**
```json
{
  "comparison_status": "success",
  "checksums_match": true,
  "integrity_status": "PASSED",
  "source_result": {
    "database": "scopus_test.db",
    "table": "silver_temp_audit",
    "row_count": 1250,
    "checksum": "1234567890"
  },
  "destination_result": {
    "database": "silver_audit.db",
    "table": "silver_staging_audit",
    "row_count": 1250,
    "checksum": "1234567890"
  },
  "row_count_match": true,
  "checksum_match": true,
  "timestamp": "2025-01-15T14:30:00"
}
```

**Failure Output:**
```json
{
  "comparison_status": "success",
  "checksums_match": false,
  "integrity_status": "FAILED",
  "error_message": "Data integrity check failed - checksums do not match",
  "source_result": {
    "row_count": 1250,
    "checksum": "1234567890"
  },
  "destination_result": {
    "row_count": 1248,
    "checksum": "0987654321"
  },
  "row_count_match": false,
  "checksum_match": false
}
```

### Comparison with Specific Columns

Compare using only specific columns (useful for excluding audit fields):

```bash
python checksum_utility.py source.db source_table \
    --compare dest.db dest_table \
    --columns "load_id,data_field1,data_field2"
```

### Configuration File Mode

Use a YAML configuration file for repeatable validation:

```bash
python checksum_utility.py --config checksum_config.yml
```

**Example:**
```bash
python checksum_utility.py --config scopus_checksum.yml
```

### Saving Reports

Save checksum reports to a specific directory:

```bash
python checksum_utility.py database.db table_name \
    --output-dir ./validation_reports \
    --load-id LOAD_20250115_143000
```

The report filename will follow the pattern: `checksum_{load_id}_{timestamp}.json`

### Pretty Print Output

Display formatted JSON output:

```bash
python checksum_utility.py database.db table_name --pretty
```

## Configuration File Format

Configuration files use YAML format and support comprehensive validation settings.

### Basic Configuration Structure

```yaml
# Metadata about this configuration
config_metadata:
  name: "Data Integrity Check"
  description: "Validates data integrity during ETL transfer"
  version: "1.0"
  data_source: "YourDataSource"

# Database connection details
databases:
  source:
    path: "source.db"
    description: "Source database"
  
  destination:
    path: "destination.db"
    description: "Destination database"

# Table configuration
tables:
  source_table: "source_table_name"
  destination_table: "destination_table_name"

# Columns to include in checksum calculation
checksum_columns:
  - "column1"
  - "column2"
  - "column3"

# Critical columns that should never contain NULL values
critical_columns:
  - "primary_key_column"
  - "load_id"

# Validation settings
validation_settings:
  fail_on_empty_table: true
  fail_on_null_critical_columns: true
  fail_on_missing_columns: true
```

### Complete Example Configuration

```yaml
# Scopus Checksum Configuration
config_metadata:
  name: "Scopus Audit Data Integrity Check"
  description: "Validates data integrity for Scopus audit data during ETL transfer"
  version: "1.0"
  created_date: "2025-01-15"
  data_source: "Scopus"

databases:
  source:
    path: "scopus_test.db"
    description: "Temporary staging database"
  
  destination:
    path: "silver_metadata.db"
    description: "Permanent audit data store"

tables:
  source_table: "silver_temp_audit"
  destination_table: "silver_staging_audit"

checksum_columns:
  - "load_id"
  - "query_parameters"
  - "ingest_timestamp"
  - "ingestion_time"
  - "payload_files"

critical_columns:
  - "load_id"

validation_settings:
  fail_on_empty_table: true
  fail_on_null_critical_columns: true
  fail_on_missing_columns: true
```

## Command-Line Interface

### Full Command Syntax

```bash
python checksum_utility.py [OPTIONS] [DATABASE] [TABLE]
```

### Arguments

#### Positional Arguments (Manual Mode)
- `database`: Path to the database file
- `table`: Name of the table to checksum

#### Optional Arguments
- `--config PATH`: Path to YAML configuration file (alternative to manual mode)
- `--compare DEST_DB DEST_TABLE`: Compare with destination database and table
- `--columns LIST`: Comma-separated list of columns to include
- `--output-dir PATH`: Directory to save report files (default: ./checksum_reports)
- `--load-id ID`: Load identifier for filename generation
- `--pretty`: Pretty-print JSON output

### Exit Codes

- `0`: Success (checksums match or single checksum generated)
- `1`: Failure (checksums don't match, error occurred, or validation failed)

## How It Works

### Checksum Generation Algorithm

1. **Column Selection**: Identifies all columns (or specified subset) and sorts them alphabetically for consistency
2. **Type Casting**: Casts each column value to STRING for uniform handling
3. **Row Concatenation**: Concatenates all column values with pipe delimiter (`|`)
4. **Row Hashing**: Generates hash for each row using DuckDB's `hash()` function
5. **Aggregation**: Concatenates all row hashes (ordered by first column)
6. **Final Hash**: Generates final table checksum from aggregated row hashes

### SQL Query Example

For a table with columns `[col1, col2, col3]`:

```sql
SELECT hash(string_agg(row_hash, '')) as table_checksum
FROM (
    SELECT hash(CONCAT_WS('|', 
        CAST(col1 AS STRING),
        CAST(col2 AS STRING), 
        CAST(col3 AS STRING)
    )) as row_hash
    FROM table_name
    ORDER BY col1
)
```

### Validation Checks

The utility performs several validation checks before generating checksums:

1. **Table Existence**: Verifies the table exists in the database
2. **Empty Table Check**: Ensures table contains data (fails if empty)
3. **NULL Key Validation**: Checks critical columns for NULL values
4. **Column Validation**: Verifies specified columns exist in the table

## Error Handling

### Common Errors and Solutions

#### Table Not Found
**Error:** `Table 'table_name' does not exist`

**Solution:**
- Verify table name spelling and case sensitivity
- Check that you're connected to the correct database
- Ensure the table has been created

#### Empty Table
**Error:** `Critical failure: Table 'table_name' is empty. Pipeline requires investigation.`

**Solution:**
- This is an intentional failure to prevent silent issues
- Investigate why the table has no data
- Check upstream pipeline stages

#### NULL Values in Critical Columns
**Error:** `Critical failure: Found N NULL values in key column 'column_name'`

**Solution:**
- Investigate data quality issues
- Check upstream data validation
- Review ETL transformation logic
- Verify primary key constraints

#### Missing Columns
**Error:** `Specified columns not found in table 'table_name': ['col1', 'col2']`

**Solution:**
- Verify column names in configuration file
- Check for typos in column specification
- Ensure schema hasn't changed

#### Configuration File Errors
**Error:** `Missing required configuration section: section_name`

**Solution:**
- Review configuration file structure
- Ensure all required sections are present
- Validate YAML syntax

## Integration Patterns

### Pipeline Integration

```python
from checksum_utility import ChecksumUtility

def validate_etl_transfer(source_db, source_table, dest_db, dest_table, load_id):
    """Validate data integrity after ETL transfer"""
    checksum_util = ChecksumUtility()
    
    result = checksum_util.compare_checksums(
        source_db=source_db,
        source_table=source_table,
        dest_db=dest_db,
        dest_table=dest_table,
        output_dir="./validation_reports",
        load_id=load_id
    )
    
    if not result.get("checksums_match", False):
        raise Exception(
            f"Data integrity validation failed: {result.get('error_message', 'Unknown error')}"
        )
    
    return result

# Use in pipeline
try:
    result = validate_etl_transfer(
        "staging.db", "temp_audit",
        "production.db", "staging_audit",
        "LOAD_20250115_143000"
    )
    print(f"Validation passed: {result['integrity_status']}")
except Exception as e:
    print(f"Validation failed: {e}")
    raise
```

### Prefect Integration

```python
from prefect import task, flow
from checksum_utility import ChecksumUtility

@task(name="validate_data_integrity")
def validate_data_integrity(config_path: str, output_dir: str) -> dict:
    """Prefect task for data integrity validation"""
    checksum_util = ChecksumUtility()
    result = checksum_util.run_config_based_comparison(config_path, output_dir)
    
    if result.get("comparison_status") == "error":
        raise Exception(f"Checksum validation failed: {result.get('error_message')}")
    
    if not result.get("checksums_match", False):
        raise Exception("Data integrity check failed - checksums do not match")
    
    return result

@flow(name="etl_with_validation")
def etl_pipeline():
    # ... ETL steps ...
    
    # Validate data integrity
    validation_result = validate_data_integrity(
        config_path="config/checksum_config.yml",
        output_dir="./validation_reports"
    )
    
    print(f"Integrity status: {validation_result['integrity_status']}")
```

### Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from checksum_utility import ChecksumUtility
from datetime import datetime

def run_checksum_validation(**context):
    """Airflow task for checksum validation"""
    checksum_util = ChecksumUtility()
    
    result = checksum_util.run_config_based_comparison(
        config_path="/path/to/config.yml",
        output_dir="/path/to/reports"
    )
    
    if not result.get("checksums_match", False):
        raise ValueError("Data integrity validation failed")
    
    # Push result to XCom for downstream tasks
    context['task_instance'].xcom_push(
        key='validation_result',
        value=result
    )

with DAG('etl_with_validation', start_date=datetime(2025, 1, 1)) as dag:
    
    # ... ETL tasks ...
    
    validate_task = PythonOperator(
        task_id='validate_data_integrity',
        python_callable=run_checksum_validation,
        provide_context=True
    )
    
    # ... downstream tasks ...
```

## Report Structure

### Single Checksum Report

```json
{
  "database": "scopus_test.db",
  "table": "silver_temp_audit",
  "row_count": 1250,
  "column_count": 5,
  "columns": [
    "ingest_timestamp",
    "ingestion_time",
    "load_id",
    "payload_files",
    "query_parameters"
  ],
  "checksum": "1234567890123456",
  "timestamp": "2025-01-15T14:30:00.123456",
  "status": "success",
  "report_file": "./checksum_reports/checksum_LOAD123_20250115_143000.json"
}
```

### Comparison Report (Success)

```json
{
  "comparison_status": "success",
  "checksums_match": true,
  "integrity_status": "PASSED",
  "source_result": {
    "database": "scopus_test.db",
    "table": "silver_temp_audit",
    "row_count": 1250,
    "column_count": 5,
    "columns": ["load_id", "query_parameters", "ingest_timestamp"],
    "checksum": "1234567890123456",
    "timestamp": "2025-01-15T14:30:00.123456",
    "status": "success"
  },
  "destination_result": {
    "database": "silver_metadata.db",
    "table": "silver_staging_audit",
    "row_count": 1250,
    "column_count": 5,
    "columns": ["load_id", "query_parameters", "ingest_timestamp"],
    "checksum": "1234567890123456",
    "timestamp": "2025-01-15T14:30:00.234567",
    "status": "success"
  },
  "row_count_match": true,
  "checksum_match": true,
  "timestamp": "2025-01-15T14:30:00.345678",
  "config_metadata": {
    "name": "Scopus Audit Data Integrity Check",
    "version": "1.0"
  },
  "report_file": "./checksum_reports/checksum_LOAD123_20250115_143000.json"
}
```

### Comparison Report (Failure)

```json
{
  "comparison_status": "success",
  "checksums_match": false,
  "integrity_status": "FAILED",
  "error_message": "Data integrity check failed - checksums do not match",
  "source_result": {
    "database": "scopus_test.db",
    "table": "silver_temp_audit",
    "row_count": 1250,
    "checksum": "1234567890123456",
    "status": "success"
  },
  "destination_result": {
    "database": "silver_metadata.db",
    "table": "silver_staging_audit",
    "row_count": 1248,
    "checksum": "6543210987654321",
    "status": "success"
  },
  "row_count_match": false,
  "checksum_match": false,
  "timestamp": "2025-01-15T14:30:00.345678"
}
```

### Error Report

```json
{
  "database": "scopus_test.db",
  "table": "silver_temp_audit",
  "row_count": 0,
  "column_count": 0,
  "columns": [],
  "checksum": null,
  "timestamp": "2025-01-15T14:30:00.123456",
  "status": "error",
  "error_message": "Critical failure: Table 'silver_temp_audit' is empty. Pipeline requires investigation."
}
```

## Best Practices

### Column Selection
1. **Include Only Stable Columns**: Exclude columns that change between environments (e.g., `created_at`, `updated_at`)
2. **Exclude Audit Fields**: Don't include auto-generated audit columns in comparisons
3. **Use Alphabetical Ordering**: The utility automatically sorts columns, so order in configuration doesn't matter
4. **Document Column Choices**: Explain in configuration why specific columns are included/excluded

### Configuration Management
1. **Version Control**: Store configuration files in version control
2. **Environment-Specific Configs**: Create separate configs for dev/staging/production
3. **Document Metadata**: Use `config_metadata` section to document purpose and ownership
4. **Regular Reviews**: Review and update configurations as schemas evolve

### Pipeline Integration
1. **Validate After Each Transfer**: Run checksums after every stage transition
2. **Save All Reports**: Maintain audit trail of all validation runs
3. **Fail Fast**: Configure pipelines to halt on validation failure
4. **Monitor Failures**: Set up alerting for checksum mismatches

### Performance Considerations
1. **Large Tables**: Checksum generation scales linearly with table size
2. **Column Selection**: Fewer columns = faster checksum generation
3. **Read-Only Access**: Utility opens databases in read-only mode
4. **Concurrent Access**: Safe to run alongside other read operations

### Debugging
1. **Enable Logging**: Set logging level to DEBUG for troubleshooting
2. **Compare Row Counts First**: Mismatched row counts indicate data loss
3. **Check Sample Data**: If checksums differ, manually sample both tables
4. **Review Recent Changes**: Check for schema or data modifications

## Advanced Usage

### Programmatic API

```python
from checksum_utility import ChecksumUtility

# Initialise utility
checksum_util = ChecksumUtility()

# Generate checksum with all options
result = checksum_util.generate_checksum(
    database_path="data.db",
    table_name="my_table",
    columns=["col1", "col2", "col3"],
    critical_columns=["primary_key"],
    output_dir="./reports",
    load_id="LOAD123"
)

# Compare checksums programmatically
comparison = checksum_util.compare_checksums(
    source_db="source.db",
    source_table="source_table",
    dest_db="dest.db",
    dest_table="dest_table",
    columns=["col1", "col2"],
    critical_columns=["id"],
    output_dir="./reports",
    load_id="LOAD123"
)

# Load and run from configuration
result = checksum_util.run_config_based_comparison(
    config_path="config.yml",
    output_dir="./reports"
)
```

### Custom Critical Columns

Specify different critical columns for NULL validation:

```python
result = checksum_util.generate_checksum(
    database_path="data.db",
    table_name="users",
    critical_columns=["user_id", "email", "created_at"]
)
```

### Batch Validation

Validate multiple table pairs:

```python
table_pairs = [
    ("staging.db", "temp_table1", "prod.db", "table1"),
    ("staging.db", "temp_table2", "prod.db", "table2"),
    ("staging.db", "temp_table3", "prod.db", "table3")
]

results = []
for source_db, source_table, dest_db, dest_table in table_pairs:
    result = checksum_util.compare_checksums(
        source_db, source_table, dest_db, dest_table
    )
    results.append(result)
    
    if not result.get("checksums_match"):
        print(f"FAILED: {source_table} -> {dest_table}")
```

## Troubleshooting

### Checksum Mismatch Investigation

When checksums don't match, follow this investigation process:

1. **Check Row Counts**
   ```sql
   SELECT COUNT(*) FROM source_table;
   SELECT COUNT(*) FROM dest_table;
   ```

2. **Compare Sample Data**
   ```sql
   SELECT * FROM source_table ORDER BY primary_key LIMIT 10;
   SELECT * FROM dest_table ORDER BY primary_key LIMIT 10;
   ```

3. **Check for NULL Differences**
   ```sql
   SELECT COUNT(*) FROM source_table WHERE column_name IS NULL;
   SELECT COUNT(*) FROM dest_table WHERE column_name IS NULL;
   ```

4. **Verify Column Data Types**
   ```sql
   DESCRIBE source_table;
   DESCRIBE dest_table;
   ```

5. **Check for Whitespace Issues**
   ```sql
   SELECT column_name, LENGTH(column_name) 
   FROM source_table 
   WHERE column_name != TRIM(column_name);
   ```

### Performance Optimisation

For very large tables:

1. **Use Column Subset**: Only include columns that must be validated
2. **Add Table Indices**: Ensure first column (used for ordering) is indexed
3. **Run During Off-Peak**: Schedule validation during low-usage periods
4. **Consider Sampling**: For very large tables, consider sampling strategies

### Common Pitfalls

1. **Timestamp Precision**: Different timestamp precisions can cause mismatches
2. **Floating Point Precision**: Small floating point differences may cause mismatches
3. **Character Encoding**: Ensure consistent encoding between databases
4. **NULL vs Empty String**: NULL and empty strings hash differently
5. **Case Sensitivity**: Column and table names may be case-sensitive

## API Reference

### ChecksumUtility Class

#### `__init__()`
Initialises the checksum utility.

#### `generate_checksum(database_path, table_name, columns=None, critical_columns=None, output_dir=None, load_id=None)`
Generates a checksum for a specified table.

**Parameters:**
- `database_path` (str): Path to the database file
- `table_name` (str): Name of the table to checksum
- `columns` (list, optional): Specific columns to include
- `critical_columns` (list, optional): Columns to check for NULLs
- `output_dir` (str, optional): Directory for report files
- `load_id` (str, optional): Load identifier for filename

**Returns:** Dictionary with checksum results and metadata

#### `compare_checksums(source_db, source_table, dest_db, dest_table, columns=None, critical_columns=None, output_dir=None, load_id=None)`
Compares checksums between two tables.

**Parameters:**
- `source_db` (str): Path to source database
- `source_table` (str): Source table name
- `dest_db` (str): Path to destination database
- `dest_table` (str): Destination table name
- `columns` (list, optional): Columns to include in comparison
- `critical_columns` (list, optional): Columns to check for NULLs
- `output_dir` (str, optional): Directory for report files
- `load_id` (str, optional): Load identifier for filename

**Returns:** Dictionary with comparison results

#### `run_config_based_comparison(config_path, output_dir='./checksum_reports')`
Runs checksum comparison using YAML configuration file.

**Parameters:**
- `config_path` (str): Path to YAML configuration
- `output_dir` (str): Directory for report files

**Returns:** Dictionary with comparison results

#### `load_config(config_path)`
Loads configuration from YAML file.

**Parameters:**
- `config_path` (str): Path to configuration file

**Returns:** Dictionary with configuration data

## Examples

### Example 1: Basic ETL Validation

```bash
# After ETL transfer, validate data integrity
python checksum_utility.py staging.db temp_users \
    --compare production.db users \
    --output-dir ./validation_reports
```

### Example 2: Audit Table Validation

```bash
# Validate audit table transfer with specific columns
python checksum_utility.py temp.db silver_temp_audit \
    --compare metadata.db silver_staging_audit \
    --columns "load_id,query_parameters,ingest_timestamp,payload_files" \
    --output-dir ./audit_validation
```

### Example 3: Configuration-Based Validation

```bash
# Run validation using configuration file
python checksum_utility.py --config scopus_checksum.yml \
    --output-dir ./reports
```

### Example 4: Pipeline Integration

```python
#!/usr/bin/env python3
"""ETL pipeline with checksum validation"""

from checksum_utility import ChecksumUtility
import sys

def main():
    # ETL process
    # ... transfer data from staging to production ...
    
    # Validate data integrity
    checksum_util = ChecksumUtility()
    result = checksum_util.compare_checksums(
        source_db="staging.db",
        source_table="temp_data",
        dest_db="production.db",
        dest_table="data",
        output_dir="./validation_reports",
        load_id="LOAD_20250115"
    )
    
    # Check result
    if result.get("integrity_status") == "PASSED":
        print("Data integrity validated successfully")
        return 0
    else:
        print(f"Data integrity validation FAILED: {result.get('error_message')}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```