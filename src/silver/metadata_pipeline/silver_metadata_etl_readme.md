# Silver Audit Metadata ETL Pipeline

A simple and robust ETL pipeline for transferring audit metadata from temporary staging tables to the permanent silver metadata layer. The pipeline implements atomic operations with idempotent behaviour, ensuring data integrity and enabling safe re-runs without duplication.

## Overview

This pipeline extracts audit data from temporary staging databases (bronze layer temp dumps), transforms it by adding metadata tracking fields, and loads it into a permanent metadata store using a MERGE pattern. The design ensures that duplicate data is automatically handled, making the pipeline safe for repeated execution and enabling incremental data loading strategies.

## Features

- **Atomic Operations**: Uses staging table pattern for transaction safety
- **Idempotent Execution**: Safe to run multiple times without creating duplicates
- **MERGE Pattern**: Automatically handles duplicate load_ids
- **Audit Trail**: Adds processing timestamp and reprocessing tracking fields
- **Comprehensive Logging**: Dual logging to console and file
- **Error Recovery**: Automatic cleanup of staging tables on failure
- **Read-Only Source**: Connects to source database in read-only mode
- **Progress Reporting**: Detailed statistics on inserted, skipped, and total rows
- **Schema Validation**: Creates destination table if it doesn't exist
- **Command-Line Interface**: Simple CLI for automated execution

## Use Cases

### Medallion Architecture Data Flow
Transfer audit metadata from bronze staging to silver metadata layer:
```bash
python silver_metadata_etl.py scopus_test.db
```

### Incremental Loading
Run after each EtLT pipeline execution to accumulate audit records:
```bash
# After EtLT pipeline completes
python scopus_json_EtLT_to_staging_table.py
python silver_metadata_etl.py scopus_test.db
```

### Batch Processing
Consolidate multiple staging batches into permanent storage:
```bash
# Process multiple source databases
python silver_metadata_etl.py batch1_staging.db
python silver_metadata_etl.py batch2_staging.db
python silver_metadata_etl.py batch3_staging.db
```

### Pipeline Recovery
Re-run safely after failures without creating duplicates:
```bash
# Pipeline failed mid-execution - safe to re-run
python silver_metadata_etl.py scopus_test.db
# Duplicate rows automatically skipped
```

## Installation

### Prerequisites

- Python 3.7 or higher
- DuckDB Python library

### Dependencies

```bash
pip install duckdb
```

### Project Structure

```
src/silver/
├── silver_metadata_etl.py           # Main ETL pipeline script
├── silver_audit_etl.log             # Execution log file
└── silver_metadata.db               # Destination database (created automatically)
```

## Configuration

### Pipeline Configuration

The pipeline is configured through the `SilverAuditETL` class constructor:

```python
class SilverAuditETL:
    def __init__(self, source_db_path: str, destination_db_path: str = "silver_metadata.db"):
        self.source_db_path = source_db_path
        self.destination_db_path = destination_db_path
        
        # Table names
        self.source_table = "silver_temp_audit"
        self.destination_table = "silver_staging_audit"
```

**Configuration Parameters:**
- `source_db_path`: Path to source database (required)
- `destination_db_path`: Path to destination database (default: "silver_metadata.db")
- `source_table`: Name of source table (default: "silver_temp_audit")
- `destination_table`: Name of destination table (default: "silver_staging_audit")

### Customising Configuration

```python
from silver_metadata_etl import SilverAuditETL

# Custom destination database
etl = SilverAuditETL(
    source_db_path="scopus_test.db",
    destination_db_path="custom_metadata.db"
)
etl.run_etl()
```

## Database Schema

### Source Table (silver_temp_audit)

Temporary staging table in bronze layer:

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

### Destination Table (silver_staging_audit)

Permanent metadata table in silver layer:

```sql
CREATE TABLE silver_staging_audit (
    load_id STRING PRIMARY KEY,
    data_source STRING,
    ingest_timestamp TIMESTAMP,
    query_parameters JSON,
    audit_schema_version INTEGER,
    audit_schema_checksum STRING,
    checksum_status STRING,
    checksum_timestamp TIMESTAMP,
    checksum_report JSON,
    ingestion_time DOUBLE,
    validation_status STRING,
    validation_errors JSON,
    validation_fails JSON,
    validation_summary STRING,
    dq_report JSON,
    payload_files JSON,
    payload_results JSON,
    -- Additional ETL metadata fields
    is_reprocessed BOOLEAN DEFAULT FALSE,
    original_processing_timestamp TIMESTAMP,
    reprocessing_reason VARCHAR
)
```

### Additional Metadata Fields

The ETL pipeline adds three tracking fields:

| Field | Type | Description |
|-------|------|-------------|
| is_reprocessed | BOOLEAN | Flag indicating if this load_id was reprocessed |
| original_processing_timestamp | TIMESTAMP | When the record was first processed by ETL |
| reprocessing_reason | VARCHAR | Reason for reprocessing (null for initial loads) |

## Usage

### Command-Line Execution

Basic usage with default destination:

```bash
python silver_metadata_etl.py scopus_test.db
```

This will:
- Read from `scopus_test.db` → `silver_temp_audit` table
- Write to `silver_metadata.db` → `silver_staging_audit` table
- Create destination database if it doesn't exist
- Skip duplicate load_ids automatically

### Programmatic Usage

```python
from silver_metadata_etl import SilverAuditETL

# Basic usage
etl = SilverAuditETL(source_db_path="scopus_test.db")
success = etl.run_etl()

if success:
    print("ETL completed successfully")
else:
    print("ETL failed")
```

### Custom Destination Database

```python
from silver_metadata_etl import SilverAuditETL

# Specify custom destination
etl = SilverAuditETL(
    source_db_path="scopus_test.db",
    destination_db_path="production_metadata.db"
)
success = etl.run_etl()
```

### Batch Processing

```python
from silver_metadata_etl import SilverAuditETL
from pathlib import Path

# Process multiple source databases
source_databases = [
    "scopus_test_batch1.db",
    "scopus_test_batch2.db",
    "scopus_test_batch3.db"
]

for source_db in source_databases:
    if Path(source_db).exists():
        etl = SilverAuditETL(source_db_path=source_db)
        success = etl.run_etl()
        
        if not success:
            print(f"Failed to process {source_db}")
            break
```

## ETL Pipeline Workflow

### Complete Pipeline Flow

```
1. INITIALISATION
   ├── Validate source database exists
   ├── Setup logging (console + file)
   ├── Configure source and destination paths
   └── Set table names

2. EXTRACT PHASE
   ├── Open read-only connection to source database
   ├── Execute SELECT * FROM silver_temp_audit
   ├── Fetch all rows
   ├── Count rows extracted
   └── Log extraction results

3. TRANSFORM PHASE
   ├── Iterate through source rows
   ├── Convert row to list
   ├── Add is_reprocessed = FALSE
   ├── Add original_processing_timestamp = NOW()
   ├── Add reprocessing_reason = NULL
   └── Log transformation results

4. LOAD PHASE
   ├── Open read-write connection to destination
   ├── Create destination table if not exists
   ├── Create temporary staging table
   ├── Load transformed data into staging table
   ├── Execute MERGE operation
   │   ├── Check for existing load_ids
   │   ├── Insert only new load_ids
   │   └── Skip duplicates
   ├── Calculate rows inserted vs skipped
   ├── Drop staging table
   └── Log load results

5. VERIFICATION & REPORTING
   ├── Count total rows in destination
   ├── Calculate statistics
   │   ├── Source rows extracted
   │   ├── New rows inserted
   │   ├── Duplicate rows skipped
   │   └── Total rows in destination
   ├── Log comprehensive summary
   └── Close all connections
```

### Detailed Process Flow

#### Extract Phase

1. **Connection Establishment**
   ```python
   conn = duckdb.connect(source_db_path, read_only=True)
   ```
   - Opens source database in read-only mode
   - Prevents accidental modifications to source
   - Allows concurrent reads

2. **Data Extraction**
   ```python
   result = conn.execute("SELECT * FROM silver_temp_audit").fetchall()
   ```
   - Extracts all columns from source table
   - Fetches complete result set into memory
   - Returns list of tuples

3. **Row Counting**
   - Counts extracted rows for reporting
   - Logs extraction statistics

#### Transform Phase

1. **Row Processing**
   - Iterates through each source row
   - Converts tuple to mutable list

2. **Metadata Addition**
   ```python
   current_timestamp = datetime.now()
   new_row = list(row) + [
       False,              # is_reprocessed
       current_timestamp,  # original_processing_timestamp
       None                # reprocessing_reason
   ]
   ```
   - Adds three new columns to each row
   - Uses current timestamp for tracking
   - Maintains chronological audit trail

3. **Validation**
   - Verifies transformation completed successfully
   - Logs transformed row count

#### Load Phase

1. **Destination Table Creation**
   ```sql
   CREATE TABLE IF NOT EXISTS silver_staging_audit (...)
   ```
   - Creates table if it doesn't exist
   - Uses complete schema definition
   - Sets primary key on load_id

2. **Staging Table Creation**
   ```python
   staging_table = f"silver_staging_audit_staging_{timestamp}"
   ```
   - Creates temporary staging table with unique name
   - Uses current timestamp to avoid conflicts
   - Mirrors destination table structure

3. **Data Loading to Staging**
   ```python
   placeholders = ','.join(['?' for _ in range(17)])
   insert_sql = f"INSERT INTO {staging_table} VALUES ({placeholders})"
   conn.executemany(insert_sql, data)
   ```
   - Bulk inserts transformed data
   - Uses parameterised queries for safety
   - Loads all rows into staging table

4. **MERGE Operation**
   ```sql
   INSERT INTO silver_staging_audit
   SELECT s.*
   FROM staging_table s
   WHERE NOT EXISTS (
       SELECT 1 
       FROM silver_staging_audit d 
       WHERE d.load_id = s.load_id
   )
   ```
   - Inserts only new load_ids
   - Automatically skips duplicates
   - Maintains data integrity via PRIMARY KEY

5. **Cleanup**
   ```python
   conn.execute(f"DROP TABLE {staging_table}")
   ```
   - Removes staging table after successful merge
   - Cleans up even on failure (finally block)
   - Prevents accumulation of temporary tables

## Output Examples

### Successful Execution

```
2025-01-15 14:30:00 - silver_audit_etl - INFO - Starting Silver Audit ETL pipeline
2025-01-15 14:30:00 - silver_audit_etl - INFO - Successfully connected to source database: scopus_test.db
2025-01-15 14:30:00 - silver_audit_etl - INFO - Successfully connected to destination database: silver_metadata.db
2025-01-15 14:30:00 - silver_audit_etl - INFO - Destination table 'silver_staging_audit' created/verified
2025-01-15 14:30:00 - silver_audit_etl - INFO - Successfully extracted 5 rows from source table
2025-01-15 14:30:00 - silver_audit_etl - INFO - Successfully transformed 5 rows
2025-01-15 14:30:00 - silver_audit_etl - INFO - Creating staging table: silver_staging_audit_staging_1736952600
2025-01-15 14:30:00 - silver_audit_etl - INFO - Loading 5 rows into staging table
2025-01-15 14:30:00 - silver_audit_etl - INFO - Executing MERGE operation
2025-01-15 14:30:00 - silver_audit_etl - INFO - Successfully inserted 5 new rows into silver_staging_audit
2025-01-15 14:30:00 - silver_audit_etl - INFO - Duplicate rows skipped: 0
2025-01-15 14:30:00 - silver_audit_etl - INFO - Total rows in destination table: 5
2025-01-15 14:30:00 - silver_audit_etl - INFO - ETL Results Summary:
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - Source rows extracted: 5
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - NEW rows inserted: 5
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - Duplicate rows skipped: 0
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - Total rows in destination: 5
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - Destination table columns: 20
2025-01-15 14:30:00 - silver_audit_etl - INFO -   - Status: All source data was new
2025-01-15 14:30:00 - silver_audit_etl - INFO - Silver Audit ETL pipeline completed successfully
2025-01-15 14:30:00 - silver_audit_etl - INFO - Database connections closed
```

### Idempotent Re-run (Duplicates Detected)

```
2025-01-15 14:35:00 - silver_audit_etl - INFO - Starting Silver Audit ETL pipeline
2025-01-15 14:35:00 - silver_audit_etl - INFO - Successfully connected to source database: scopus_test.db
2025-01-15 14:35:00 - silver_audit_etl - INFO - Successfully connected to destination database: silver_metadata.db
2025-01-15 14:35:00 - silver_audit_etl - INFO - Destination table 'silver_staging_audit' created/verified
2025-01-15 14:35:00 - silver_audit_etl - INFO - Successfully extracted 5 rows from source table
2025-01-15 14:35:00 - silver_audit_etl - INFO - Successfully transformed 5 rows
2025-01-15 14:35:00 - silver_audit_etl - INFO - Creating staging table: silver_staging_audit_staging_1736952900
2025-01-15 14:35:00 - silver_audit_etl - INFO - Loading 5 rows into staging table
2025-01-15 14:35:00 - silver_audit_etl - INFO - Executing MERGE operation
2025-01-15 14:35:00 - silver_audit_etl - INFO - Successfully inserted 0 new rows into silver_staging_audit
2025-01-15 14:35:00 - silver_audit_etl - INFO - Duplicate rows skipped: 5
2025-01-15 14:35:00 - silver_audit_etl - INFO - Total rows in destination table: 5
2025-01-15 14:35:00 - silver_audit_etl - INFO - ETL Results Summary:
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - Source rows extracted: 5
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - NEW rows inserted: 0
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - Duplicate rows skipped: 5
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - Total rows in destination: 5
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - Destination table columns: 20
2025-01-15 14:35:00 - silver_audit_etl - INFO -   - Status: All data was already present (idempotent run)
2025-01-15 14:35:00 - silver_audit_etl - INFO - Silver Audit ETL pipeline completed successfully
2025-01-15 14:35:00 - silver_audit_etl - INFO - Database connections closed
```

### Partial Update (Mixed New and Duplicate)

```
2025-01-15 14:40:00 - silver_audit_etl - INFO - Starting Silver Audit ETL pipeline
2025-01-15 14:40:00 - silver_audit_etl - INFO - Successfully extracted 10 rows from source table
2025-01-15 14:40:00 - silver_audit_etl - INFO - Successfully transformed 10 rows
2025-01-15 14:40:00 - silver_audit_etl - INFO - Successfully inserted 3 new rows into silver_staging_audit
2025-01-15 14:40:00 - silver_audit_etl - INFO - Duplicate rows skipped: 7
2025-01-15 14:40:00 - silver_audit_etl - INFO - Total rows in destination table: 8
2025-01-15 14:40:00 - silver_audit_etl - INFO - ETL Results Summary:
2025-01-15 14:40:00 - silver_audit_etl - INFO -   - Source rows extracted: 10
2025-01-15 14:40:00 - silver_audit_etl - INFO -   - NEW rows inserted: 3
2025-01-15 14:40:00 - silver_audit_etl - INFO -   - Duplicate rows skipped: 7
2025-01-15 14:40:00 - silver_audit_etl - INFO -   - Total rows in destination: 8
2025-01-15 14:40:00 - silver_audit_etl - INFO -   - Status: Partial update - some data was already present
2025-01-15 14:40:00 - silver_audit_etl - INFO - Silver Audit ETL pipeline completed successfully
```

## Integration Patterns

### Pipeline Orchestration

```python
from scopus_json_EtLT_to_staging_table import run_etlt_pipeline
from silver_metadata_etl import SilverAuditETL

def complete_data_pipeline():
    """Complete data pipeline from JSON to silver layer"""
    
    # 1. Run EtLT pipeline to process JSON files
    print("Step 1: Processing JSON files...")
    run_etlt_pipeline()
    
    # 2. Transfer audit data to permanent storage
    print("Step 2: Transferring audit metadata...")
    etl = SilverAuditETL(source_db_path="scopus_test.db")
    success = etl.run_etl()
    
    if not success:
        raise Exception("Metadata ETL failed")
    
    print("Pipeline completed successfully")

# Execute complete pipeline
complete_data_pipeline()
```

### Prefect Integration

```python
from prefect import task, flow
from silver_metadata_etl import SilverAuditETL

@task(name="transfer_audit_metadata")
def transfer_audit_metadata(source_db: str) -> dict:
    """Prefect task for audit metadata ETL"""
    etl = SilverAuditETL(source_db_path=source_db)
    success = etl.run_etl()
    
    if not success:
        raise Exception("Audit metadata ETL failed")
    
    return {"status": "success", "source_db": source_db}

@flow(name="scopus_data_pipeline")
def scopus_pipeline():
    # Previous tasks...
    
    # Transfer audit metadata to permanent storage
    audit_result = transfer_audit_metadata(source_db="scopus_test.db")
    
    return audit_result
```

### Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from silver_metadata_etl import SilverAuditETL

def run_metadata_etl(**context):
    """Airflow task for metadata ETL"""
    source_db = context['params'].get('source_db', 'scopus_test.db')
    
    etl = SilverAuditETL(source_db_path=source_db)
    success = etl.run_etl()
    
    if not success:
        raise ValueError("Metadata ETL failed")
    
    # Push success metric to XCom
    context['task_instance'].xcom_push(
        key='etl_success',
        value=True
    )

with DAG(
    'scopus_metadata_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily'
) as dag:
    
    # Previous tasks...
    
    metadata_etl_task = PythonOperator(
        task_id='transfer_audit_metadata',
        python_callable=run_metadata_etl,
        params={'source_db': 'scopus_test.db'},
        provide_context=True
    )
    
    # Set dependencies...
```

### Sequential Pipeline Execution

```bash
#!/bin/bash
# complete_pipeline.sh

set -e  # Exit on error

echo "Starting complete data pipeline..."

# Step 1: Process JSON files to staging
echo "Step 1: Running EtLT pipeline..."
python scopus_json_EtLT_to_staging_table.py

# Step 2: Transfer audit metadata
echo "Step 2: Transferring audit metadata..."
python silver_metadata_etl.py scopus_test.db

# Step 3: Run checksum validation
echo "Step 3: Validating checksums..."
python checksum_utility.py --config scopus_checksum.yml

# Step 4: Run schema validation
echo "Step 4: Validating schema..."
python run_schema_registry.py validate scopus silver_stg_scopus

echo "Pipeline completed successfully!"
```

## Error Handling

### Common Errors and Solutions

#### Source Database Not Found
**Error:** `Source database file not found: scopus_test.db`

**Solution:**
- Verify the source database path is correct
- Ensure the EtLT pipeline has run successfully
- Check file permissions and existence:
  ```bash
  ls -l scopus_test.db
  ```

#### Source Table Missing
**Error:** `Failed to extract data from source table: Catalog Error: Table with name silver_temp_audit does not exist`

**Solution:**
- Ensure the EtLT pipeline has created the source table
- Verify table name matches configuration
- Check table exists:
  ```python
  import duckdb
  conn = duckdb.connect("scopus_test.db")
  print(conn.execute("SHOW TABLES").fetchall())
  ```

#### Primary Key Violation
**Error:** `Constraint Error: Duplicate key "LOAD_20250115_143000" violates primary key constraint`

**Solution:**
- This should not occur due to MERGE pattern
- If it does occur, check MERGE SQL logic
- Verify destination table has PRIMARY KEY constraint

#### Insufficient Permissions
**Error:** `IO Error: Cannot open file "silver_metadata.db": Permission denied`

**Solution:**
- Check write permissions on destination directory
- Verify disk space availability
- Ensure no other process has file locked

#### Memory Issues
**Error:** `Out of memory while fetching data`

**Solution:**
- For very large tables, consider batch processing
- Increase available memory
- Monitor memory usage during execution

### Error Recovery

The pipeline implements automatic cleanup:

```python
try:
    # Load data operations
    pass
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    
    # Automatic cleanup
    if staging_table:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            logger.info(f"Cleaned up staging table: {staging_table}")
        except Exception as cleanup_error:
            logger.warning(f"Failed to clean up: {cleanup_error}")
    
    raise
```

**Recovery Steps:**
1. Review error logs in `silver_audit_etl.log`
2. Verify source and destination databases are accessible
3. Check for and remove any orphaned staging tables
4. Re-run the pipeline (idempotent design ensures safety)

## Performance Considerations

### Optimisation Strategies

**Data Volume:**
- Pipeline loads entire source table into memory
- Suitable for tables up to several million rows
- For larger datasets, consider batch processing

**Database Performance:**
- Read-only connection to source prevents locks
- Staging table pattern enables atomic operations
- Primary key on load_id ensures fast duplicate detection

**Network Considerations:**
- Designed for local database files
- Not optimised for remote database connections
- Consider network latency if databases are remote

### Scaling Recommendations

For large datasets:

1. **Batch Processing**
   ```python
   # Process in batches
   batch_size = 10000
   offset = 0
   
   while True:
       query = f"SELECT * FROM silver_temp_audit LIMIT {batch_size} OFFSET {offset}"
       batch = conn.execute(query).fetchall()
       
       if not batch:
           break
       
       # Process batch...
       offset += batch_size
   ```

2. **Parallel Processing**
   - Split source data by date ranges
   - Process multiple date ranges in parallel
   - Merge results in destination (safe due to PRIMARY KEY)

3. **Database Tuning**
   ```python
   # Increase memory allocation
   conn.execute("SET memory_limit='8GB'")
   conn.execute("SET threads=4")
   ```

## Best Practices

### Pipeline Execution
1. **Always Run After EtLT**: Ensure staging table is populated
2. **Monitor Logs**: Review logs for warnings and errors
3. **Check Statistics**: Verify inserted vs skipped row counts
4. **Test Idempotency**: Run twice to verify duplicate handling
5. **Backup Destination**: Backup metadata database before major changes

### Data Integrity
1. **Validate Load IDs**: Ensure load_ids are unique and meaningful
2. **Check Timestamps**: Verify ingest_timestamp values are correct
3. **Review Skipped Rows**: Investigate why rows are skipped
4. **Monitor Growth**: Track destination table growth over time

### Operational Practices
1. **Schedule Regular Runs**: Integrate into daily pipeline schedule
2. **Archive Staging**: Consider archiving source staging tables after successful ETL
3. **Log Retention**: Maintain logs for audit and debugging purposes
4. **Alerting**: Set up alerts for ETL failures
5. **Documentation**: Document any custom configurations or modifications

### Maintenance
1. **Vacuum Database**: Periodically vacuum destination database:
   ```python
   conn.execute("VACUUM silver_staging_audit")
   ```
2. **Analyse Statistics**: Update table statistics for query optimisation:
   ```python
   conn.execute("ANALYZE silver_staging_audit")
   ```
3. **Monitor Disk Usage**: Track database file growth
4. **Clean Old Logs**: Rotate `silver_audit_etl.log` file regularly

## Troubleshooting

### Debugging Techniques

**Enable Debug Logging:**
```python
import logging

logger = logging.getLogger('silver_audit_etl')
logger.setLevel(logging.DEBUG)
```

**Inspect Staging Table:**
```python
import duckdb

conn = duckdb.connect("scopus_test.db", read_only=True)

# Check row count
count = conn.execute("SELECT COUNT(*) FROM silver_temp_audit").fetchone()[0]
print(f"Source rows: {count}")

# View sample data
samples = conn.execute("SELECT * FROM silver_temp_audit LIMIT 5").fetchall()
for row in samples:
    print(row)

# Check for duplicates
duplicates = conn.execute("""
    SELECT load_id, COUNT(*) as count
    FROM silver_temp_audit
    GROUP BY load_id
    HAVING COUNT(*) > 1
""").fetchall()
print(f"Duplicates: {duplicates}")
```

**Verify Destination:**
```python
conn = duckdb.connect("silver_metadata.db")

# Check total rows
total = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
print(f"Destination rows: {total}")

# Check recent additions
recent = conn.execute("""
    SELECT load_id, original_processing_timestamp
    FROM silver_staging_audit
    ORDER BY original_processing_timestamp DESC
    LIMIT 10
""").fetchall()
print("Recent loads:")
for load_id, timestamp in recent:
    print(f"  {load_id}: {timestamp}")
```

**Compare Source and Destination:**
```python
# Check which load_ids are in source but not destination
source_conn = duckdb.connect("scopus_test.db", read_only=True)
dest_conn = duckdb.connect("silver_metadata.db")

source_ids = set([row[0] for row in source_conn.execute(
    "SELECT load_id FROM silver_temp_audit"
).fetchall()])

dest_ids = set([row[0] for row in dest_conn.execute(
    "SELECT load_id FROM silver_staging_audit"
).fetchall()])

missing = source_ids - dest_ids
print(f"Load IDs in source but not destination: {missing}")
```

### Common Issues

**ETL Returns Success But No Rows Inserted:**
- All source data already exists in destination
- This is normal for idempotent re-runs
- Check logs for "All data was already present" message

**Staging Table Not Cleaned Up:**
- Check for staging tables:
  ```sql
  SHOW TABLES LIKE '%staging%'
  ```
- Manually drop if necessary:
  ```sql
  DROP TABLE IF EXISTS silver_staging_audit_staging_1234567890
  ```

**Slow Performance:**
- Check destination database size
- Vacuum and analyse database
- Verify adequate disk I/O performance
- Consider indexing on frequently queried fields

## API Reference

### SilverAuditETL Class

#### `__init__(source_db_path: str, destination_db_path: str = "silver_metadata.db")`
Initialises the ETL pipeline.

**Parameters:**
- `source_db_path` (str): Path to source database file
- `destination_db_path` (str): Path to destination database file (default: "silver_metadata.db")

**Example:**
```python
from silver_metadata_etl import SilverAuditETL

# Basic initialisation
etl = SilverAuditETL(source_db_path="scopus_test.db")

# Custom destination
etl = SilverAuditETL(
    source_db_path="scopus_test.db",
    destination_db_path="custom_metadata.db"
)
```

#### `run_etl() -> bool`
Executes the complete ETL pipeline.

**Returns:** 
- `True` if ETL completes successfully
- `False` if ETL fails

**Raises:** 
- `Exception` on critical errors

**Example:**
```python
etl = SilverAuditETL(source_db_path="scopus_test.db")
success = etl.run_etl()

if success:
    print("ETL completed successfully")
else:
    print("ETL failed")
    sys.exit(1)
```

### Internal Methods

#### `_setup_logging() -> logging.Logger`
Configures logging for the ETL process.

**Returns:** Configured logger instance

**Behaviour:**
- Creates both console and file handlers
- Console handler set to INFO level
- File handler set to DEBUG level
- Logs to `silver_audit_etl.log`

#### `_get_source_connection() -> duckdb.DuckDBPyConnection`
Creates read-only connection to source database.

**Returns:** DuckDB connection object

**Raises:** Exception if connection fails

**Behaviour:**
- Opens database in read-only mode
- Prevents accidental source modifications
- Logs connection success/failure

#### `_get_destination_connection() -> duckdb.DuckDBPyConnection`
Creates read-write connection to destination database.

**Returns:** DuckDB connection object

**Raises:** Exception if connection fails

**Behaviour:**
- Opens database in read-write mode
- Creates database file if it doesn't exist
- Logs connection success/failure

#### `_create_destination_table(conn: duckdb.DuckDBPyConnection) -> None`
Creates destination table with required schema.

**Parameters:**
- `conn` (DuckDBPyConnection): Database connection

**Raises:** Exception if table creation fails

**Behaviour:**
- Uses CREATE TABLE IF NOT EXISTS for idempotency
- Defines complete schema including metadata fields
- Sets PRIMARY KEY on load_id column

#### `_extract_data(conn: duckdb.DuckDBPyConnection) -> Tuple[list, int]`
Extracts data from source table.

**Parameters:**
- `conn` (DuckDBPyConnection): Database connection

**Returns:** Tuple of (data_rows, row_count)

**Raises:** Exception if extraction fails

**Behaviour:**
- Executes SELECT * FROM silver_temp_audit
- Fetches all rows into memory
- Logs row count extracted

#### `_transform_data(source_data: list) -> list`
Transforms source data by adding metadata columns.

**Parameters:**
- `source_data` (list): Raw data from source table

**Returns:** Transformed data with additional columns

**Raises:** Exception if transformation fails

**Behaviour:**
- Converts each row tuple to list
- Adds is_reprocessed = FALSE
- Adds original_processing_timestamp = NOW()
- Adds reprocessing_reason = NULL
- Logs transformation count

#### `_load_data(conn: duckdb.DuckDBPyConnection, data: list) -> int`
Loads transformed data using MERGE pattern.

**Parameters:**
- `conn` (DuckDBPyConnection): Database connection
- `data` (list): Transformed data to load (list with 17 columns)

**Returns:** Number of new rows inserted (not including duplicates)

**Raises:** Exception if load fails

**Behaviour:**
- Creates temporary staging table
- Loads data into staging table
- Executes MERGE operation (INSERT WHERE NOT EXISTS)
- Skips duplicate load_ids automatically
- Drops staging table after completion
- Cleans up staging table on error

**Algorithm:**
```
1. Create staging_TIMESTAMP table
2. Insert all data into staging table
3. Count rows before merge
4. INSERT from staging WHERE load_id NOT IN destination
5. Count rows after merge
6. Calculate rows_inserted = rows_after - rows_before
7. Drop staging table
8. Return rows_inserted
```

#### `_log_final_results(dest_conn: duckdb.DuckDBPyConnection, source_rows: int, loaded_rows: int) -> None`
Logs comprehensive ETL statistics.

**Parameters:**
- `dest_conn` (DuckDBPyConnection): Destination connection
- `source_rows` (int): Number of rows extracted from source
- `loaded_rows` (int): Number of new rows inserted

**Behaviour:**
- Queries total row count in destination
- Calculates duplicate count (source_rows - loaded_rows)
- Logs detailed summary with all statistics
- Determines and logs status message

**Status Messages:**
- "All data was new" - if loaded_rows == source_rows
- "All data was already present (idempotent run)" - if loaded_rows == 0
- "Partial update - some data was already present" - otherwise

## Advanced Usage

### Custom Logging Configuration

Override default logging behaviour:

```python
import logging
from silver_metadata_etl import SilverAuditETL

# Configure custom logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('custom_etl.log'),
        logging.StreamHandler()
    ]
)

# Run ETL with custom logging
etl = SilverAuditETL(source_db_path="scopus_test.db")
etl.run_etl()
```

### Monitoring and Metrics Collection

Extend the ETL class to collect detailed metrics:

```python
from silver_metadata_etl import SilverAuditETL
import time

class MonitoredETL(SilverAuditETL):
    """Extended ETL with monitoring and metrics"""
    
    def run_etl(self) -> dict:
        """Run ETL and return detailed metrics"""
        start_time = time.time()
        
        # Run parent ETL
        success = super().run_etl()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Collect metrics
        metrics = {
            'success': success,
            'duration_seconds': duration,
            'start_time': start_time,
            'end_time': end_time,
            'source_db': self.source_db_path,
            'destination_db': self.destination_db_path
        }
        
        # Log metrics
        self.logger.info(f"ETL Metrics: {metrics}")
        
        return metrics

# Use monitored version
etl = MonitoredETL(source_db_path="scopus_test.db")
metrics = etl.run_etl()

print(f"ETL Duration: {metrics['duration_seconds']:.2f} seconds")
print(f"Success: {metrics['success']}")
```

### Conditional Processing

Only run ETL when there's new data to process:

```python
from silver_metadata_etl import SilverAuditETL
import duckdb

def run_etl_if_new_data(source_db: str, destination_db: str = "silver_metadata.db"):
    """Only run ETL if there's new data"""
    
    # Check if source has new load_ids
    source_conn = duckdb.connect(source_db, read_only=True)
    dest_conn = duckdb.connect(destination_db, read_only=True)
    
    try:
        source_ids = set([row[0] for row in source_conn.execute(
            "SELECT load_id FROM silver_temp_audit"
        ).fetchall()])
        
        dest_ids = set([row[0] for row in dest_conn.execute(
            "SELECT load_id FROM silver_staging_audit"
        ).fetchall()])
        
        new_ids = source_ids - dest_ids
        
        if not new_ids:
            print("No new data to process")
            return False
        
        print(f"Found {len(new_ids)} new load_ids to process:")
        for load_id in sorted(new_ids):
            print(f"  - {load_id}")
        
        # Run ETL
        etl = SilverAuditETL(source_db, destination_db)
        return etl.run_etl()
        
    finally:
        source_conn.close()
        dest_conn.close()

# Use conditional processing
if __name__ == "__main__":
    run_etl_if_new_data("scopus_test.db")
```

### Batch Processing with Error Handling

Process multiple source databases with comprehensive error handling:

```python
from silver_metadata_etl import SilverAuditETL
from pathlib import Path
import logging

def process_multiple_sources(source_databases: list, continue_on_error: bool = True):
    """Process multiple source databases with error handling"""
    results = []
    
    for source_db in source_databases:
        result = {
            'source': source_db,
            'success': False,
            'error': None,
            'rows_inserted': 0,
            'rows_skipped': 0
        }
        
        try:
            # Check if file exists
            if not Path(source_db).exists():
                result['error'] = 'File not found'
                logging.warning(f"Source database not found: {source_db}")
                results.append(result)
                continue
            
            # Run ETL
            logging.info(f"Processing: {source_db}")
            etl = SilverAuditETL(source_db_path=source_db)
            success = etl.run_etl()
            
            result['success'] = success
            results.append(result)
            
        except Exception as e:
            result['error'] = str(e)
            logging.error(f"Failed to process {source_db}: {e}")
            results.append(result)
            
            if not continue_on_error:
                logging.error("Stopping batch processing due to error")
                break
    
    # Print comprehensive summary
    total = len(results)
    successful = sum(1 for r in results if r['success'])
    failed = total - successful
    
    print("\n" + "="*60)
    print("BATCH PROCESSING SUMMARY")
    print("="*60)
    print(f"Total databases: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print("\nFailed databases:")
        for result in results:
            if not result['success']:
                print(f"  - {result['source']}: {result['error']}")
    
    print("="*60)
    
    return results

# Example usage
if __name__ == "__main__":
    sources = [
        "batch1_scopus_test.db",
        "batch2_scopus_test.db",
        "batch3_scopus_test.db"
    ]
    
    results = process_multiple_sources(sources, continue_on_error=True)
    
    # Exit with error code if any failed
    all_successful = all(r['success'] for r in results)
    sys.exit(0 if all_successful else 1)
```

### Data Validation Post-ETL

Validate ETL results with comprehensive checks:

```python
from silver_metadata_etl import SilverAuditETL
import duckdb

def validate_etl_results(destination_db: str = "silver_metadata.db"):
    """Validate ETL results with comprehensive checks"""
    conn = duckdb.connect(destination_db, read_only=True)
    
    validations = {}
    issues = []
    
    try:
        # Check 1: No duplicate load_ids
        duplicates = conn.execute("""
            SELECT load_id, COUNT(*) as count
            FROM silver_staging_audit
            GROUP BY load_id
            HAVING COUNT(*) > 1
        """).fetchall()
        
        validations['no_duplicates'] = len(duplicates) == 0
        if duplicates:
            issues.append(f"Found {len(duplicates)} duplicate load_ids")
        
        # Check 2: All required fields populated
        null_checks = conn.execute("""
            SELECT 
                SUM(CASE WHEN load_id IS NULL THEN 1 ELSE 0 END) as null_load_ids,
                SUM(CASE WHEN data_source IS NULL THEN 1 ELSE 0 END) as null_data_sources,
                SUM(CASE WHEN ingest_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps,
                SUM(CASE WHEN original_processing_timestamp IS NULL THEN 1 ELSE 0 END) as null_processing_ts
            FROM silver_staging_audit
        """).fetchone()
        
        validations['all_fields_populated'] = all(count == 0 for count in null_checks)
        if not validations['all_fields_populated']:
            issues.append(f"Found NULL values in required fields: {null_checks}")
        
        # Check 3: Reasonable timestamp values
        timestamp_check = conn.execute("""
            SELECT COUNT(*) FROM silver_staging_audit
            WHERE original_processing_timestamp > CURRENT_TIMESTAMP
            OR original_processing_timestamp < '2020-01-01'
        """).fetchone()[0]
        
        validations['valid_timestamps'] = timestamp_check == 0
        if timestamp_check > 0:
            issues.append(f"Found {timestamp_check} rows with invalid timestamps")
        
        # Check 4: JSON fields are valid
        json_check = conn.execute("""
            SELECT COUNT(*) FROM silver_staging_audit
            WHERE TRY_CAST(query_parameters AS JSON) IS NULL
            AND query_parameters IS NOT NULL
        """).fetchone()[0]
        
        validations['valid_json'] = json_check == 0
        if json_check > 0:
            issues.append(f"Found {json_check} rows with invalid JSON")
        
        # Check 5: is_reprocessed flag consistency
        reprocessed_check = conn.execute("""
            SELECT COUNT(*) FROM silver_staging_audit
            WHERE is_reprocessed = TRUE AND reprocessing_reason IS NULL
        """).fetchone()[0]
        
        validations['reprocessed_flag_consistent'] = reprocessed_check == 0
        if reprocessed_check > 0:
            issues.append(f"Found {reprocessed_check} reprocessed rows without reason")
        
        # Check 6: Data volume checks
        total_rows = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
        validations['has_data'] = total_rows > 0
        if total_rows == 0:
            issues.append("Destination table is empty")
        
    finally:
        conn.close()
    
    # Print validation results
    print("\n" + "="*60)
    print("ETL VALIDATION RESULTS")
    print("="*60)
    
    all_passed = all(validations.values())
    
    for check, passed in validations.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {check}")
    
    if issues:
        print("\nISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
    
    print("="*60)
    print(f"Overall Status: {'PASSED' if all_passed else 'FAILED'}")
    print("="*60)
    
    return all_passed

# Use validation after ETL
if __name__ == "__main__":
    etl = SilverAuditETL(source_db_path="scopus_test.db")
    
    if etl.run_etl():
        if validate_etl_results():
            print("ETL and validation completed successfully")
            sys.exit(0)
        else:
            print("ETL completed but validation failed")
            sys.exit(1)
    else:
        print("ETL failed")
        sys.exit(1)
```

### Incremental Statistics Tracking

Track ETL statistics over time:

```python
from silver_metadata_etl import SilverAuditETL
from datetime import datetime
import json
from pathlib import Path

class StatisticsTracker:
    """Track ETL statistics over time"""
    
    def __init__(self, stats_file: str = "etl_statistics.json"):
        self.stats_file = Path(stats_file)
        self.stats = self._load_stats()
    
    def _load_stats(self):
        """Load existing statistics"""
        if self.stats_file.exists():
            with open(self.stats_file, 'r') as f:
                return json.load(f)
        return {'runs': []}
    
    def _save_stats(self):
        """Save statistics to file"""
        with open(self.stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)
    
    def record_run(self, success: bool, source_db: str, rows_inserted: int, 
                   rows_skipped: int, duration: float):
        """Record ETL run statistics"""
        run_stats = {
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'source_db': source_db,
            'rows_inserted': rows_inserted,
            'rows_skipped': rows_skipped,
            'duration_seconds': duration
        }
        
        self.stats['runs'].append(run_stats)
        self._save_stats()
    
    def get_summary(self):
        """Get summary statistics"""
        if not self.stats['runs']:
            return "No ETL runs recorded"
        
        total_runs = len(self.stats['runs'])
        successful_runs = sum(1 for r in self.stats['runs'] if r['success'])
        total_rows_inserted = sum(r['rows_inserted'] for r in self.stats['runs'])
        avg_duration = sum(r['duration_seconds'] for r in self.stats['runs']) / total_runs
        
        return {
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': total_runs - successful_runs,
            'total_rows_inserted': total_rows_inserted,
            'average_duration_seconds': avg_duration
        }

# Use statistics tracker
tracker = StatisticsTracker()

import time
start_time = time.time()

etl = SilverAuditETL(source_db_path="scopus_test.db")
success = etl.run_etl()

duration = time.time() - start_time

# Record run (you'd need to extract actual counts from logs or modify ETL to return them)
tracker.record_run(
    success=success,
    source_db="scopus_test.db",
    rows_inserted=5,  # This would come from ETL
    rows_skipped=0,   # This would come from ETL
    duration=duration
)

# Print summary
summary = tracker.get_summary()
print(json.dumps(summary, indent=2))
```

## Complete Examples

### Example 1: Basic ETL Execution

```bash
# Command-line execution
python silver_metadata_etl.py scopus_test.db
```

### Example 2: Complete Pipeline Integration

```python
#!/usr/bin/env python3
"""Complete data processing pipeline from JSON to validated silver layer"""

from scopus_json_EtLT_to_staging_table import run_etlt_pipeline
from silver_metadata_etl import SilverAuditETL
from checksum_utility import ChecksumUtility
import sys
import logging

def complete_pipeline():
    """Execute complete data pipeline with all validation steps"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("="*60)
    print("COMPLETE DATA PIPELINE")
    print("="*60)
    
    try:
        # Step 1: Process JSON files to staging
        print("\n[1/4] Processing JSON files to staging tables...")
        run_etlt_pipeline()
        print("✓ JSON processing completed")
        
        # Step 2: Transfer audit metadata to permanent storage
        print("\n[2/4] Transferring audit metadata to silver layer...")
        etl = SilverAuditETL(source_db_path="scopus_test.db")
        success = etl.run_etl()
        
        if not success:
            print("✗ Metadata ETL failed")
            return 1
        print("✓ Metadata transfer completed")
        
        # Step 3: Validate data integrity with checksums
        print("\n[3/4] Validating data integrity with checksums...")
        checksum_util = ChecksumUtility()
        result = checksum_util.run_config_based_comparison(
            config_path="scopus_checksum.yml",
            output_dir="./checksum_reports"
        )
        
        if not result.get("checksums_match"):
            print("✗ Checksum validation failed")
            return 1
        print("✓ Data integrity validated")
        
        # Step 4: Run post-ETL validation
        print("\n[4/4] Running post-ETL validation...")
        if not validate_etl_results():
            print("✗ Post-ETL validation failed")
            return 1
        print("✓ Post-ETL validation passed")
        
        print("\n" + "="*60)
        print("✓ COMPLETE PIPELINE FINISHED SUCCESSFULLY")
        print("="*60)
        return 0
        
    except Exception as e:
        logging.error(f"Pipeline failed with exception: {e}")
        print(f"\n✗ Pipeline failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(complete_pipeline())
```

### Example 3: Scheduled Daily ETL with Notifications

```python
#!/usr/bin/env python3
"""Scheduled daily ETL job with email notifications"""

from silver_metadata_etl import SilverAuditETL
from datetime import datetime
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class DailyETLJob:
    """Daily scheduled ETL with notifications"""
    
    def __init__(self, email_config: dict = None):
        self.email_config = email_config or {}
        self.setup_logging()
    
    def setup_logging(self):
        """Configure logging with daily log file"""
        log_file = f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, success: bool, message: str):
        """Send email notification"""
        if not self.email_config:
            return
        
        try:
            subject = f"ETL {'Success' if success else 'FAILURE'} - {datetime.now().strftime('%Y-%m-%d')}"
            
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from']
            msg['To'] = self.email_config['to']
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], 
                                 self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], 
                        self.email_config['password'])
            server.send_message(msg)
            server.quit()
            
            self.logger.info("Notification email sent")
            
        except Exception as e:
            self.logger.error(f"Failed to send notification: {e}")
    
    def run(self):
        """Execute daily ETL job"""
        self.logger.info("Starting daily ETL job")
        start_time = datetime.now()
        
        try:
            etl = SilverAuditETL(source_db_path="scopus_test.db")
            success = etl.run_etl()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if success:
                message = f"""
Daily ETL Job Completed Successfully

Start Time: {start_time}
End Time: {end_time}
Duration: {duration:.2f} seconds

Check logs for detailed information.
                """
                self.logger.info("Daily ETL completed successfully")
                self.send_notification(True, message)
                return 0
            else:
                message = f"""
Daily ETL Job FAILED

Start Time: {start_time}
End Time: {end_time}
Duration: {duration:.2f} seconds

Check logs for error details.
                """
                self.logger.error("Daily ETL failed")
                self.send_notification(False, message)
                return 1
                
        except Exception as e:
            message = f"""
Daily ETL Job CRASHED

Error: {str(e)}
Time: {datetime.now()}

Check logs immediately for error details.
            """
            self.logger.error(f"Daily ETL crashed: {e}")
            self.send_notification(False, message)
            return 1

if __name__ == "__main__":
    import sys
    
    # Configure email notifications (optional)
    email_config = {
        'from': 'etl@example.com',
        'to': 'admin@example.com',
        'smtp_server': 'smtp.example.com',
        'smtp_port': 587,
        'username': 'etl@example.com',
        'password': 'your-password'
    }
    
    job = DailyETLJob(email_config=email_config)
    sys.exit(job.run())
```

### Example 4: Multi-Environment ETL with Configuration Management

```python
#!/usr/bin/env python3
"""Multi-environment ETL with configuration management"""

from silver_metadata_etl import SilverAuditETL
import os
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict

@dataclass
class EnvironmentConfig:
    """Configuration for specific environment"""
    name: str
    source_db: str
    destination_db: str
    validation_rules: Dict
    notification_enabled: bool

class MultiEnvironmentETL:
    """Manage ETL across multiple environments"""
    
    ENVIRONMENTS = {
        'development': EnvironmentConfig(
            name='development',
            source_db='dev_scopus_test.db',
            destination_db='dev_silver_metadata.db',
            validation_rules={'strict': False},
            notification_enabled=False
        ),
        'staging': EnvironmentConfig(
            name='staging',
            source_db='stg_scopus_test.db',
            destination_db='stg_silver_metadata.db',
            validation_rules={'strict': True},
            notification_enabled=True
        ),
        'production': EnvironmentConfig(
            name='production',
            source_db='prod_scopus_test.db',
            destination_db='prod_silver_metadata.db',
            validation_rules={'strict': True},
            notification_enabled=True
        )
    }
    
    @classmethod
    def get_config(cls, environment: str) -> EnvironmentConfig:
        """Get configuration for environment"""
        if environment not in cls.ENVIRONMENTS:
            raise ValueError(
                f"Unknown environment: {environment}. "
                f"Valid options: {list(cls.ENVIRONMENTS.keys())}"
            )
        return cls.ENVIRONMENTS[environment]
    
    @classmethod
    def run_for_environment(cls, environment: str = None) -> bool:
        """Run ETL for specific environment"""
        
        # Default to environment variable or development
        if environment is None:
            environment = os.getenv('ETL_ENV', 'development')
        
        print(f"Running ETL for environment: {environment.upper()}")
        print("="*60)
        
        # Get configuration
        config = cls.get_config(environment)
        
        # Validate source database exists
        if not Path(config.source_db).exists():
            print(f"Error: Source database not found: {config.source_db}")
            return False
        
        # Run ETL
        etl = SilverAuditETL(
            source_db_path=config.source_db,
            destination_db_path=config.destination_db
        )
        
        success = etl.run_etl()
        
        # Environment-specific post-processing
        if success and config.validation_rules.get('strict'):
            print("\nRunning strict validation checks...")
            success = validate_etl_results(config.destination_db)
        
        if success:
            print(f"\n✓ ETL completed successfully for {environment}")
        else:
            print(f"\n✗ ETL failed for {environment}")
        
        return success

if __name__ == "__main__":
    import sys
    
    # Get environment from command line or environment variable
    environment = sys.argv[1] if len(sys.argv) > 1 else None
    
    success = MultiEnvironmentETL.run_for_environment(environment)
    sys.exit(0 if success else 1)
```

Usage examples:
```bash
# Run for development
python multi_env_etl.py development

# Run for production
python multi_env_etl.py production

# Use environment variable
export ETL_ENV=staging
python multi_env_etl.py
```

## Testing Strategies

### Unit Testing

```python
import unittest
from unittest.mock import Mock, patch, MagicMock
from silver_metadata_etl import SilverAuditETL

class TestSilverAuditETL(unittest.TestCase):
    """Unit tests for SilverAuditETL"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.etl = SilverAuditETL(
            source_db_path="test_source.db",
            destination_db_path="test_destination.db"
        )
    
    def test_initialisation(self):
        """Test ETL initialisation"""
        self.assertEqual(self.etl.source_db_path, "test_source.db")
        self.assertEqual(self.etl.destination_db_path, "test_destination.db")
        self.assertEqual(self.etl.source_table, "silver_temp_audit")
        self.assertEqual(self.etl.destination_table, "silver_staging_audit")
    
    @patch('silver_metadata_etl.duckdb.connect')
    def test_get_source_connection(self, mock_connect):
        """Test source connection creation"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        conn = self.etl._get_source_connection()
        
        mock_connect.assert_called_once_with("test_source.db", read_only=True)
        self.assertEqual(conn, mock_conn)
    
    def test_transform_data(self):
        """Test data transformation"""
        source_data = [
            ('LOAD123', 'scopus', '2025-01-15', '{}'),
            ('LOAD124', 'scopus', '2025-01-16', '{}')
        ]
        
        transformed = self.etl._transform_data(source_data)
        
        # Check that each row has 3 additional columns
        self.assertEqual(len(transformed), 2)
        self.assertEqual(len(transformed[0]), 7)  # 4 original + 3 new
        
        # Check new column values
        self.assertFalse(transformed[0][4])  # is_reprocessed
        self.assertIsNotNone(transformed[0][5])  # original_processing_timestamp
        self.assertIsNone(transformed[0][6])  # reprocessing_reason

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing

```python
import unittest
import tempfile
import duckdb
from pathlib import Path
from silver_metadata_etl import SilverAuditETL

class TestSilverAuditETLIntegration(unittest.TestCase):
    """Integration tests for SilverAuditETL"""
    
    def setUp(self):
        """Create temporary databases for testing"""
        self.temp_dir = tempfile.mkdtemp()
        self.source_db = Path(self.temp_dir) / "test_source.db"
        self.dest_db = Path(self.temp_dir) / "test_dest.db"
        
        # Create source database with test data
        self._create_source_database()
    
    def tearDown(self):
        """Clean up temporary files"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def _create_source_database(self):
        """Create source database with test data"""
        conn = duckdb.connect(str(self.source_db))
        
        # Create source table
        conn.execute("""
            CREATE TABLE silver_temp_audit (
                load_id STRING PRIMARY KEY,
                data_source STRING,
                ingest_timestamp TIMESTAMP,
                query_parameters JSON
            )
        """)
        
        # Insert test data
        conn.execute("""
            INSERT INTO silver_temp_audit VALUES
            ('LOAD001', 'scopus', '2025-01-15 10:00:00', '{"query": "test1"}'),
            ('LOAD002', 'scopus', '2025-01-15 11:00:00', '{"query": "test2"}'),
            ('LOAD003', 'scopus', '2025-01-15 12:00:00', '{"query": "test3"}')
        """)
        
        conn.close()
    
    def test_complete_etl_flow(self):
        """Test complete ETL flow"""
        # Run ETL
        etl = SilverAuditETL(
            source_db_path=str(self.source_db),
            destination_db_path=str(self.dest_db)
        )
        
        success = etl.run_etl()
        self.assertTrue(success)
        
        # Verify destination data
        conn = duckdb.connect(str(self.dest_db))
        
        count = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
        self.assertEqual(count, 3)
        
        # Verify metadata fields
        result = conn.execute("""
            SELECT is_reprocessed, original_processing_timestamp, reprocessing_reason
            FROM silver_staging_audit
            LIMIT 1
        """).fetchone()
        
        self.assertFalse(result[0])  # is_reprocessed
        self.assertIsNotNone(result[1])  # original_processing_timestamp
        self.assertIsNone(result[2])  # reprocessing_reason
        
        conn.close()
    
    def test_idempotent_execution(self):
        """Test that running ETL twice doesn't create duplicates"""
        etl = SilverAuditETL(
            source_db_path=str(self.source_db),
            destination_db_path=str(self.dest_db)
        )
        
        # First run
        success1 = etl.run_etl()
        self.assertTrue(success1)
        
        # Second run (should be idempotent)
        success2 = etl.run_etl()
        self.assertTrue(success2)
        
        # Verify no duplicates
        conn = duckdb.connect(str(self.dest_db))
        count = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
        self.assertEqual(count, 3)  # Still only 3 rows
        
        # Verify no duplicate load_ids
        duplicates = conn.execute("""
            SELECT load_id, COUNT(*) as count
            FROM silver_staging_audit
            GROUP BY load_id
            HAVING COUNT(*) > 1
        """).fetchall()
        
        self.assertEqual(len(duplicates), 0)
        
        conn.close()

if __name__ == '__main__':
    unittest.main()
```

### Performance Testing

```python
import unittest
import time
import duckdb
import tempfile
from pathlib import Path
from silver_metadata_etl import SilverAuditETL

class TestSilverAuditETLPerformance(unittest.TestCase):
    """Performance tests for SilverAuditETL"""
    
    def setUp(self):
        """Create temporary databases"""
        self.temp_dir = tempfile.mkdtemp()
        self.source_db = Path(self.temp_dir) / "perf_source.db"
        self.dest_db = Path(self.temp_dir) / "perf_dest.db"
    
    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def _create_large_dataset(self, row_count: int):
        """Create source database with large dataset"""
        conn = duckdb.connect(str(self.source_db))
        
        conn.execute("""
            CREATE TABLE silver_temp_audit (
                load_id STRING PRIMARY KEY,
                data_source STRING,
                ingest_timestamp TIMESTAMP,
                query_parameters JSON
            )
        """)
        
        # Generate test data
        conn.execute(f"""
            INSERT INTO silver_temp_audit
            SELECT 
                'LOAD' || LPAD(CAST(i AS STRING), 10, '0') as load_id,
                'scopus' as data_source,
                CURRENT_TIMESTAMP as ingest_timestamp,
                '{{"query": "test"}}' as query_parameters
            FROM generate_series(1, {row_count}) as t(i)
        """)
        
        conn.close()
    
    def test_performance_10k_rows(self):
        """Test performance with 10,000 rows"""
        self._create_large_dataset(10000)
        
        etl = SilverAuditETL(
            source_db_path=str(self.source_db),
            destination_db_path=str(self.dest_db)
        )
        
        start_time = time.time()
        success = etl.run_etl()
        duration = time.time() - start_time
        
        self.assertTrue(success)
        print(f"\n10K rows processed in {duration:.2f} seconds")
        
        # Verify row count
        conn = duckdb.connect(str(self.dest_db))
        count = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
        self.assertEqual(count, 10000)
        conn.close()
        
        # Performance assertion (should complete in reasonable time)
        self.assertLess(duration, 60)  # Should complete within 60 seconds
    
    def test_performance_incremental_load(self):
        """Test performance of incremental loading"""
        # Initial load
        self._create_large_dataset(5000)
        
        etl = SilverAuditETL(
            source_db_path=str(self.source_db),
            destination_db_path=str(self.dest_db)
        )
        
        etl.run_etl()
        
        # Add more data
        conn = duckdb.connect(str(self.source_db))
        conn.execute("""
            INSERT INTO silver_temp_audit
            SELECT 
                'LOADNEW' || LPAD(CAST(i AS STRING), 10, '0') as load_id,
                'scopus' as data_source,
                CURRENT_TIMESTAMP as ingest_timestamp,
                '{"query": "test"}' as query_parameters
            FROM generate_series(1, 5000) as t(i)
        """)
        conn.close()
        
        # Incremental load
        start_time = time.time()
        success = etl.run_etl()
        duration = time.time() - start_time
        
        self.assertTrue(success)
        print(f"\nIncremental load of 5K rows in {duration:.2f} seconds")
        
        # Verify total count
        conn = duckdb.connect(str(self.dest_db))
        count = conn.execute("SELECT COUNT(*) FROM silver_staging_audit").fetchone()[0]
        self.assertEqual(count, 10000)
        conn.close()

if __name__ == '__main__':
    unittest.main()
```

## Monitoring and Observability

### Logging Best Practices

```python
import logging
from silver_metadata_etl import SilverAuditETL

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler()
    ]
)

# Add custom context
class ContextFilter(logging.Filter):
    """Add custom context to log records"""
    
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id
    
    def filter(self, record):
        record.run_id = self.run_id
        return True

# Use context in logging
import uuid
run_id = str(uuid.uuid4())

logger = logging.getLogger('silver_audit_etl')
logger.addFilter(ContextFilter(run_id))

logger.info(f"Starting ETL run: {run_id}")

etl = SilverAuditETL(source_db_path="scopus_test.db")
etl.run_etl()

logger.info(f"Completed ETL run: {run_id}")
```

### Metrics Collection

```python
from silver_metadata_etl import SilverAuditETL
import time
from dataclasses import dataclass
from typing import Optional

@dataclass
class ETLMetrics:
    """ETL performance metrics"""
    run_id: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    success: Optional[bool] = None
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    rows_skipped: int = 0
    error_message: Optional[str] = None

class MetricsCollector:
    """Collect and export ETL metrics"""
    
    def __init__(self):
        self.current_metrics = None
    
    def start_run(self, run_id: str):
        """Start metrics collection for a run"""
        self.current_metrics = ETLMetrics(
            run_id=run_id,
            start_time=time.time()
        )
    
    def end_run(self, success: bool, error: str = None):
        """End metrics collection"""
        if self.current_metrics:
            self.current_metrics.end_time = time.time()
            self.current_metrics.duration = (
                self.current_metrics.end_time - self.current_metrics.start_time
            )
            self.current_metrics.success = success
            self.current_metrics.error_message = error
    
    def export_metrics(self) -> dict:
        """Export metrics as dictionary"""
        if not self.current_metrics:
            return {}
        
        return {
            'run_id': self.current_metrics.run_id,
            'duration_seconds': self.current_metrics.duration,
            'success': self.current_metrics.success,
            'rows_extracted': self.current_metrics.rows_extracted,
            'rows_loaded': self.current_metrics.rows_loaded,
            'rows_skipped': self.current_metrics.rows_skipped,
            'error': self.current_metrics.error_message
        }

# Use metrics collector
import uuid

collector = MetricsCollector()
run_id = str(uuid.uuid4())

collector.start_run(run_id)

try:
    etl = SilverAuditETL(source_db_path="scopus_test.db")
    success = etl.run_etl()
    collector.end_run(success=success)
except Exception as e:
    collector.end_run(success=False, error=str(e))

# Export metrics
metrics = collector.export_metrics()
print(json.dumps(metrics, indent=2))
```

### Health Check Endpoint

```python
from flask import Flask, jsonify
from silver_metadata_etl import SilverAuditETL
import duckdb
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check if destination database is accessible
        conn = duckdb.connect("silver_metadata.db", read_only=True)
        
        # Get recent ETL activity
        recent_loads = conn.execute("""
            SELECT COUNT(*) FROM silver_staging_audit
            WHERE original_processing_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
        """).fetchone()[0]
        
        # Get total row count
        total_rows = conn.execute("""
            SELECT COUNT(*) FROM silver_staging_audit
        """).fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': {
                'accessible': True,
                'total_rows': total_rows,
                'recent_loads_24h': recent_loads
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 503

@app.route('/metrics')
def metrics():
    """Metrics endpoint"""
    try:
        conn = duckdb.connect("silver_metadata.db", read_only=True)
        
        # Collect various metrics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_loads,
                MIN(original_processing_timestamp) as oldest_load,
                MAX(original_processing_timestamp) as newest_load,
                COUNT(DISTINCT data_source) as data_sources
            FROM silver_staging_audit
        """).fetchone()
        
        conn.close()
        
        return jsonify({
            'total_loads': stats[0],
            'oldest_load': str(stats[1]),
            'newest_load': str(stats[2]),
            'data_sources': stats[3]
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue: Staging Table Not Cleaned Up

**Symptoms:**
- Orphaned staging tables remain in database
- Error: "Table already exists"

**Diagnosis:**
```python
import duckdb

conn = duckdb.connect("silver_metadata.db")
tables = conn.execute("SHOW TABLES").fetchall()
staging_tables = [t for t in tables if 'staging' in t[0]]
print(f"Found staging tables: {staging_tables}")
```

**Solution:**
```python
# Manually clean up orphaned staging tables
for table in staging_tables:
    if '_staging_' in table[0]:
        conn.execute(f"DROP TABLE IF EXISTS {table[0]}")
        print(f"Dropped: {table[0]}")
```

#### Issue: Memory Errors with Large Datasets

**Symptoms:**
- Out of memory errors during extract phase
- System becomes unresponsive

**Solution:**
Implement batch processing:

```python
def batch_extract(conn, batch_size=10000):
    """Extract data in batches"""
    offset = 0
    all_data = []
    
    while True:
        batch = conn.execute(f"""
            SELECT * FROM silver_temp_audit
            LIMIT {batch_size} OFFSET {offset}
        """).fetchall()
        
        if not batch:
            break
        
        all_data.extend(batch)
        offset += batch_size
    
    return all_data
```

#### Issue: Connection Timeout

**Symptoms:**
- Database connection fails
- Timeout errors

**Solution:**
```python
import duckdb

# Increase connection timeout
conn = duckdb.connect("silver_metadata.db")
conn.execute("SET lock_timeout = '10min'")
conn.execute("SET statement_timeout = '30min'")
```

### Debug Mode

Enable detailed debugging:

```python
import logging
from silver_metadata_etl import SilverAuditETL

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s'
)

# Add SQL query logging
import duckdb

def log_sql_queries(conn):
    """Log all SQL queries"""
    original_execute = conn.execute
    
    def logged_execute(query, *args, **kwargs):
        logging.debug(f"Executing SQL: {query}")
        return original_execute(query, *args, **kwargs)
    
    conn.execute = logged_execute
    return conn

# Run ETL with debug mode
etl = SilverAuditETL(source_db_path="scopus_test.db")
etl.run_etl()
```

## Best Practices Summary

### Development
1. Test with small datasets first
2. Use version control for schema changes
3. Document any custom modifications
4. Implement comprehensive error handling
5. Add logging at key decision points

### Deployment
1. Use environment-specific configurations
2. Implement health check endpoints
3. Set up monitoring and alerting
4. Schedule regular backups
5. Document deployment procedures

### Operations
1. Monitor ETL execution times
2. Track data volumes over time
3. Review logs regularly
4. Maintain audit trail of all runs
5. Test disaster recovery procedures

### Performance
1. Vacuum databases regularly
2. Monitor disk usage
3. Optimise for your data volume
4. Consider indexing frequently queried columns
5. Profile slow operations

## Appendix

### Exit Codes

The ETL script uses standard exit codes:

- `0`: Success
- `1`: Failure (general error, validation failed, etc.)

### Environment Variables

Supported environment variables:

- `ETL_ENV`: Target environment (development, staging, production)
- `ETL_LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `ETL_SOURCE_DB`: Override source database path
- `ETL_DEST_DB`: Override destination database path

### File Locations

Default file locations:

- Log file: `silver_audit_etl.log` (in current directory)
- Source database: As specified in argument
- Destination database: `silver_metadata.db` (in current directory)

### Performance Benchmarks

Approximate performance benchmarks on standard hardware:

| Row Count | Extract | Transform | Load | Total |
|-----------|---------|-----------|------|-------|
| 1,000 | <1s | <1s | <1s | ~2s |
| 10,000 | ~2s | ~1s | ~2s | ~5s |
| 100,000 | ~15s | ~10s | ~20s | ~45s |
| 1,000,000 | ~2min | ~1.5min | ~3min | ~7min |

*Benchmarks may vary based on hardware, database size, and system load*