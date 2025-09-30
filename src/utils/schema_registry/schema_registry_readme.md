# Schema Registry System

A comprehensive light weight schema detection, versioning, and compatibility checking system for data pipeline validation across the medallion architecture.

## Overview

The Schema Registry System automatically detects schema changes in your data pipeline, evaluates their compatibility impact, and provides recommendations for handling those changes. It implements semantic versioning, maintains a complete audit trail, and can be configured to halt pipelines when breaking changes are detected.

## Features

- **Automatic Schema Detection**: Extracts schema information from DuckDB tables including column types, constraints, and metadata
- **Change Detection**: Identifies additions, removals, modifications, and reordering of columns
- **Compatibility Assessment**: Evaluates changes against configurable rules to determine if they are safe, warning-level, or breaking
- **Semantic Versioning**: Automatically increments version numbers (major.minor.patch) based on change severity
- **Audit Trail**: Maintains complete history of schema versions and evolution
- **Configurable Actions**: Define custom notification methods and pipeline control based on compatibility levels
- **Standalone Operation**: Can run independently without orchestration frameworks

## Architecture

### Core Components

- **SchemaRegistry**: Main coordination class that orchestrates validation workflow
- **SchemaDetector**: Extracts schema information from database tables
- **SchemaComparator**: Detects differences between schema versions
- **CompatibilityRulesEngine**: Evaluates changes against compatibility rules
- **SchemaRegistryDB**: Manages persistence of schemas, changes, and rules
- **ConfigManager**: Handles configuration loading and management

### Database Schema

The system maintains three core tables:

1. **schema_versions**: Stores schema snapshots with versioning
2. **schema_evolution_log**: Records all detected changes
3. **schema_compatibility_rules**: Defines rules for change evaluation

## Installation

### Prerequisites

- Python 3.8 or higher
- DuckDB
- SQLite3

### Dependencies

```bash
pip install duckdb pyyaml
```

### Project Structure

```
src/utils/schema_registry/
├── __init__.py
├── core.py                    # Main registry implementation
├── detector.py                # Schema detection logic
├── comparator.py              # Schema comparison
├── rules_engine.py            # Compatibility rules evaluation
├── database.py                # Database operations
├── config.py                  # Configuration management
├── run_schema_registry.py     # Standalone runner
└── config/
    └── schema_registry_config.yml
```

## Configuration

### Basic Configuration File

Create `config/schema_registry_config.yml`:

```yaml
# Database paths
database_path: "src/utilities/schema_registry/schema_registry.db"
reports_directory: "src/utilities/schema_registry/schema_reports"

# Auto-versioning settings
enable_auto_versioning: true

# Version increment rules based on compatibility level
version_increment_rules:
  safe: "patch"      # 1.0.0 -> 1.0.1
  warning: "minor"   # 1.0.0 -> 1.1.0
  breaking: "major"  # 1.0.0 -> 2.0.0

# Notification configuration
notifications:
  safe:
    method: "log"
    level: "info"
    email: false
    halt_pipeline: false
  
  warning:
    method: "both"
    level: "warning"
    email: true
    halt_pipeline: false
  
  breaking:
    method: "both"
    level: "error"
    email: true
    halt_pipeline: true
```

### Configuration Options

#### Notification Methods
- `log`: Write to application logs only
- `email`: Send email notification (requires integration with Prefect)
- `both`: Log and send email

#### Notification Levels
- `info`: Informational message
- `warning`: Warning that requires attention
- `error`: Error that may halt pipeline

#### Version Increment Rules
- `patch`: Increment patch version (1.0.0 -> 1.0.1)
- `minor`: Increment minor version (1.0.0 -> 1.1.0)
- `major`: Increment major version (1.0.0 -> 2.0.0)

## Usage

### Programmatic Usage

#### Basic Validation

```python
from src.utils.schema_registry import SchemaRegistry

# Initialise registry
registry = SchemaRegistry(
    db_path="schema_registry.db",
    data_db_path="your_data.db",
    config_path="config/schema_registry_config.yml"
)

# Validate a table schema
validation_report, report_path = registry.validate_and_report(
    data_source="scopus",
    table_name="silver_stg_scopus",
    load_id="LOAD_20250101_123456"
)

# Check validation results
status = validation_report['validation_results']['overall_status']
should_halt = validation_report['validation_results']['should_halt_pipeline']

if should_halt:
    raise Exception(f"Breaking schema changes detected: {status}")
```

#### Register Initial Schema

```python
# First-time registration of a table
result = registry.register_initial_schema(
    data_source="scopus",
    table_name="silver_stg_scopus"
)
```

#### Extract Audit Data

```python
# Get audit data for downstream tables
validation_report, _ = registry.validate_and_report(
    data_source="scopus",
    table_name="silver_stg_scopus"
)

audit_data = registry.extract_audit_data(validation_report)
# audit_data contains: schema_version, schema_hash, validation_status, etc.
```

#### View Schema History

```python
# Get schema evolution history
history = registry.get_schema_history(
    data_source="scopus",
    table_name="silver_stg_scopus",
    limit=10
)

for version in history:
    print(f"Version {version['schema_version']}: {version['detected_at']}")
```

### Command-Line Usage

The schema registry includes a standalone runner for command-line operations.

#### Validate a Table

```bash
python src/utils/schema_registry/run_schema_registry.py validate \
    scopus silver_stg_scopus \
    --load-id LOAD_20250101_123456 \
    --data-db scopus_data.db
```

#### Register Initial Schema

```bash
python src/utils/schema_registry/run_schema_registry.py register \
    scopus silver_stg_scopus \
    --data-db scopus_data.db
```

#### View Schema History

```bash
python src/utils/schema_registry/run_schema_registry.py history \
    scopus silver_stg_scopus \
    --limit 10
```

#### View Recent Changes

```bash
python src/utils/schema_registry/run_schema_registry.py changes \
    --data-source scopus \
    --limit 20
```

#### Validate Configuration

```bash
python src/utils/schema_registry/run_schema_registry.py config
```

### Command-Line Options

```
Options:
  --load-id        Load ID for tracking
  --config         Path to config YAML file
  --data-db        Path to data database
  --schema-db      Path to schema registry database
  --no-report      Skip saving JSON report
  --no-summary     Skip printing summary
  --limit          Limit for history/changes queries
```

## Compatibility Rules

The system includes default compatibility rules that can be customised.

### Default Rules

| Rule ID | Change Type | From | To | Category | Compatibility | Action |
|---------|-------------|------|-----|----------|---------------|--------|
| R001 | add_column | any | nullable | any | safe | proceed |
| R002 | add_column | any | not_null | any | breaking | halt |
| R003 | remove_column | any | any | any | breaking | halt |
| R004 | change_datatype | any | any | string_to_numeric | breaking | halt |
| R005 | change_datatype | varchar_small | varchar_large | string | safe | proceed |
| R006 | change_datatype | varchar_large | varchar_small | string | warning | warn_proceed |
| R007 | change_datatype | integer | bigint | numeric | safe | proceed |
| R008 | change_nullable | not_null | nullable | any | safe | proceed |
| R009 | change_nullable | nullable | not_null | any | breaking | halt |
| R010 | reorder_columns | any | any | any | safe | proceed |

### Adding Custom Rules

```python
from src.utils.schema_registry import CompatibilityRulesEngine

# Add a custom rule
rule_data = {
    'rule_id': 'CUSTOM_001',
    'change_type': 'add_column',
    'from_constraint': 'any',
    'to_constraint': 'not_null',
    'data_type_category': 'string',
    'compatibility_level': 'warning',
    'auto_action': 'warn_proceed',
    'rule_config': {
        'description': 'Allow non-null string additions with warning'
    }
}

rules_engine.add_custom_rule(rule_data)
```

## Validation Report Structure

The system generates comprehensive JSON reports with the following structure:

```json
{
  "metadata": {
    "validation_id": "VAL_20250101_123456",
    "validation_timestamp": "2025-01-01T12:34:56",
    "data_source": "scopus",
    "table_name": "silver_stg_scopus",
    "load_id": "LOAD_20250101_123456"
  },
  "schema_info": {
    "schema_id": "scopus_silver_stg_scopus_1.2.0",
    "schema_version": "1.2.0",
    "schema_hash": "abc123...",
    "column_count": 25,
    "table_row_count": 10000
  },
  "validation_results": {
    "overall_status": "warning",
    "recommended_action": "warn_proceed",
    "has_changes": true,
    "change_count": 3,
    "breaking_changes": 0,
    "should_halt_pipeline": false
  },
  "changes": {
    "has_changes": true,
    "change_summary": {
      "columns_added": 2,
      "columns_removed": 0,
      "columns_modified": 1,
      "columns_reordered": 0
    },
    "detailed_changes": [...]
  },
  "evaluation": {
    "overall_compatibility": "warning",
    "overall_action": "warn_proceed",
    "evaluated_changes": [...],
    "rule_matches": [...]
  },
  "audit_data": {
    "audit_schema_version": "1.2.0",
    "audit_schema_hash_value": "abc123...",
    "schema_validation_status": "warning",
    "schema_validation_timestamp": "2025-01-01T12:34:56"
  }
}
```

## Integration Patterns

### Pipeline Integration

```python
from src.utils.schema_registry import SchemaRegistry

def validate_staging_table(data_source, table_name, load_id):
    """Validate schema before processing"""
    registry = SchemaRegistry(
        db_path="schema_registry.db",
        data_db_path="data.db"
    )
    
    validation_report, _ = registry.validate_and_report(
        data_source=data_source,
        table_name=table_name,
        load_id=load_id
    )
    
    # Check if pipeline should halt
    if validation_report['validation_results']['should_halt_pipeline']:
        raise Exception(
            f"Breaking schema changes detected in {table_name}. "
            f"Status: {validation_report['validation_results']['overall_status']}"
        )
    
    # Extract audit data for downstream tables
    audit_data = registry.extract_audit_data(validation_report)
    
    return audit_data

# Use in pipeline
try:
    audit_data = validate_staging_table("scopus", "silver_stg_scopus", "LOAD_123")
    # Continue with processing...
except Exception as e:
    # Handle validation failure
    logger.error(f"Schema validation failed: {e}")
    raise
```

### Audit Table Population

```python
def populate_audit_fields(conn, table_name, audit_data, load_id):
    """Populate audit columns in production table"""
    conn.execute(f"""
        UPDATE {table_name}
        SET 
            audit_schema_version = ?,
            audit_schema_hash_value = ?,
            schema_validation_status = ?,
            schema_validation_timestamp = ?
        WHERE load_id = ?
    """, (
        audit_data['audit_schema_version'],
        audit_data['audit_schema_hash_value'],
        audit_data['schema_validation_status'],
        audit_data['schema_validation_timestamp'],
        load_id
    ))
```

## Change Types

### Column Addition
- **Safe**: Adding nullable column or column with default value
- **Breaking**: Adding non-nullable column without default value

### Column Removal
- **Breaking**: Always considered breaking as downstream dependencies may fail

### Data Type Changes
- **Safe**: Widening conversions (INTEGER to BIGINT, VARCHAR(50) to VARCHAR(100))
- **Warning**: Narrowing conversions (VARCHAR(100) to VARCHAR(50))
- **Breaking**: Type category changes (STRING to NUMERIC, NUMERIC to STRING)

### Nullable Changes
- **Safe**: Making non-nullable column nullable
- **Breaking**: Making nullable column non-nullable

### Default Value Changes
- **Safe**: Usually safe, but may affect new row inserts

### Column Reordering
- **Safe**: Column position changes don't affect named column access

## Best Practices

### Schema Design
1. Always add new columns as nullable or with default values
2. Never remove columns without proper deprecation
3. Plan data type changes carefully, preferring widening over narrowing
4. Use semantic versioning to communicate change severity

### Validation Workflow
1. Register initial schema when onboarding new data sources
2. Run validation at the beginning of each pipeline run
3. Review warning-level changes regularly
4. Investigate breaking changes immediately
5. Maintain change documentation alongside schema versions

### Configuration Management
1. Use stricter validation rules in production environments
2. Configure appropriate notification methods for given team
3. Set halt_pipeline=true for breaking changes in production
4. Regularly review and update compatibility rules

### Monitoring
1. Monitor schema change frequency by data source
2. Track validation failure rates
3. Set up alerts for breaking changes
4. Review schema evolution patterns quarterly

## Troubleshooting

### Table Not Found
**Issue**: `Table does not exist` error during validation

**Solution**:
- Verify table name spelling and case sensitivity
- Ensure database connection is correct
- Check that table has been created in the specified database

### Schema Hash Mismatch
**Issue**: Schema appears changed but columns look identical

**Solution**:
- Check for whitespace differences in column names
- Verify data type representation (VARCHAR vs TEXT)
- Review column ordering if position-sensitive

### Rules Not Matching
**Issue**: Changes evaluated with default heuristics instead of rules

**Solution**:
- Review rule specificity and constraints
- Check that rules are marked as active
- Verify rule change_type matches detected changes
- Use `validate_config` command to inspect rules

### Performance Issues
**Issue**: Schema detection slow for large tables

**Solution**:
- Schema detection only queries metadata, not data
- If still slow, check database indices
- Consider running validation asynchronously

## API Reference

### SchemaRegistry

#### `validate_schema(data_source, table_name, load_id=None)`
Validates table schema and returns validation report.

#### `validate_and_report(data_source, table_name, load_id=None, save_report=True)`
Validates schema and saves JSON report. Returns (validation_report, report_path).

#### `register_initial_schema(data_source, table_name)`
Registers a table's schema for the first time.

#### `get_schema_history(data_source, table_name, limit=10)`
Retrieves schema version history.

#### `get_recent_changes(data_source=None, limit=20)`
Gets recent schema changes across sources.

#### `extract_audit_data(validation_report)`
Extracts audit fields from validation report.

### SchemaDetector

#### `extract_table_schema(table_name)`
Extracts complete schema information from a table.

#### `validate_table_exists(table_name)`
Checks if a table exists in the database.

#### `extract_load_id(table_name)`
Attempts to extract load_id from staging table.

### SchemaComparator

#### `compare_schemas(old_schema, new_schema)`
Compares two schemas and returns detected changes.

#### `has_breaking_changes(changes)`
Checks if any changes are breaking.

#### `get_change_impact_summary(changes)`
Generates human-readable summary of changes.
