# Research Data Analytics Platform

A work in progress data platform for ingesting, processing, validating and analysing bibliometric data from multiple academic sources. Built on medallion architecture principles with robust data quality controls and schema governance.

## Overview

This platform implements a complete data pipeline for research analytics, supporting ingestion from APIs such as Scopus, SciVal, Altmetric and Overton. The architecture follows medallion design patterns (bronze, silver, gold layers) with comprehensive validation, quality checking and schema registry capabilities.

### Key Features

- **Multi-source API Integration**: Modular adapter for REST API ingestion with state management and recovery
- **Medallion Architecture**: Bronze (raw), Silver (validated), Gold (aggregated) data layers
- **Schema Registry**: Automatic schema detection, versioning and compatibility checking
- **Data Quality Framework**: Tiered validation with Soda Core integration
- **Checksum Validation**: Data integrity verification across pipeline stages
- **Prefect Orchestration**: Workflow management and monitoring capabilities
- **DuckDB Storage**: High-performance analytical database for all layers

## Project Structure

```
prototype-analytics-platform/
├── config/
│   ├── apis/                        # API configurations
│   │   ├── altmetric.yaml
│   │   ├── overton.yaml
│   │   ├── scival.yaml
│   │   └── scopus.yaml
│   ├── connections.yaml             # Database connection parameters
│   └── environment.yaml             # Environment-specific settings
│
├── src/
│   ├── bronze/
│   │   ├── api/                     # Generic REST API adapter
│   │   │   ├── configs/             # TOML configurations per data source
│   │   │   ├── data/                # Input/output/state directories
│   │   │   ├── src/api_adapter/     # Core adapter components
│   │   │   ├── tests/               # Comprehensive test suite
│   │   │   ├── api_readme.md        # Detailed API adapter documentation
│   │   │   └── requirements.txt
│   │   └── archive/                 # Legacy ingestion scripts
│   │       └── archive_readme.md    # Documentation for archived scripts
│   │
│   ├── silver/
│   │   ├── JSON_to_tabular_EtLT/    # Extract, transform, Load, Transform pipeline
│   │   │   ├── scopus_json_EtLT_to_staging_table.py
│   │   │   ├── stg_schema.sql       # Staging table DDL
│   │   │   └── scopus_etlt_readme.md
│   │   └── metadata_pipeline/       # Audit metadata management
│   │       ├── silver_metadata_etl.py
│   │       └── silver_metadata_etl_readme.md
│   │
│   └── utils/
│       ├── checksum_tool/           # Data integrity validation
│       │   ├── checksum_utility.py
│       │   ├── scopus_checksum.yml
│       │   └── checksum_tool_readme.md
│       ├── schema_registry/         # Schema versioning and compatibility
│       │   ├── core.py
│       │   ├── detector.py
│       │   ├── comparator.py
│       │   ├── rules_engine.py
│       │   ├── database.py
│       │   ├── config.py
│       │   ├── run_schema_registry.py
│       │   ├── config/
│       │   │   └── schema_registry_config.yml
│       │   └── schema_registry_readme.md
│       ├── soda_data_quality/       # Tiered data quality checks
│       │   ├── checks/
│       │   │   ├── scopus_critical_checks.yml
│       │   │   ├── scopus_quality_checks.yml
│       │   │   └── scopus_monitoring_checks.yml
│       │   ├── soda_configuration.yml
│       │   └── soda_dq.py
│       └── ge_auto_profiler.py      # Great Expectations profiling tool
│
└── README.md                        # This file
```

## Architecture

### Medallion Data Layers

**Bronze Layer (Raw Data)**
- Raw API responses stored as JSON in DuckDB
- Complete technical metadata preservation
- State management for resumable processing
- Idempotent UPSERT operations

**Silver Layer (Validated Data) - not completed**
- Flattened tabular structures from JSON
- Schema validation and versioning
- Data quality checks (tiered approach)
- Checksum verification
- Audit metadata tracking
- Utilise AutomateDV dbt package for generating Data Vault 2.0 logic

**Gold Layer (Analytics-Ready) - not completed**
- Aggregated and enriched datasets
- Business logic transformations
- Optimised for analytical queries
- See HP-DWH repo for example gold dataset

### Data Flow

```
API Sources → Bronze (Raw JSON) → Silver (Validated Tables) → Gold (Analytics)
              ↓                    ↓                          ↓
              State Management     Schema Registry            Aggregations
              Cache (optional)     Checksum Validation
                                  Data Quality Checks
```

## Components

### Bronze Layer: API Adapter

A generic REST API adapter designed for bibliometric data sources with comprehensive state management and error recovery.

**Location**: `src/bronze/api/`

**Key Features**:
- TOML-based configuration for multiple data sources
- State persistence using DuckDB for recovery
- Configurable payload validation
- Multiple pagination strategies (offset, page, cursor)
- Rate limiting and usage tracking
- Optional development caching
- Prefect orchestration support

**Usage**:
```bash
# Run complete pipeline
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/scopus_search_entities.txt \
  --run

# With caching enabled (development)
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/scopus_search_entities.txt \
  --run --cache
```

**Core Components**:
- `ConfigLoader`: TOML configuration and environment validation
- `DatabaseManager`: DuckDB connection and transaction management
- `StateManager`: Processing state persistence and recovery
- `HTTPClient`: Authentication, retries and rate limiting
- `PayloadValidator`: Configurable response validation
- `PaginationStrategy`: Multiple pagination pattern support
- `FileHasher`: Content deduplication
- `ManifestGenerator`: Processing summary generation
- `RecoveryManager`: Partial failure recovery
- `RateLimitTracker`: API usage tracking across sessions
- `CacheManager`: Development caching with lock management

### Silver Layer: EtLT Pipeline

Transforms raw JSON responses into validated tabular structures.

**Location**: `src/silver/JSON_to_tabular_EtLT/`

**Key Features**:
- JSON structure flattening
- Intelligent field mapping
- Context-aware transformations
- Array handling and normalisation
- Metadata preservation
- Author name construction
- Null normalisation

**Usage**:
```bash
python scopus_json_EtLT_to_staging_table.py
```

**Transformation Highlights**:
- Removes Scopus-specific `@_fa` keys
- Constructs full author names from components
- Converts pipe-delimited strings to JSON arrays
- Normalises various null representations
- Generates content hashes for deduplication

### Silver Layer: Metadata Pipeline

Transfers audit metadata from staging to permanent storage.

**Location**: `src/silver/metadata_pipeline/`

**Key Features**:
- Atomic operations with staging table pattern
- Idempotent execution (safe to re-run)
- MERGE pattern for duplicate handling
- Read-only source connections
- Automatic cleanup on failure

**Usage**:
```bash
python silver_metadata_etl.py scopus_test.db
```

### Utilities: Schema Registry

Automatic schema detection, versioning and compatibility checking.

**Location**: `src/utils/schema_registry/`

**Key Features**:
- Automatic schema detection from DuckDB tables
- Change detection (additions, removals, modifications)
- Compatibility assessment (safe, warning, breaking)
- Semantic versioning (major.minor.patch)
- Complete audit trail
- Configurable pipeline control
- Standalone operation

**Usage**:
```bash
# Validate a table
python run_schema_registry.py validate scopus silver_stg_scopus \
  --load-id LOAD_20250101_123456

# Register initial schema
python run_schema_registry.py register scopus silver_stg_scopus

# View schema history
python run_schema_registry.py history scopus silver_stg_scopus
```

**Programmatic Usage**:
```python
from src.utils.schema_registry import SchemaRegistry

registry = SchemaRegistry(
    db_path="schema_registry.db",
    data_db_path="your_data.db"
)

validation_report, report_path = registry.validate_and_report(
    data_source="scopus",
    table_name="silver_stg_scopus",
    load_id="LOAD_20250101_123456"
)
```

### Utilities: Checksum Tool

Data integrity validation through checksum generation and comparison.

**Location**: `src/utils/checksum_tool/`

**Key Features**:
- Deterministic checksums using DuckDB native hash function
- Flexible column selection
- Automatic type casting
- Critical column validation
- Empty table detection
- Comparison mode for source/destination validation
- YAML configuration support

**Usage**:
```bash
# Generate checksum
python checksum_utility.py database.db table_name

# Compare checksums
python checksum_utility.py source.db source_table \
  --compare dest.db dest_table

# Use configuration file
python checksum_utility.py --config scopus_checksum.yml
```

### Utilities: Data Quality Framework

Tiered data quality validation using Soda Core.

**Location**: `src/utils/soda_data_quality/`

**Three-Tier Approach**:

1. **Critical Checks** (Pipeline Blocking)
   - Core identifier validation
   - Schema compliance
   - Essential field presence

2. **Quality Checks** (Investigation Required)
   - Content quality standards
   - Format compliance
   - Duplication detection

3. **Monitoring Checks** (Trend Analysis)
   - Length monitoring with progressive thresholds
   - Range monitoring
   - Duplication trends

**Usage**:
```bash
python soda_dq.py silver_stg_scopus
```

**Exit Codes**:
- 0: Success (all checks passed or warnings only)
- 1: Quality issues detected
- 2: Critical failures (pipeline blocking)
- 3: System error

## Installation

### Prerequisites

- Python 3.10 or higher
- DuckDB
- Virtual environment tool (venv or conda)

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r src/bronze/api/requirements.txt
pip install duckdb pyyaml soda-core-duckdb
```

### Environment Configuration

Create a `.env` file in the project root:

```bash
# API credentials
SCOPUS_API_KEY=your_scopus_key
SCOPUS_INST_ID=your_institution_token
SCIVAL_API_KEY=your_scival_key
ALTMETRIC_API_KEY=your_altmetric_key
OVERTON_API_KEY=your_overton_key
```

## Configuration

### API Configuration

Each data source requires a TOML configuration file in `src/bronze/api/configs/`:

```toml
[api]
name = "scopus_search"
base_url = "https://api.elsevier.com/content/search/scopus"

[authentication]
type = "api_key_and_token"
method = "query_params"
api_key_env = "SCOPUS_API_KEY"
inst_token_env = "SCOPUS_INST_ID"

[pagination]
strategy = "offset_limit"
items_per_page = 25

[rate_limits]
requests_per_second = 2.0
weekly_limit = 20000
```

### Schema Registry Configuration

Configuration file at `src/utils/schema_registry/config/schema_registry_config.yml`:

```yaml
database_path: "src/utilities/schema_registry/schema_registry.db"
reports_directory: "src/utilities/schema_registry/schema_reports"

enable_auto_versioning: true

version_increment_rules:
  safe: "patch"
  warning: "minor"
  breaking: "major"

notifications:
  safe:
    method: "log"
    level: "info"
    halt_pipeline: false
  
  warning:
    method: "both"
    level: "warning"
    halt_pipeline: false
  
  breaking:
    method: "both"
    level: "error"
    halt_pipeline: true
```

## Complete Pipeline Workflow

### 1. Data Ingestion (Bronze Layer)

```bash
# Ingest from Scopus API
cd src/bronze/api
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/scopus_search_entities.txt \
  --run
```

### 2. EtLT Transformation (Silver Staging)

```bash
# Transform JSON to tabular format
cd src/silver/JSON_to_tabular_EtLT
python scopus_json_EtLT_to_staging_table.py
```

### 3. Schema Validation

```bash
# Validate schema
cd src/utils/schema_registry
python run_schema_registry.py validate scopus silver_stg_scopus \
  --load-id LOAD_20250101_123456
```

### 4. Data Quality Checks

```bash
# Run tiered quality checks
cd src/utils/soda_data_quality
python soda_dq.py silver_stg_scopus
```

### 5. Checksum Verification

```bash
# Verify data integrity
cd src/utils/checksum_tool
python checksum_utility.py --config scopus_checksum.yml
```

### 6. Metadata Transfer (Silver Production)

```bash
# Transfer audit metadata to permanent storage
cd src/silver/metadata_pipeline
python silver_metadata_etl.py scopus_test.db
```

## Testing

Some components of the platform include comprehensive test coverage following Test-Driven Development principles.

### API Adapter Tests

```bash
cd src/bronze/api
pytest src/tests/ -v

# With coverage
pytest src/tests/ --cov=src/api_adapter --cov-report=html
```

### Test Structure

- AAA pattern (Arrange, Act, Assert)
- Descriptive naming conventions
- Mock external dependencies
- BDD requirements documented

## Monitoring and Observability

### Logging

All components use structured logging with appropriate levels:

- **DEBUG**: Detailed execution information
- **INFO**: Progress and milestone events
- **WARNING**: Potential issues requiring attention
- **ERROR**: Failures requiring investigation

### Reports and Manifests

Each pipeline stage generates comprehensive reports:

- **API Ingestion**: Processing manifests with statistics
- **Schema Registry**: Validation reports with change details
- **Data Quality**: Tiered check results with pass/fail status
- **Checksum**: Integrity verification results

### Metrics

Key metrics tracked throughout the pipeline:

- Processing duration
- Success/failure rates
- Data volumes
- Quality check pass rates
- Schema change frequency

## Error Handling and Recovery

### State Management

The platform implements robust state management:

- **Bronze Layer**: DuckDB-based state persistence
- **Resume Capability**: Automatic continuation from last checkpoint
- **Partial Failure Handling**: Continue processing remaining items
- **File Integrity**: Hash-based deduplication

### Recovery Patterns

```bash
# Resume API ingestion with existing load ID
python prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/entities.txt \
  --load-id scopus_20250101_123456 \
  --run

# Re-run EtLT (idempotent)
python scopus_json_EtLT_to_staging_table.py

# Re-run metadata transfer (safe due to MERGE pattern)
python silver_metadata_etl.py scopus_test.db
```

## Best Practices

### API Ingestion

1. Always enable caching during development to avoid quota consumption
2. Use load IDs for tracking and recovery
3. Monitor rate limits through logs
4. Validate configurations before processing large entity lists

### Schema Management

1. Register initial schemas when onboarding new data sources
2. Run validation at the beginning of each pipeline run
3. Review warning-level changes regularly
4. Investigate breaking changes immediately

### Data Quality

1. Establish baseline quality metrics
2. Monitor trends in quality checks
3. Set appropriate thresholds for each tier
4. Review failed checks systematically

### Pipeline Operations

1. Run checksums after each transformation stage
2. Maintain audit trail of all processing runs
3. Archive staging data periodically
4. Document schema changes alongside versions

## Performance Considerations

### Current Scale

- Approximately 8,000 entities (authors/institutions)
- Up to 200 pages per entity
- Approximately 10,000 total publications expected per year
- Single-threaded processing to respect rate limits

### Optimisation Strategies

**Bronze Layer**:
- State management prevents reprocessing
- UPSERT operations for idempotency
- Content hashing for deduplication
- Indexed database queries

**Silver Layer**:
- Batch loading via DataFrames
- SQL-based transformations
- Truncate-and-reload for clean state

**Utilities**:
- Read-only database connections where possible
- Efficient checksum algorithms
- Minimal memory footprint

## Troubleshooting

### Common Issues

**Database Connection Errors**
```bash
# Verify database file exists and has correct permissions
ls -la data/databases/
```

**Configuration Validation Failures**
```bash
# Check TOML syntax
python -c "import tomllib; tomllib.load(open('config.toml', 'rb'))"
```

**Rate Limit Exceeded**
```bash
# Check current usage
cat data/rate_limits/scopus_search_usage.json
```

**Authentication Failures**
```bash
# Verify environment variables
python -c "import os; print(os.getenv('SCOPUS_API_KEY'))"
```

### Debug Mode

Enable detailed logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Development

### Adding a New Data Source

1. Create TOML configuration in `src/bronze/api/configs/`
2. Create entity input file in `data/input/`
3. Test configuration validation
4. Run with caching enabled
5. Add schema to registry
6. Configure quality checks
7. Set up checksum validation

### Extending the Pipeline

The modular architecture allows for:

- Additional API adapters
- Custom transformation logic
- New validation rules
- Enhanced quality checks
- Alternative storage backends

## Documentation

Detailed documentation available for each component:

- API Adapter: `src/bronze/api/api_readme.md`
- Archived Scripts: `src/bronze/archive/archive_readme.md`
- EtLT Pipeline: `src/silver/JSON_to_tabular_EtLT/scopus_etlt_readme.md`
- Metadata Pipeline: `src/silver/metadata_pipeline/silver_metadata_etl_readme.md`
- Schema Registry: `src/utils/schema_registry/schema_registry_readme.md`
- Checksum Tool: `src/utils/checksum_tool/checksum_tool_readme.md`