# Generic REST API Adapter for Bibliometric Data Sources

A modular, test-driven Python application for ingesting data from REST APIs with comprehensive state management, error recovery, and configurable validation. Designed for bibliometric data sources (Scopus, SciVal, Web of Science) but adaptable to any JSON based REST API. Behaviour driven development has been used to map system requirements in familiar domain language (see docs/bdd_gherkin_requirements.md)

## Overview

This adapter provides a robust solution for API data ingestion with the following key features:

- **TOML-based configuration** for business logic (rate limits, pagination, validation rules)
- **.env file management** for development secrets
- **DuckDB-based state persistence** for resumable processing and failure recovery
- **Comprehensive error handling** with partial failure support
- **Configurable payload validation** per API requirements
- **Multiple pagination strategies** (offset-based, page-based, cursor-based)
- **Rate limiting and usage tracking** across sessions
- **Optional development caching** to avoid API quotas during testing
- **Prefect orchestration** for workflow management and monitoring

## Architecture Principles

- **Object-oriented design** with composition over inheritance
- **Single Responsibility Principle** - each component has one clear purpose
- **Constructor dependency injection** for testability
- **Test-Driven Development** with comprehensive test coverage
- **Direct method calls** between components (no event-driven complexity)

## Project Structure

```
src/bronze/api/
├── configs/                    # TOML configuration files per data source
│   └── scopus_search.toml     # Example: Scopus API configuration
├── data/
│   ├── input/                 # Entity lists (author IDs, institution IDs)
│   ├── output/
│   │   ├── raw/              # Raw JSON responses
│   │   └── manifest/         # Processing summaries
│   ├── state/                # State management data
│   ├── databases/            # DuckDB database files
│   └── cache/                # Development cache (optional)
├── src/
│   └── api_adapter/
│       ├── config_loader.py           # TOML configuration loading
│       ├── database_manager.py        # DuckDB connection management
│       ├── state_manager.py           # Processing state persistence
│       ├── http_client.py             # HTTP requests with retry logic
│       ├── payload_validator.py       # Response validation
│       ├── pagination_strategy.py     # Pagination pattern handling
│       ├── file_hasher.py            # Content deduplication
│       ├── manifest_generator.py      # Processing summary generation
│       ├── recovery_manager.py        # Partial failure recovery
│       ├── rate_limit_tracker.py     # API usage tracking
│       ├── cache_manager.py          # Development caching
│       └── prefect_api_orchestrator.py # Prefect workflow orchestration
└── tests/                     # Comprehensive test suite (TDD approach)
```

## Installation

### Prerequisites

- Python 3.10 or higher
- Virtual environment tool (venv or conda)

### Setup Instructions

1. **Clone the repository**
   ```bash
   cd src/bronze/api/
   ```

2. **Create and activate virtual environment**
   ```bash
   # Using venv
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Or using conda
   conda create -n api_adapter python=3.10
   conda activate api_adapter
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   
   Create a `.env` file in the `src/bronze/api/` directory:
   ```bash
   # Example .env file
   SCOPUS_API_KEY=your_scopus_api_key_here
   SCOPUS_INST_ID=your_institution_token_here
   SCIVAL_API_KEY=your_scival_api_key_here
   WOS_API_KEY=your_wos_api_key_here
   ```

5. **Verify installation**
   ```bash
   # Run tests to verify setup
   pytest src/tests/ -v
   ```

## Configuration

### TOML Configuration Files

Each data source requires a TOML configuration file in `configs/`. See `configs/scopus_search.toml` for a complete example.

**Key configuration sections:**

```toml
[api]
name = "scopus_search"
base_url = "https://api.elsevier.com/content/search/scopus"

[authentication]
type = "api_key_and_token"  # Options: api_key, bearer_token, api_key_and_token
method = "query_params"      # Options: query_params, headers
api_key_env = "SCOPUS_API_KEY"
inst_token_env = "SCOPUS_INST_ID"

[pagination]
strategy = "offset_limit"    # Options: offset_limit, page_based, cursor_based
items_per_page = 25
start_param = "start"
count_param = "count"
total_results_path = "search-results.opensearch:totalResults"

[rate_limits]
requests_per_second = 2.0
weekly_limit = 20000

[retries]
max_attempts = 3
backoff_factor = 2.0
status_codes_to_retry = [429, 500, 502, 503, 504]

[payload_validation]
response_completeness = true
rate_limit_headers = true
empty_response_detection = true
json_structure_validation = true

[payload_validation.headers]
total_results_field = "search-results.opensearch:totalResults"
rate_limit_remaining = "X-RateLimit-Remaining"
```

### Entity Input Files

Create text files with one entity ID per line:

```
# data/input/scopus_search_entities.txt
7004212771
7005400538
24726138500
```

## Usage

### Using Prefect Orchestration (Recommended)

Prefect provides workflow management, monitoring, and scheduling capabilities.

**Start Prefect server** (in a separate terminal):
```bash
prefect server start
```

**Run complete pipeline:**
```bash
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/scopus_search_entities.txt \
  --run
```

**With development caching enabled:**
```bash
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --entities data/input/scopus_search_entities.txt \
  --run \
  --cache
```

**Create sample entity file:**
```bash
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --create-sample
```

**Validate configuration only:**
```bash
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/scopus_search.toml \
  --validate-only
```

**View Prefect dashboard:**
Open your browser to `http://127.0.0.1:4200` to monitor workflow execution, view logs, and track processing status.

## Core Features

### State Management & Recovery

The adapter uses DuckDB to persist processing state, enabling:

- **Automatic resumption** from the last successfully processed page
- **Partial failure recovery** - failed entities don't stop processing
- **File integrity validation** using content hashes
- **Deduplication** via UPSERT operations

Processing state includes:
- Last processed page number
- Total results count
- Processed item identifiers (DOIs, etc.)
- File names and content hashes
- Error logs with page numbers

### Error Handling

**Three-tier error handling strategy:**

1. **Component-level**: Expected errors handled within components
2. **Entity-level**: Entity failures recorded, processing continues with remaining entities
3. **Orchestrator-level**: Unexpected exceptions bubble up for visibility

**Critical vs transient errors:**
- **Stop processing**: Rate limit exceeded, authentication failures, permanent API errors
- **Continue processing**: Timeouts, temporary server errors, validation failures

### Payload Validation

Configurable validation methods per data source:

- **Response completeness**: Verify totalResults matches actual item count
- **Rate limit headers**: Check for presence of rate limiting information
- **JSON structure**: Validate expected top-level keys exist
- **Empty response detection**: Identify responses with no data

Each validation can be enabled/disabled in TOML configuration.

### Rate Limiting

**Two-level rate limiting:**

1. **Requests per second**: Enforced between individual requests
2. **Weekly limits**: Tracked across sessions using persistent storage

Usage tracking survives script restarts and provides quota monitoring.

### Pagination Strategies

**Three pagination patterns supported:**

1. **Offset-based** (Scopus): `?start=0&count=25`
2. **Page-based** (SciVal): `?page=1&size=25`
3. **Cursor-based** (Web of Science): `?cursor=abc123&limit=25`

Configured via TOML `pagination.strategy` field.

## Data Storage

### Raw Data Database (`api_ingestion.db`)

Stores complete API responses with technical metadata:

```sql
CREATE TABLE api_responses (
    load_id VARCHAR,
    data_source VARCHAR,
    entity_id VARCHAR,
    page_number INTEGER,
    request_timestamp TIMESTAMP,
    response_timestamp TIMESTAMP,
    raw_response JSON,
    technical_metadata JSON,
    log_entry JSON,
    file_hash VARCHAR,
    PRIMARY KEY (load_id, entity_id, page_number)
);
```

### State Management Database (`api_state_management.db`)

Tracks processing progress for recovery:

```sql
CREATE TABLE processing_state (
    load_id VARCHAR,
    entity_id VARCHAR,
    data_source VARCHAR,
    last_page INTEGER,
    last_start_index INTEGER,
    total_results INTEGER,
    pages_processed INTEGER,
    completed BOOLEAN,
    processed_items JSON,
    file_names JSON,
    file_hashes JSON,
    errors JSON,
    created_timestamp TIMESTAMP,
    last_updated TIMESTAMP,
    PRIMARY KEY (load_id, entity_id)
);
```

### Processing Manifests

JSON manifests generated for each processing run containing:

- **Summary statistics**: Success rate, total entities, total responses
- **Entity-level details**: Processing time, pages processed, errors
- **Error summary**: Error types, affected entities
- **Processing duration**: Total time, average per entity

## Testing

Comprehensive test suite following Test-Driven Development principles:

```bash
# Run all tests
pytest src/tests/ -v

# Run specific test file
pytest src/tests/test_http_client.py -v

# Run with coverage report
pytest src/tests/ --cov=src/api_adapter --cov-report=html
```

**Test structure:**
- **AAA pattern**: Arrange, Act, Assert
- **Descriptive naming**: `test_[method]_[scenario]_[expected_outcome]()`
- **Mock external dependencies** for isolated unit testing
- **BDD requirements** documented in `bdd_gherkin_requirements.md`

## Development

### Adding a New Data Source

1. **Create TOML configuration** in `configs/`:
   ```bash
   cp configs/scopus_search.toml configs/your_api.toml
   ```

2. **Update configuration** with API-specific settings:
   - Authentication type and environment variables
   - Pagination strategy and parameters
   - Rate limits
   - Response field mappings

3. **Create entity input file** in `data/input/`:
   ```bash
   echo "entity_001" > data/input/your_api_entities.txt
   echo "entity_002" >> data/input/your_api_entities.txt
   ```

4. **Test configuration**:
   ```bash
   python src/api_adapter/prefect_api_orchestrator.py \
     --config configs/your_api.toml \
     --validate-only
   ```

5. **Run with caching** during development:
   ```bash
   python src/api_adapter/prefect_api_orchestrator.py \
     --config configs/your_api.toml \
     --entities data/input/your_api_entities.txt \
     --run \
     --cache
   ```

### Development Best Practices

- **Always enable caching** during development to avoid API quota consumption
- **Write tests first** following TDD methodology
- **Use descriptive commit messages** following conventional commits
- **Update BDD requirements** when adding new features
- **Validate configuration** before processing large entity lists

## Troubleshooting

### Common Issues

**Configuration validation fails:**
```bash
# Check TOML syntax
python -c "import tomllib; tomllib.load(open('configs/your_config.toml', 'rb'))"

# Verify environment variables
python src/api_adapter/prefect_api_orchestrator.py \
  --config configs/your_config.toml \
  --validate-only
```

**Database connection errors:**
```bash
# Ensure database directories exist
mkdir -p data/databases

# Check database file permissions
ls -la data/databases/
```

**Rate limit exceeded:**
```bash
# Check current usage
# Look for rate_limits directory
cat data/rate_limits/scopus_search_usage.json

# Wait for weekly window to reset or adjust limits in TOML
```

**Authentication failures:**
```bash
# Verify environment variables are set
python -c "import os; print(os.getenv('SCOPUS_API_KEY'))"

# Check .env file location (must be in api/ root directory)
ls -la .env
```

### Logging

Logs are written to `logs/` directory with API-specific names (configured in TOML):

```bash
# View recent logs
tail -f logs/scopus_search_api.log

# Search for errors
grep ERROR logs/scopus_search_api.log
```

## Performance Considerations

**Current scale:**
- ~8,000 entities (authors/institutions)
- ~200 pages per entity maximum
- ~10,000 total publications expected
- Single-threaded processing to respect rate limits

**Optimisation strategies:**
- State management prevents reprocessing
- UPSERT operations for idempotency
- Content hashing for deduplication
- Indexed database queries for fast lookups

---

## Advanced Features

### Custom Pagination Strategy

To add a new pagination strategy:

1. Create class in `pagination_strategy.py`:
   ```python
   class CustomPagination:
       def get_next_page_params(self, current_params, response, page_num):
           # Implementation
           pass
       
       def extract_total_results(self, response):
           # Implementation
           pass
   ```

2. Register in `PaginationFactory.STRATEGIES`:
   ```python
   STRATEGIES = {
       'custom': CustomPagination,
       # ... existing strategies
   }
   ```

3. Use in TOML configuration:
   ```toml
   [pagination]
   strategy = "custom"
   ```

### Query Customisation

For API-specific query formatting, the orchestrator automatically handles:

- **Scopus**: Wraps entity IDs with `AU-ID(entity_id)`
- **Other APIs**: Uses entity ID directly

Override by modifying `_build_api_request()` in the orchestrator.
