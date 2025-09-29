# Generic REST API Adapter - Requirements Summary

## Architecture Overview

**Objective:** Refactor the existing Scopus API ingestion script into a generalised REST API adapter that can handle multiple bibliometric data sources (Scopus, SciVal, Web of Science) via TOML configuration.

**Design Principles:**
- Object-oriented design with composition
- Simple, focused components with single responsibility
- Constructor dependency injection for testability
- Direct method calls between components (no event-driven complexity)

## Core Components

### 1. Configuration Management
- **TOML files** for business configuration (rate limits, endpoints, pagination settings)
- **.env files** for development secrets (API keys, tokens)
- **Startup validation** of all configuration via `config_validator` script
- **No hot reloading** - restart required for configuration changes
- **TOML specifies environment variable names**, Python derives secrets from .env
- **API-specific log file names** configured in TOML (e.g., 'scopus_search_api.log')

### 2. Interface Design
**Core Interfaces (required for all data sources):**
- `Authentication` - credential management
- `Retry` - retry logic with exponential backoff  
- `RateLimited` - enforce requests per second limits

**Optional Interface:**
- `Cacheable` - development-only caching via CLI flag `--cache=true`

### 3. Component Architecture

#### APIOrchestrator
- Simple Python script coordinating the workflow
- Handles high-level progress reporting and error coordination
- Continues processing remaining entities on partial failures
- **Methods:**
  - `load_entities_from_file(file_path: Path) -> List[str]`
  - `generate_load_id(data_source: str) -> str`
  - `pass_load_id_to_components(load_id: str) -> None`
  - `process_single_entity(entity_id: str, load_id: str) -> None`
  - `handle_partial_failure(entity_id: str, error: Exception) -> None`
  - `calculate_processing_metrics(results: Dict) -> Dict`

#### HTTPClient  
- Pure HTTP operations with authentication
- Handles network timeouts, HTTP 4xx/5xx errors with integrated retry logic
- Creates new connection per script run, closes on completion
- Uses standard context managers for resource cleanup
- **Methods:**
  - `authenticate(credentials: Dict[str, Any]) -> None`
  - `make_request(request: APIRequest) -> APIResponse` (includes retry + exponential backoff)
  - `apply_rate_limit() -> None`
  - `close_connection() -> None`

#### PayloadValidator
- **Per-page validation** during processing (acceptable I/O overhead for scale)
- **Configurable validation methods** via TOML with boolean enabled/disabled fields
- **Response completeness** validation (totalResults vs actual items)
- **Rate limit header** tracking (configurable per data source)
- **JSON structure validation** - schema existence, response type, empty response detection
- **No detailed field validation** (handled in separate downstream flattening script)
- **API-specific field mapping** in TOML (e.g., Scopus uses "opensearch:totalResults")
- **Methods:**
  - `validate_response(response: APIResponse, config: Dict) -> bool` (calls individual validations)
  - `validate_response_completeness(response: APIResponse, config: Dict) -> bool`
  - `validate_rate_limit_headers(response: APIResponse, config: Dict) -> bool`
  - `validate_json_structure(response: APIResponse) -> bool`
  - `detect_empty_response(response: APIResponse) -> bool`

#### StateManager
- **DuckDB-based state persistence** in unified database file (`api_state_management.db`)
- **Single consolidated state table** with composite primary key (load_id, entity_id)
- **JSON columns** for arrays and complex data (processed_items, file_names, file_hashes, errors)
- **Input file as source of truth** for entities to process (no database tracking of requested entities)
- **Simple recovery logic** via SQL queries for incomplete entities
- Atomic transaction updates
- **Methods:**
  - `load_state(entity_id: str, load_id: str) -> ProcessingState | None`
  - `save_state(state: ProcessingState) -> None`
  - `mark_completed(entity_id: str, load_id: str) -> None`
  - `get_incomplete_entities(entity_ids: List[str], load_id: str) -> List[str]`
  - `update_page_progress(entity_id: str, load_id: str, page_num: int) -> None`
  - `add_processed_item(entity_id: str, load_id: str, identifier: str) -> None`
  - `record_file_hash(entity_id: str, load_id: str, filename: str, hash: str) -> None`
  - `log_processing_error(entity_id: str, load_id: str, error: Exception, page_num: int) -> None`

#### DatabaseManager
- **Dedicated database management class** for separation of concerns
- Database connection lifecycle management
- Schema creation and validation
- **Consolidated transaction management** with execute_with_transaction pattern
- **UPSERT operations** for idempotent processing
- Query execution interface
- **Methods:**
  - `create_connection(db_path: Path) -> Connection`
  - `create_tables() -> None`
  - `execute_with_transaction(query: str, params: tuple) -> Any`
  - `upsert_api_response(response_data: Dict) -> None`
  - `insert_manifest_data(manifest_data: Dict) -> None`
  - `check_response_exists(load_id: str, entity_id: str, page_num: int) -> bool`
  - `verify_data_integrity(load_id: str, entity_id: str) -> bool`
  - `close_connection() -> None`

#### FileHasher
- **Content deduplication logic** for idempotent file operations
- **Methods:**
  - `generate_content_hash(data: Dict[str, Any]) -> str`
  - `compare_file_hashes(hash1: str, hash2: str) -> bool`

#### ManifestGenerator
- **Separate manifest generation logic** from orchestrator
- **Methods:**
  - `generate_manifest_data(processing_results: Dict) -> Dict`
  - `calculate_summary_statistics(results: Dict) -> Dict`

#### RecoveryManager
- **Handles partial failure restart logic** from original script
- **Methods:**
  - `identify_failed_pages(entity_id: str, load_id: str) -> List[int]`
  - `validate_existing_files(entity_id: str, load_id: str) -> bool`
  - `restart_from_page(entity_id: str, load_id: str, page_num: int) -> None`

#### RateLimitTracker
- **Tracks API usage across sessions** for weekly limits
- **Methods:**
  - `track_request(data_source: str, timestamp: datetime) -> None`
  - `check_weekly_limit(data_source: str) -> bool`
  - `get_current_usage(data_source: str) -> int`

#### CacheManager
- **Development-only feature** enabled via CLI
- **API-specific cache namespaces** (scopus_search_cache, scival_metrics_cache)
- **Simple file lock with timeout** (30 seconds) for rare race conditions
- **Cache lock cleanup** on process termination
- **Methods:**
  - `get_cached_response(cache_key: str) -> APIResponse | None`
  - `store_response(cache_key: str, response: APIResponse) -> None`
  - `generate_cache_key(request: APIRequest) -> str`
  - `clear_cache() -> None`
  - `acquire_lock() -> bool`
  - `release_lock() -> None`

#### PaginationStrategy
- **Explicit registration** via factory pattern
- Support for offset-based, page-based, and cursor-based pagination
- Configurable via TOML `pagination.strategy` field

## Data Storage Strategy

### Raw Data Storage
- **JSON payloads** stored as JSON columns in DuckDB
- **Technical metadata** included (data_source, request_timestamp, load_id)
- **Partition by data_source** for performance
- **Index by load_id** for chronological ordering
- **No data extraction** - store complete raw responses

### Raw Data Schema
```sql
-- Main data database: api_ingestion.db
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

-- Normalised manifest table for processing summaries
CREATE TABLE processing_manifests (
    load_id VARCHAR PRIMARY KEY,
    data_source VARCHAR,
    manifest_data JSON,
    processing_summary JSON,
    generated_timestamp TIMESTAMP,
    total_entities INTEGER,
    total_responses INTEGER
);

-- Partition by data_source, indexed by load_id
CREATE INDEX idx_data_source_load_id ON api_responses (data_source, load_id);
CREATE INDEX idx_request_timestamp ON api_responses (request_timestamp);
```

**UPSERT Pattern for Idempotency:**
```sql
INSERT INTO api_responses (...) VALUES (...) 
ON CONFLICT (load_id, entity_id, page_number) 
DO UPDATE SET raw_response = EXCLUDED.raw_response,
              technical_metadata = EXCLUDED.technical_metadata,
              log_entry = EXCLUDED.log_entry;
```

**Example Data Sources:**
- `scival_author_lookup` - SciVal author profile data
- `scopus_search` - Scopus publication search results  
- `wos_expanded` - Web of Science expanded record data
- `wos_incites` - InCites analytics data

### State Management Schema
```sql
-- Unified state database: api_state_management.db
CREATE TABLE processing_state (
    load_id VARCHAR,
    entity_id VARCHAR,
    data_source VARCHAR,
    last_page INTEGER DEFAULT 0,
    last_start_index INTEGER DEFAULT 0,
    total_results INTEGER,
    pages_processed INTEGER DEFAULT 0,
    completed BOOLEAN DEFAULT FALSE,
    processed_items JSON,    -- Array of DOIs/identifiers
    file_names JSON,         -- Array of generated file names  
    file_hashes JSON,        -- Map of filename -> hash
    errors JSON,             -- Array of error objects
    created_timestamp TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (load_id, entity_id)
);
```

**Recovery Logic:**
- Input file (author_ids.txt) defines entities to process
- Query incomplete entities: `SELECT entity_id FROM processing_state WHERE load_id = ? AND completed = FALSE`

### Logging Strategy
- **Structured JSON logging** for debugging and operational metadata
- **Log levels:** DEBUG (pagination), INFO (progress), WARN (retries), ERROR (failures)
- **Log JSON stored as column** in DuckDB for unified technical metadata
- **Metrics emission** alongside logs (processing rates, error counts)
- **API-specific log file names** from TOML configuration

## Rate Limiting & Caching

### Rate Limiting
- **Weekly rate limits** configured in TOML (with default fallback)
- **Requests per second** enforcement as primary throttling mechanism
- **No sub-weekly tracking** to avoid complexity

### Caching
- **Development-only feature** enabled via CLI
- **API-specific cache namespaces** (scopus_search_cache, scival_metrics_cache)
- **Simple file lock with timeout** (30 seconds) for rare race conditions
- **Cache lock cleanup** on process termination

## Error Handling & Recovery

### Error Handling Strategy
- **Component-level error handling** for expected errors
- **Bubble up unexpected exceptions** to orchestrator level
- **Continue processing on partial failures**
- **Structured error logging** with stack traces in JSON format

### Recovery Mechanism
- **Resume from last successful page** using DuckDB state
- **File hash deduplication** to prevent duplicate downloads
- **Validation of existing files** before marking complete
- **State-based incomplete entity detection**

## Configuration Structure

### TOML Configuration Sections
```toml
[api] # name, base_url
[authentication] # type, environment variable references
[pagination] # strategy, items_per_page, parameter names
[rate_limits] # requests_per_second, weekly_limit (optional)
[retries] # max_attempts, backoff_factor, status_codes_to_retry
[default_parameters] # API-specific default parameters
[cache] # enabled, expiration_seconds, directory
[response_mapping] # paths for extracting pagination info
[logging] # log_file_name for API-specific logging

[payload_validation] # configurable validation methods
response_completeness = true
rate_limit_headers = false
empty_response_detection = true  
json_structure_validation = true

[payload_validation.headers] # API-specific field mappings
total_results_field = "search-results.opensearch:totalResults"
rate_limit_remaining = "X-RateLimit-Remaining"
```

### Environment Variables (.env)
```
SCOPUS_API_KEY=your_key_here
SCOPUS_INST_ID=your_token_here
SCIVAL_API_KEY=your_scival_key
WOS_API_KEY=your_wos_key
```

## Testing Strategy

### BDD for Requirements
- **Gherkin Given-When-Then** syntax for class requirement mapping
- Focus on behaviour specification rather than implementation details

### TDD for Implementation  
- **AAA pattern** (Arrange-Act-Assert)
- **Descriptive naming:** `test_[method]_[scenario]_[expected_outcome]()`
- **Constructor dependency injection** to enable mocking

## Data Pipeline Integration

### Downstream Processing
- **First pipeline:** Download and store raw JSON with technical metadata
- **Second pipeline:** Extract and flatten JSON to tabular format
- **Third pipeline:** Apply business logic transformations

### Query Patterns
- **Individual transactional queries** for debugging specific dates/load_ids
- **Range-based extraction** for downstream ETL processing
- **Technical metadata queries** for operational monitoring

## Scalability Constraints

### Current Scale
- ~8000 entities (authors/institutions)
- ~200 pages per entity maximum (50 items per page)
- ~10,000 total publications expected

### Performance Considerations
- **Single-threaded processing** to respect API rate limits
- **Simple resource management** - no connection pooling
- **File-based manifest generation** maintained for compatibility
- **No backwards compatibility** requirements due to active development