# BDD Requirements - Gherkin Syntax for Generic API Adapter

**Behaviour Driven Development (BDD) Structure:**
- **Given:** Input conditions (the context and preconditions for the scenario)
- **When:** Actions performed (the specific action or method being tested)  
- **Then:** Expected outcomes (the observable results and assertions)

BDD has been used to make it clear what the responsibility and behaviour of each component is, ensuring single responsibility and focused functionality.

## ConfigLoader - TOML configuration and environment variable validation

### Feature: Load TOML Configuration
**As a** data engineer  
**I want** to load API configuration from TOML files  
**So that** I can configure different data sources without changing code

#### Scenario: Loading valid TOML configuration
**Given:** a valid TOML configuration file exists  
**When:** I call load_toml_config with the file path  
**Then:** it should return an APIConfig object with all required fields populated

#### Scenario: Missing required TOML fields
**Given:** a TOML file with missing required sections  
**When:** I call load_toml_config with the file path  
**Then:** it should raise a ConfigurationError with details of missing fields

#### Scenario: Invalid TOML syntax
**Given:** a TOML file with invalid syntax  
**When:** I call load_toml_config with the file path  
**Then:** it should raise a ParseError with line number information

### Feature: Validate Environment Variables
**As a** data engineer  
**I want** to validate that required environment variables are set  
**So that** authentication failures are caught at startup

#### Scenario: All required environment variables present
**Given:** an APIConfig specifying required environment variables  
**And:** all environment variables are set in the .env file  
**When:** I call validate_environment_variables  
**Then:** it should return True

#### Scenario: Missing environment variables
**Given:** an APIConfig requiring SCOPUS_API_KEY  
**And:** SCOPUS_API_KEY is not set in environment  
**When:** I call validate_environment_variables  
**Then:** it should raise an EnvironmentError listing missing variables

---

## DatabaseManager - Connection management, transactions, and UPSERT operations

### Feature: Database Connection Management
**As a** system component  
**I want** to manage database connections reliably  
**So that** data operations are consistent and resources are cleaned up

#### Scenario: Successful database connection
**Given:** a valid database path  
**When:** I call create_connection  
**Then:** it should return a working database connection

#### Scenario: Invalid database path
**Given:** an invalid database path  
**When:** I call create_connection  
**Then:** it should raise a DatabaseConnectionError

#### Scenario: Connection cleanup
**Given:** an active database connection  
**When:** I call close_connection  
**Then:** it should close the connection and release resources

### Feature: Transactional Operations
**As a** data persistence layer  
**I want** to execute operations within transactions  
**So that** data consistency is maintained on failures

#### Scenario: Successful transaction
**Given:** a valid SQL query and parameters  
**When:** I call execute_with_transaction  
**Then:** it should execute the query and commit the transaction

#### Scenario: Failed transaction rollback
**Given:** an invalid SQL query  
**When:** I call execute_with_transaction  
**Then:** it should rollback the transaction and raise the original error

### Feature: UPSERT Operations for Idempotency
**As a** data ingestion system  
**I want** to use UPSERT operations  
**So that** reprocessing the same data doesn't create duplicates

#### Scenario: Insert new API response
**Given:** response data that doesn't exist in the database  
**When:** I call upsert_api_response  
**Then:** it should insert the new record

#### Scenario: Update existing API response
**Given:** response data with matching primary key  
**When:** I call upsert_api_response  
**Then:** it should update the existing record with new data

---

## HTTPClient - Authentication, retries, and rate limiting

### Feature: API Authentication
**As an** API client  
**I want** to authenticate with different APIs  
**So that** I can access protected resources

#### Scenario: API key authentication
**Given:** credentials containing an API key  
**When:** I call authenticate  
**Then:** it should configure headers with the API key

#### Scenario: Bearer token authentication
**Given:** credentials containing a bearer token  
**When:** I call authenticate  
**Then:** it should set Authorization header with Bearer token

### Feature: HTTP Requests with Retry Logic
**As an** API client  
**I want** to make HTTP requests with automatic retries  
**So that** temporary failures don't stop processing

#### Scenario: Successful API request
**Given:** a valid API request  
**When:** I call make_request  
**Then:** it should return a successful APIResponse

#### Scenario: Retry on temporary failure
**Given:** an API request that fails with HTTP 503  
**When:** I call make_request  
**Then:** it should retry the request with exponential backoff

#### Scenario: Permanent failure after max retries
**Given:** an API request that consistently fails  
**When:** I call make_request and exceed max retry attempts  
**Then:** it should raise a PermanentAPIError

### Feature: Rate Limiting
**As an** API client  
**I want** to respect rate limits  
**So that** I don't exceed API quotas

#### Scenario: Apply rate limit delay
**Given:** a configured requests-per-second limit  
**When:** I call apply_rate_limit  
**Then:** it should wait the appropriate time before allowing next request

#### Scenario: No delay needed
**Given:** sufficient time has passed since last request  
**When:** I call apply_rate_limit  
**Then:** it should return immediately without delay

---

## StateManager - State persistence and recovery

### Feature: Load Processing State
**As a** recovery system  
**I want** to load existing processing state  
**So that** I can resume from where processing stopped

#### Scenario: Load existing state
**Given:** a processing state exists for entity and load_id  
**When:** I call load_state  
**Then:** it should return the ProcessingState object

#### Scenario: No existing state
**Given:** no state exists for entity and load_id  
**When:** I call load_state  
**Then:** it should return None

### Feature: Save Processing State
**As a** recovery system  
**I want** to save processing state after each page  
**So that** recovery can resume from the correct point

#### Scenario: Save new state
**Given:** a ProcessingState object for a new entity  
**When:** I call save_state  
**Then:** it should insert the state into the database

#### Scenario: Update existing state
**Given:** a ProcessingState object for an existing entity  
**When:** I call save_state  
**Then:** it should update the existing state record

### Feature: Identify Incomplete Entities
**As a** processing orchestrator  
**I want** to identify which entities need processing  
**So that** I only process incomplete work

#### Scenario: All entities complete
**Given:** all entities in the input list are marked complete  
**When:** I call get_incomplete_entities  
**Then:** it should return an empty list

#### Scenario: Some entities incomplete
**Given:** some entities are not marked complete  
**When:** I call get_incomplete_entities  
**Then:** it should return the list of incomplete entity IDs

---

## PayloadValidator - Configurable response validation

### Feature: Configurable Response Validation
**As a** data quality system  
**I want** to validate API responses based on configuration  
**So that** different APIs can have appropriate validation

#### Scenario: All validations enabled and pass
**Given:** a valid API response  
**And:** all validation methods enabled in configuration  
**When:** I call validate_response  
**Then:** it should return True

#### Scenario: Response completeness validation fails
**Given:** an API response with mismatched totalResults  
**And:** response completeness validation is enabled  
**When:** I call validate_response_completeness  
**Then:** it should return False

#### Scenario: Rate limit validation with missing headers
**Given:** an API response without rate limit headers  
**And:** rate limit validation is enabled  
**When:** I call validate_rate_limit_headers  
**Then:** it should return False

#### Scenario: Empty response detection
**Given:** an API response with zero results  
**When:** I call detect_empty_response  
**Then:** it should return True

#### Scenario: Invalid JSON structure
**Given:** an API response with missing required top-level keys  
**When:** I call validate_json_structure  
**Then:** it should return False

---

## FileHasher - Content deduplication

### Feature: Content Hash Generation
**As a** deduplication system  
**I want** to generate consistent hashes for content  
**So that** duplicate data can be identified

#### Scenario: Generate hash for identical content
**Given:** two identical dictionaries  
**When:** I call generate_content_hash on both  
**Then:** both should return the same hash value

#### Scenario: Generate different hashes for different content
**Given:** two different dictionaries  
**When:** I call generate_content_hash on both  
**Then:** they should return different hash values

#### Scenario: Order-independent hashing
**Given:** two dictionaries with same data but different key order  
**When:** I call generate_content_hash on both  
**Then:** both should return the same hash value

---

## PaginationStrategy - API Pagination handling via Factory pattern

### Feature: Offset-Based Pagination
**As a** pagination handler  
**I want** to calculate correct offset parameters  
**So that** all pages of data are retrieved

#### Scenario: First page parameters
**Given:** a request for the first page  
**When:** I call get_next_page_params with page_num=1  
**Then:** it should return start=0 and count=items_per_page

#### Scenario: Subsequent page parameters
**Given:** a request for the third page with 25 items per page  
**When:** I call get_next_page_params with page_num=3  
**Then:** it should return start=50 and count=25

#### Scenario: No more pages available
**Given:** current offset exceeds total results  
**When:** I call get_next_page_params  
**Then:** it should return None

### Feature: Total Results Extraction
**As a** pagination handler  
**I want** to extract total result count from API response  
**So that** I know when to stop pagination

#### Scenario: Extract total from Scopus response
**Given:** a Scopus API response with opensearch:totalResults  
**When:** I call extract_total_results  
**Then:** it should return the integer value of totalResults

#### Scenario: Missing total results field
**Given:** an API response without the expected total results field  
**When:** I call extract_total_results  
**Then:** it should return None

---

## CacheManager - Development caching with lock management

### Feature: Response Caching
**As a** development tool  
**I want** to cache API responses  
**So that** repeated requests during development don't hit API limits

#### Scenario: Cache miss - store new response
**Given:** a cache key that doesn't exist  
**When:** I call get_cached_response  
**Then:** it should return None

#### Scenario: Cache hit - return stored response
**Given:** a cached API response  
**When:** I call get_cached_response with the same key  
**Then:** it should return the cached APIResponse

#### Scenario: Store response in cache
**Given:** an APIResponse and cache key  
**When:** I call store_response  
**Then:** the response should be stored and retrievable

### Feature: Cache Lock Management
**As a** cache system  
**I want** to prevent concurrent access to cache files  
**So that** cache corruption is avoided

#### Scenario: Acquire available lock
**Given:** no existing lock file  
**When:** I call acquire_lock  
**Then:** it should create lock file and return True

#### Scenario: Lock already exists
**Given:** an existing lock file within timeout period  
**When:** I call acquire_lock  
**Then:** it should return False

#### Scenario: Stale lock cleanup
**Given:** a lock file older than timeout period  
**When:** I call acquire_lock  
**Then:** it should remove stale lock and create new lock

---

## RecoveryManager - Failed page identification and recovery

### Feature: Failed Page Identification
**As a** recovery system  
**I want** to identify which pages failed during processing  
**So that** only failed pages are reprocessed

#### Scenario: Identify pages with errors
**Given:** an entity with processing errors in state  
**When:** I call identify_failed_pages  
**Then:** it should return a list of page numbers that encountered errors

#### Scenario: No failed pages
**Given:** an entity with successful processing state  
**When:** I call identify_failed_pages  
**Then:** it should return an empty list

### Feature: File Validation
**As a** recovery system  
**I want** to validate existing downloaded files  
**So that** corrupted files are detected and reprocessed

#### Scenario: All files valid
**Given:** an entity with all files present and valid hashes  
**When:** I call validate_existing_files  
**Then:** it should return True

#### Scenario: Missing or corrupted files
**Given:** an entity with missing or invalid hash files  
**When:** I call validate_existing_files  
**Then:** it should return False

### Feature: Page-Level Recovery
**As a** recovery system  
**I want** to restart processing from a specific page  
**So that** partial failures don't require complete reprocessing

#### Scenario: Restart from failed page
**Given:** an entity that failed at page 5  
**When:** I call restart_from_page with page_num=5  
**Then:** it should reset processing state to begin at page 5

#### Scenario: Clear subsequent invalid data
**Given:** an entity restarting from page 3  
**When:** I call restart_from_page with page_num=3  
**Then:** it should remove any data from pages 3 and above

---

## RateLimitTracker - API usage tracking and weekly limit checking

### Feature: API Usage Tracking
**As a** rate limiting system  
**I want** to track API requests across sessions  
**So that** weekly limits are respected

#### Scenario: Track new request
**Given:** a successful API request  
**When:** I call track_request with data_source and timestamp  
**Then:** it should record the request in the usage log

#### Scenario: Multiple requests same source
**Given:** multiple requests to the same data source  
**When:** I call track_request multiple times  
**Then:** it should accumulate the request count

### Feature: Weekly Limit Checking
**As a** rate limiting system  
**I want** to check current usage against weekly limits  
**So that** processing stops before exceeding quotas

#### Scenario: Within weekly limit
**Given:** current usage is 100 requests and weekly limit is 500  
**When:** I call check_weekly_limit  
**Then:** it should return True

#### Scenario: Exceeding weekly limit
**Given:** current usage is 450 requests and weekly limit is 500  
**And:** 60 more requests are needed  
**When:** I call check_weekly_limit  
**Then:** it should return False

### Feature: Current Usage Calculation
**As a** monitoring system  
**I want** to calculate current weekly usage  
**So that** remaining quota can be determined

#### Scenario: Calculate current week usage
**Given:** multiple requests in the current week  
**When:** I call get_current_usage  
**Then:** it should return the sum of requests in the past 7 days

#### Scenario: Usage from previous week excluded
**Given:** requests from 8 days ago  
**When:** I call get_current_usage  
**Then:** it should not include requests older than 7 days

---

## ManifestGenerator - API processing summary generation

### Feature: Processing Summary Generation
**As a** reporting system  
**I want** to generate processing summaries  
**So that** ingestion results can be tracked and audited

#### Scenario: Generate complete manifest
**Given:** processing results from multiple entities  
**When:** I call generate_manifest_data  
**Then:** it should return a complete manifest with summary statistics

#### Scenario: Empty processing results
**Given:** no processing results  
**When:** I call generate_manifest_data  
**Then:** it should return a manifest with zero counts

### Feature: Summary Statistics Calculation
**As a** reporting system  
**I want** to calculate summary statistics  
**So that** processing performance can be evaluated

#### Scenario: Calculate success rates
**Given:** processing results with successful and failed entities  
**When:** I call calculate_summary_statistics  
**Then:** it should return success rate, total entities, and total responses

#### Scenario: Calculate processing duration
**Given:** processing results with start and end timestamps  
**When:** I call calculate_summary_statistics  
**Then:** it should return total processing duration and average per entity

---

## APIOrchestrator - High level coordination and entity processing

### Feature: Entity List Loading
**As a** processing coordinator  
**I want** to load entity lists from input files  
**So that** the system knows which entities to process

#### Scenario: Load valid entity file
**Given:** a text file containing entity IDs, one per line  
**When:** I call load_entities_from_file  
**Then:** it should return a list of entity IDs

#### Scenario: Handle missing entity file
**Given:** a non-existent entity file path  
**When:** I call load_entities_from_file  
**Then:** it should raise a FileNotFoundError

### Feature: Load ID Generation
**As a** processing coordinator  
**I want** to generate unique load identifiers  
**So that** each processing run can be tracked separately

#### Scenario: Generate unique load ID
**Given:** a data source name  
**When:** I call generate_load_id  
**Then:** it should return a unique identifier combining source and timestamp

#### Scenario: Different sources generate different IDs
**Given:** different data source names  
**When:** I call generate_load_id for each  
**Then:** each should return a unique identifier

### Feature: Single Entity Processing
**As a** processing coordinator  
**I want** to process individual entities with full pagination  
**So that** each entity's data is completely retrieved

#### Scenario: Process complete entity successfully
**Given:** a valid entity ID and load ID  
**When:** I call process_single_entity  
**Then:** it should retrieve all pages and mark entity as complete

#### Scenario: Handle entity processing failure
**Given:** an entity that fails during processing  
**When:** I call process_single_entity  
**Then:** it should log the error and preserve partial progress

### Feature: Partial Failure Handling
**As a** processing coordinator  
**I want** to continue processing other entities when one fails  
**So that** single failures don't stop the entire batch

#### Scenario: Continue after entity failure
**Given:** an entity that throws an exception  
**When:** I call handle_partial_failure  
**Then:** it should log the error and allow processing to continue

#### Scenario: Record failure for recovery
**Given:** a failed entity  
**When:** I call handle_partial_failure  
**Then:** it should record the failure in state management for later recovery