-- Silver layer staging audit table
CREATE TABLE IF NOT EXISTS silver_temp_audit (
    load_id STRING PRIMARY KEY,
    data_source STRING,
    ingest_timestamp TIMESTAMP, -- request_timestamp
    query_parameters JSON, -- query_parameter array
    audit_schema_version INTEGER, -- currently static value, will be derived from schema registry module
    audit_schema_hash_value STRING, -- hash of columns, data types and JSON structures
    schema_validation_report JSON,
    schema_validation_status BOOLEAN,
    checksum_status STRING,  -- 'PASSED', 'FAILED', 'ERROR'  
    checksum_timestamp TIMESTAMP,
    checksum_report JSON,
    checksum_value STRING,
    ingestion_time DOUBLE, -- from manifest file, duration_seconds, in processing summary
    validation_status STRING, -- from GE
    validation_errors JSON,
    validation_fails JSON,
    validation_summary STRING, -- from GE
    dq_report JSON, -- JSON report generated from soda dq script
    payload_files JSON, -- from manifest file, provides array with file names and file sizes in KB
    payload_results JSON, -- array containing ingestion details, from manifest file as 'results' array within authors array
);

CREATE TABLE IF NOT EXISTS silver_stg_scopus (
    load_id STRING, -- from metadata array
    scopus_schema_version INTEGER,
    scopus_schema_hash_value STRING,
    scopus_schema_report JSON,
    checksum_status STRING,  -- 'PASSED', 'FAILED', 'ERROR'  
    checksum_timestamp TIMESTAMP,
    checksum_report JSON,
    checksum_value STRING,
    data_source STRING, -- from 'data_source' within metadata array of json payload
    eid STRING PRIMARY KEY, -- eid
    title STRING, -- dc:title
    first_author STRING, -- dc:creator
    journal_name STRING, -- prism:publicationName
    issn STRING, -- prism:issn
    eissn STRING, -- prism:eIssn
    journal_volume INTEGER, -- "prism:volume"
    journal_issue STRING, -- prism:issueIdentifier
    journal_page_range STRING, -- prism:pageRange
    publication_date DATE, -- prism:coverDate
    doi STRING, -- prism:doi
    abstract STRING, -- dc:description - need to constrain by character limit -> what is Pure's limit? Good practise to take Pure's character limit if we will be sending enriched metadata back
    citations INTEGER, -- citedby-count
    affiliations JSON, -- 'affiliation' array containing fields which will map to affiliation_id, affiliation_name, affiliation_city, affiliation_country
    pubmed_id BIGINT, -- pubmed-id
    publishing_format STRING, -- prism:aggregationType
    document_type STRING, -- subtypeDescription
    author_count JSON, -- 'author-count' array containing fields which wll map to exported_author_count (@limit) and total_author_count (@total)
    authors JSON, -- 'author' array containing fields which will map to author_id (authid), author_first_name (given_name), author_surname (surname), author_initials (intials), author_affiliation_id ($ - position 2 in afid array) - transformation note: create author_name field out of first name and surname
    publication_keywords JSON, -- authkeywords - transformation notes: contains | separated values which will need to be transformed to a proper array structure (["", "", ...]]) which will enable native database array operations, better performance on queries, type safety, grouping and aggregation by individual keywords, tag based search and filtering
    funder_source_id STRING, --source-id 
    funder_acronym STRING, -- fund-acr 
    funder_number STRING, -- fund-no
    funder_name STRING, -- fund-sponsor
    open_access_flag BOOLEAN, -- openaccessFlag, returns true/false
    open_access_status JSON -- freetoreadLabel array containing number of entries denoted by $, with each entry being the category of open access
);
