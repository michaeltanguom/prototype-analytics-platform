"""
Silver Audit Metadata Dump ETL Pipeline

A simple ETL script for transferring audit data from scopus_test.db (bronze layer temp dump) to silver_metadata.db which forms the metadata layer

"""

import duckdb
import logging
import sys
import os
from datetime import datetime
from typing import Tuple


class SilverAuditETL:
    """
    Simplified ETL pipeline for transferring audit data between DuckDB databases.
    """
    
    def __init__(self, source_db_path: str, destination_db_path: str = "silver_metadata.db"):
        """
        Initialise the ETL pipeline.
        
        Args:
            source_db_path: Path to the source database file
            destination_db_path: Path to the destination database file
        """
        self.source_db_path = source_db_path
        self.destination_db_path = destination_db_path
        self.logger = self._setup_logging()
        
        # Table names
        self.source_table = "silver_temp_audit"
        self.destination_table = "silver_staging_audit"
        
    def _setup_logging(self) -> logging.Logger:
        """
        Configure logging for the ETL process.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger('silver_audit_etl')
        logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler('silver_audit_etl.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def _get_source_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Create a read-only connection to the source database.
        
        Returns:
            DuckDB connection object
        """
        try:
            conn = duckdb.connect(self.source_db_path, read_only=True)
            self.logger.info(f"Successfully connected to source database: {self.source_db_path}")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to source database: {e}")
            raise
    
    def _get_destination_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Create a read-write connection to the destination database.
        
        Returns:
            DuckDB connection object
        """
        try:
            conn = duckdb.connect(self.destination_db_path)
            self.logger.info(f"Successfully connected to destination database: {self.destination_db_path}")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to destination database: {e}")
            raise
    
    def _create_destination_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Create the destination table with the required schema.
        
        Args:
            conn: DuckDB connection object
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver_staging_audit (
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
            is_reprocessed BOOLEAN DEFAULT FALSE,
            original_processing_timestamp TIMESTAMP,
            reprocessing_reason VARCHAR
        )
        """
        
        try:
            conn.execute(create_table_sql)
            self.logger.info(f"Destination table '{self.destination_table}' created/verified")
        except Exception as e:
            self.logger.error(f"Failed to create destination table: {e}")
            raise
    
    def _extract_data(self, conn: duckdb.DuckDBPyConnection) -> Tuple[list, int]:
        """
        Extract data from the source table.
        
        Args:
            conn: DuckDB connection object
            
        Returns:
            Tuple of (data_rows, row_count)
        """
        try:
            # Get all data from source table
            result = conn.execute(f"SELECT * FROM {self.source_table}").fetchall()
            row_count = len(result)
            
            self.logger.info(f"Successfully extracted {row_count} rows from source table")
            return result, row_count
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from source table: {e}")
            raise
    
    def _transform_data(self, source_data: list) -> list:
        """
        Transform source data by adding new columns for destination table.
        
        Args:
            source_data: Raw data from source table
            
        Returns:
            Transformed data with additional columns
        """
        try:
            current_timestamp = datetime.now()
            transformed_data = []
            
            for row in source_data:
                # Convert row to list and add new columns
                new_row = list(row) + [
                    False,  # is_reprocessed
                    current_timestamp,  # original_processing_timestamp
                    None  # reprocessing_reason
                ]
                transformed_data.append(new_row)
            
            self.logger.info(f"Successfully transformed {len(transformed_data)} rows")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform data: {e}")
            raise
    
    def _load_data(self, conn: duckdb.DuckDBPyConnection, data: list) -> int:
        """
        Load transformed data into the destination table using MERGE pattern.
        Ensures atomic and idempotent operations while preserving audit history.
        
        Args:
            conn: DuckDB connection object
            data: Transformed data to load (list with 17 columns)
            
        Returns:
            Number of new rows inserted
        """
        if not data:
            self.logger.info("No data to load")
            return 0
        
        staging_table = None
        
        try:
            # Create staging table with unique timestamp suffix
            staging_table = f"{self.destination_table}_staging_{int(datetime.now().timestamp())}"
            
            self.logger.info(f"Creating staging table: {staging_table}")
            
            # Create staging table with same structure as destination
            conn.execute(f"""
                CREATE TABLE {staging_table} AS 
                SELECT * FROM {self.destination_table} 
                WHERE 1=0
            """)
            
            # Load data into staging table
            placeholders = ','.join(['?' for _ in range(17)])
            insert_sql = f"INSERT INTO {staging_table} VALUES ({placeholders})"
            
            self.logger.info(f"Loading {len(data)} rows into staging table")
            conn.executemany(insert_sql, data)
            
            # Get count of rows before merge for reporting
            result = conn.execute(f"SELECT COUNT(*) FROM {self.destination_table}").fetchone()
            rows_before = result[0] if result else 0
            
            # Execute MERGE operation - Fixed syntax for DuckDB 1.0.0
            self.logger.info("Executing MERGE operation")
            merge_sql = f"""
                INSERT INTO {self.destination_table}
                SELECT s.*
                FROM {staging_table} s
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {self.destination_table} d 
                    WHERE d.load_id = s.load_id
                )
            """
            
            conn.execute(merge_sql)
            
            # Get count after merge to calculate new rows
            result = conn.execute(f"SELECT COUNT(*) FROM {self.destination_table}").fetchone()
            rows_after = result[0] if result else 0
            rows_inserted = rows_after - rows_before
            
            # Clean up staging table
            conn.execute(f"DROP TABLE {staging_table}")
            staging_table = None
            
            self.logger.info(f"Successfully inserted {rows_inserted} new rows into {self.destination_table}")
            self.logger.info(f"Duplicate rows skipped: {len(data) - rows_inserted}")
            self.logger.info(f"Total rows in destination table: {rows_after}")
            
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Failed to load data into {self.destination_table}: {e}")
            
            # Clean up staging table if it exists
            if staging_table:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
                    self.logger.info(f"Cleaned up staging table: {staging_table}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to clean up staging table {staging_table}: {cleanup_error}")
            
            raise

    def _log_final_results(self, dest_conn: duckdb.DuckDBPyConnection, 
                        source_rows: int, loaded_rows: int) -> None:
        """
        Enhanced logging to show incremental vs total counts.
        
        Args:
            dest_conn: Destination database connection
            source_rows: Number of rows from source
            loaded_rows: Number of NEW rows inserted (not total)
        """
        try:
            # Get total count in destination table
            total_count = dest_conn.execute(f"SELECT COUNT(*) FROM {self.destination_table}").fetchone()[0]
            
            self.logger.info(f"ETL Results Summary:")
            self.logger.info(f"  - Source rows extracted: {source_rows}")
            self.logger.info(f"  - NEW rows inserted: {loaded_rows}")
            self.logger.info(f"  - Duplicate rows skipped: {source_rows - loaded_rows}")
            self.logger.info(f"  - Total rows in destination: {total_count}")
            
            # Get schema info
            schema_info = dest_conn.execute(f"DESCRIBE {self.destination_table}").fetchall()
            self.logger.info(f"  - Destination table columns: {len(schema_info)}")
            
            # Additional audit insights
            if loaded_rows == 0:
                self.logger.info("  - Status: All data was already present (idempotent run)")
            elif loaded_rows == source_rows:
                self.logger.info("  - Status: All source data was new")
            else:
                self.logger.info("  - Status: Partial update - some data was already present")
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve final results: {e}")
    
    def run_etl(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            True if ETL completes successfully, False otherwise
        """
        self.logger.info("Starting Silver Audit ETL pipeline")
        
        try:
            # Check if source file exists
            if not os.path.exists(self.source_db_path):
                self.logger.error(f"Source database file not found: {self.source_db_path}")
                return False
            
            # Connect to databases
            source_conn = self._get_source_connection()
            dest_conn = self._get_destination_connection()
            
            try:
                # Create destination table
                self._create_destination_table(dest_conn)
                
                # Extract data
                source_data, source_row_count = self._extract_data(source_conn)
                
                # Transform data
                transformed_data = self._transform_data(source_data)
                
                # Load data
                loaded_rows = self._load_data(dest_conn, transformed_data)
                
                # Log final results
                self._log_final_results(dest_conn, source_row_count, loaded_rows)
                
                self.logger.info("Silver Audit ETL pipeline completed successfully")
                return True
                
            finally:
                source_conn.close()
                dest_conn.close()
                self.logger.info("Database connections closed")
                
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            return False


def main():
    """
    Main function to run the ETL pipeline.
    """
    if len(sys.argv) != 2:
        print("Usage: python silver_audit_etl.py <source_db_filename>")
        sys.exit(1)
    
    source_db_filename = sys.argv[1]
    
    # Initialise and run ETL
    etl = SilverAuditETL(source_db_filename)
    success = etl.run_etl()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
