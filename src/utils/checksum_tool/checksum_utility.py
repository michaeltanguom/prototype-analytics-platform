"""
Checksum Utility for Data Integrity Validation

A utility script for generating and comparing checksums of DuckDB tables
using DuckDB's native hash() function with automatic type casting.

Usage:
    python checksum_utility.py <database_path> <table_name>
    python checksum_utility.py <database_path> <table_name> --compare <comparison_database> <comparison_table>
    python checksum_utility.py --config <config_file.yml>

"""

import duckdb
import json
import sys
import argparse
import yaml
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChecksumUtility:
    """
    Utility class for generating and comparing table checksums using DuckDB's native hash function.
    """
    
    def __init__(self):
        """Initialise the checksum utility."""
        pass
    
    def _get_connection(self, database_path: str, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        """
        Create a new DuckDB connection.
        
        Args:
            database_path: Path to the database file
            read_only: Whether to open in read-only mode
            
        Returns:
            DuckDB connection object
            
        Raises:
            Exception: If connection fails
        """
        try:
            conn = duckdb.connect(database_path, read_only=read_only)
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to database '{database_path}': {e}")
    
    def _get_table_info(self, conn: duckdb.DuckDBPyConnection, table_name: str, 
                       specified_columns: Optional[list] = None) -> Tuple[int, list]:
        """
        Get basic table information including row count and column names.
        
        Args:
            conn: DuckDB connection object
            table_name: Name of the table to analyse
            specified_columns: Optional list of specific columns to include
            
        Returns:
            Tuple of (row_count, sorted_column_names)
            
        Raises:
            Exception: If table doesn't exist or query fails
        """
        try:
            # Check if table exists
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", 
                [table_name]
            ).fetchone()[0]
            
            if table_exists == 0:
                raise Exception(f"Table '{table_name}' does not exist")
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Get available column names
            columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
            available_columns = [row[0] for row in columns_result]
            
            if specified_columns:
                # Validate that all specified columns exist
                missing_columns = [col for col in specified_columns if col not in available_columns]
                if missing_columns:
                    raise Exception(f"Specified columns not found in table '{table_name}': {missing_columns}")
                
                # Use specified columns in alphabetical order
                column_names = sorted(specified_columns)
            else:
                # Use all columns in alphabetical order
                column_names = sorted(available_columns)
            
            return row_count, column_names
            
        except Exception as e:
            raise Exception(f"Failed to get table info for '{table_name}': {e}")
    
    def _validate_table_state(self, row_count: int, table_name: str) -> None:
        """
        Validate that the table is in a valid state for checksum generation.
        
        Args:
            row_count: Number of rows in the table
            table_name: Name of the table being validated
            
        Raises:
            Exception: If table is in an invalid state
        """
        if row_count == 0:
            raise Exception(f"Critical failure: Table '{table_name}' is empty. Pipeline requires investigation.")
    
    def _check_null_key_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str, 
                               column_names: list, critical_columns: Optional[list] = None) -> None:
        """
        Check for NULL values in key columns (primary key and critical audit fields).
        
        Args:
            conn: DuckDB connection object
            table_name: Name of the table to check
            column_names: List of all column names
            critical_columns: Optional list of critical columns to check for NULLs
            
        Raises:
            Exception: If NULL values found in critical columns
        """
        # Use provided critical columns, or default to load_id
        if critical_columns is None:
            critical_columns = ['load_id']
        
        # Check which critical columns exist in this table
        existing_critical_columns = [col for col in critical_columns if col in column_names]
        
        if not existing_critical_columns:
            return  # No critical columns to check
        
        try:
            for column in existing_critical_columns:
                null_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
                ).fetchone()[0]
                
                if null_count > 0:
                    raise Exception(
                        f"Critical failure: Found {null_count} NULL values in key column '{column}' "
                        f"in table '{table_name}'. Pipeline requires investigation."
                    )
                    
        except Exception as e:
            if "Critical failure" in str(e):
                raise  # Re-raise our custom exceptions
            else:
                raise Exception(f"Failed to check NULL values in table '{table_name}': {e}")
    
    def _generate_table_checksum(self, conn: duckdb.DuckDBPyConnection, table_name: str, 
                                column_names: list) -> str:
        """
        Generate a checksum for the entire table using DuckDB's native hash function.
        
        Args:
            conn: DuckDB connection object
            table_name: Name of the table to checksum
            column_names: List of column names in alphabetical order
            
        Returns:
            Hexadecimal string representation of the table checksum
            
        Raises:
            Exception: If checksum generation fails
        """
        try:
            # Create concatenated string of all columns (cast to string, ordered alphabetically)
            column_casts = [f"CAST({col} AS STRING)" for col in column_names]
            concat_expression = f"CONCAT_WS('|', {', '.join(column_casts)})"
            
            # Generate hash for each row, then aggregate all row hashes
            query = f"""
            SELECT hash(string_agg(row_hash, '')) as table_checksum
            FROM (
                SELECT hash({concat_expression}) as row_hash
                FROM {table_name}
                ORDER BY {column_names[0]}  -- Order by first column for consistency
            )
            """
            
            result = conn.execute(query).fetchone()
            checksum = str(result[0]) if result[0] is not None else "0"
            
            return checksum
            
        except Exception as e:
            raise Exception(f"Failed to generate checksum for table '{table_name}': {e}")
    
    def generate_checksum(self, database_path: str, table_name: str, 
                         columns: Optional[list] = None, critical_columns: Optional[list] = None,
                         output_dir: Optional[str] = None, load_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a checksum for a specified table.
        
        Args:
            database_path: Path to the database file
            table_name: Name of the table to checksum
            columns: Optional list of specific columns to include in checksum
            critical_columns: Optional list of critical columns to check for NULLs
            output_dir: Optional directory to save checksum report
            load_id: Optional load_id for filename generation
            
        Returns:
            Dictionary containing checksum results and metadata
        """
        conn = None
        try:
            # Connect to database
            conn = self._get_connection(database_path, read_only=True)
            
            # Get table information
            row_count, column_names = self._get_table_info(conn, table_name, columns)
            
            # Validate table state
            self._validate_table_state(row_count, table_name)
            
            # Check for NULL values in critical columns
            self._check_null_key_columns(conn, table_name, column_names, critical_columns)
            
            # Generate checksum
            checksum = self._generate_table_checksum(conn, table_name, column_names)
            
            # Return results
            result = {
                "database": database_path,
                "table": table_name,
                "row_count": row_count,
                "column_count": len(column_names),
                "columns": column_names,
                "checksum": checksum,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
            # Save to file if output_dir specified
            if output_dir:
                output_file = self._save_checksum_report(result, load_id, output_dir)
                if output_file:
                    result['report_file'] = str(output_file)
            
            return result
            
        except Exception as e:
            result = {
                "database": database_path,
                "table": table_name,
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "checksum": None,
                "timestamp": datetime.now().isoformat(),
                "status": "error",
                "error_message": str(e)
            }
            
            # Save error result to file if output_dir specified
            if output_dir:
                output_file = self._save_checksum_report(result, load_id, output_dir)
                if output_file:
                    result['report_file'] = str(output_file)
            
            return result
            
        finally:
            if conn:
                conn.close()
    
    def compare_checksums(self, source_db: str, source_table: str, 
                         dest_db: str, dest_table: str, columns: Optional[list] = None,
                         critical_columns: Optional[list] = None, output_dir: Optional[str] = None,
                         load_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare checksums between two tables.
        
        Args:
            source_db: Path to source database
            source_table: Name of source table
            dest_db: Path to destination database  
            dest_table: Name of destination table
            columns: Optional list of specific columns to include in comparison
            critical_columns: Optional list of critical columns to check for NULLs
            output_dir: Optional directory to save comparison report
            load_id: Optional load_id for filename generation
            
        Returns:
            Dictionary containing comparison results
        """
        try:
            # Generate checksums for both tables using the same column set
            source_result = self.generate_checksum(source_db, source_table, columns, critical_columns)
            dest_result = self.generate_checksum(dest_db, dest_table, columns, critical_columns)
            
            # Check if either operation failed
            if source_result["status"] == "error":
                comparison_result = {
                    "comparison_status": "error",
                    "error_message": f"Source checksum failed: {source_result['error_message']}",
                    "source_result": source_result,
                    "destination_result": None,
                    "checksums_match": False,
                    "timestamp": datetime.now().isoformat()
                }
            elif dest_result["status"] == "error":
                comparison_result = {
                    "comparison_status": "error", 
                    "error_message": f"Destination checksum failed: {dest_result['error_message']}",
                    "source_result": source_result,
                    "destination_result": dest_result,
                    "checksums_match": False,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Compare results
                checksums_match = (
                    source_result["checksum"] == dest_result["checksum"] and
                    source_result["row_count"] == dest_result["row_count"]
                )
                
                comparison_result = {
                    "comparison_status": "success",
                    "checksums_match": checksums_match,
                    "source_result": source_result,
                    "destination_result": dest_result,
                    "row_count_match": source_result["row_count"] == dest_result["row_count"],
                    "checksum_match": source_result["checksum"] == dest_result["checksum"],
                    "timestamp": datetime.now().isoformat()
                }
                
                if not checksums_match:
                    comparison_result["integrity_status"] = "FAILED"
                    comparison_result["error_message"] = "Data integrity check failed - checksums do not match"
                else:
                    comparison_result["integrity_status"] = "PASSED"
            
            # Save comparison result to file if output_dir specified
            if output_dir:
                output_file = self._save_checksum_report(comparison_result, load_id, output_dir)
                if output_file:
                    comparison_result['report_file'] = str(output_file)
            
            return comparison_result
            
        except Exception as e:
            error_result = {
                "comparison_status": "error",
                "error_message": f"Comparison failed: {str(e)}",
                "source_result": None,
                "destination_result": None,
                "checksums_match": False,
                "timestamp": datetime.now().isoformat()
            }
            
            # Save error result to file if output_dir specified
            if output_dir:
                output_file = self._save_checksum_report(error_result, load_id, output_dir)
                if output_file:
                    error_result['report_file'] = str(output_file)
            
            return error_result
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Dictionary containing configuration data
            
        Raises:
            Exception: If config file cannot be loaded or is invalid
        """
        try:
            if not os.path.exists(config_path):
                raise Exception(f"Configuration file not found: {config_path}")
                
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                
            # Validate required config sections
            required_sections = ['databases', 'tables', 'checksum_columns']
            for section in required_sections:
                if section not in config:
                    raise Exception(f"Missing required configuration section: {section}")
                    
            return config
            
        except yaml.YAMLError as e:
            raise Exception(f"Invalid YAML in config file: {e}")
        except Exception as e:
            raise Exception(f"Failed to load configuration: {e}")
    
    def run_config_based_comparison(self, config_path: str, output_dir: str = "./checksum_reports") -> Dict[str, Any]:
        """
        Run checksum comparison using configuration file and save results to file.
        
        Args:
            config_path: Path to the YAML configuration file
            output_dir: Directory to save checksum report files
            
        Returns:
            Dictionary containing comparison results
        """
        try:
            # Load configuration
            config = self.load_config(config_path)
            
            # Extract configuration values
            source_db = config['databases']['source']['path']
            dest_db = config['databases']['destination']['path']
            source_table = config['tables']['source_table']
            dest_table = config['tables']['destination_table']
            checksum_columns = config['checksum_columns']
            critical_columns = config.get('critical_columns', ['load_id'])
            
            # Extract load_id from source table before running comparison
            load_id = self._extract_load_id_from_source_table(source_db, source_table)
            
            # Run comparison
            result = self.compare_checksums(
                source_db, source_table, dest_db, dest_table, 
                checksum_columns, critical_columns
            )
            
            # Add config metadata to result
            result['config_metadata'] = config.get('config_metadata', {})
            result['config_file'] = config_path
            
            # Save result to file with extracted load_id
            output_file = self._save_checksum_report(result, load_id, output_dir)
            if output_file:
                result['report_file'] = str(output_file)
            
            return result
            
        except Exception as e:
            return {
                "comparison_status": "error",
                "error_message": f"Config-based comparison failed: {str(e)}",
                "config_file": config_path,
                "timestamp": datetime.now().isoformat()
            }
    
    def _extract_load_id_from_source_table(self, database_path: str, table_name: str) -> Optional[str]:
        """
        Extract load_id directly from the source table using a simple query.
        
        Args:
            database_path: Path to the database file
            table_name: Name of the table to query
            
        Returns:
            load_id string if found, None otherwise
        """
        try:
            logger.info(f"Extracting load_id from table: {table_name}")
            
            conn = self._get_connection(database_path, read_only=True)
            query = f"SELECT DISTINCT load_id FROM {table_name} LIMIT 1"
            result = conn.execute(query).fetchone()
            conn.close()
            
            if result and result[0] is not None:
                load_id = str(result[0])
                logger.info(f"Successfully extracted load_id: {load_id}")
                return load_id
            else:
                logger.warning(f"No load_id found in table {table_name}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to extract load_id from {table_name}: {e}")
            return None
    
    def _save_checksum_report(self, result: Dict[str, Any], load_id: Optional[str], 
                             output_dir: str) -> Optional[Path]:
        """
        Save checksum report to file with specified naming pattern.
        
        Args:
            result: Checksum comparison result
            load_id: Load ID for filename (if available)
            output_dir: Directory to save the file
            
        Returns:
            Path to saved file, or None if save failed
        """
        try:
            from pathlib import Path
            
            # Create output directory if it doesn't exist
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Generate filename with pattern: checksum_*{load_id}*.json
            if load_id:
                filename = f"checksum_{load_id}_{timestamp}.json"
            else:
                filename = f"checksum_report_{timestamp}.json"
            
            file_path = output_path / filename
            
            # Save JSON report
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved checksum report to: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to save checksum report: {e}")
            return None


def main():
    """
    Main function to handle command line interface.
    """
    parser = argparse.ArgumentParser(
        description="Generate and compare checksums for DuckDB tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Generate checksum for a single table:
    python checksum_utility.py scopus_test.db silver_temp_audit
    
  Generate checksum using specific columns only:
    python checksum_utility.py scopus_test.db silver_temp_audit --columns "load_id,ingest_timestamp,payload_files"
    
  Compare checksums between two tables:
    python checksum_utility.py scopus_test.db silver_temp_audit --compare silver_audit.db silver_staging_audit
    
  Compare checksums using specific columns:
    python checksum_utility.py scopus_test.db silver_temp_audit --compare silver_audit.db silver_staging_audit --columns "load_id,query_parameters,ingest_timestamp,ingestion_time,payload_files"
    
  Run using configuration file:
    python checksum_utility.py --config scopus_checksum.yml
    
  Save reports to specific directory:
    python checksum_utility.py --config scopus_checksum.yml --output-dir /path/to/reports
    
  Manual mode with report saving:
    python checksum_utility.py scopus_test.db silver_temp_audit --output-dir ./reports --load-id load123
        """
    )
    
    # Create mutually exclusive group for config vs manual mode
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--config", type=str, 
                           help="Path to YAML configuration file")
    mode_group.add_argument("database", nargs='?', 
                           help="Path to the database file")
    
    parser.add_argument("table", nargs='?', 
                       help="Name of the table to checksum")
    parser.add_argument("--compare", nargs=2, metavar=("DEST_DB", "DEST_TABLE"),
                       help="Compare with destination database and table")
    parser.add_argument("--columns", type=str, 
                       help="Comma-separated list of columns to include in checksum (e.g., 'load_id,ingest_timestamp,payload_files')")
    parser.add_argument("--output-dir", type=str, default="./checksum_reports",
                       help="Directory to save checksum report files (default: ./checksum_reports)")
    parser.add_argument("--load-id", type=str,
                       help="Load ID for filename generation (used in report filename)")
    parser.add_argument("--pretty", action="store_true", 
                       help="Pretty-print JSON output")
    
    args = parser.parse_args()
    
    # Initialise utility
    checksum_util = ChecksumUtility()
    
    try:
        if args.config:
            # Config-based mode
            result = checksum_util.run_config_based_comparison(args.config, args.output_dir)
        else:
            # Manual mode - validate required arguments
            if not args.database or not args.table:
                parser.error("database and table are required when not using --config")
                
            # Parse columns if provided
            columns = None
            if args.columns:
                columns = [col.strip() for col in args.columns.split(',')]
            
            if args.compare:
                # Comparison mode
                dest_db, dest_table = args.compare
                result = checksum_util.compare_checksums(
                    args.database, args.table, dest_db, dest_table, columns, None, args.output_dir, args.load_id
                )
            else:
                # Single checksum mode
                result = checksum_util.generate_checksum(args.database, args.table, columns, None, args.output_dir, args.load_id)
        
        # Output results
        if args.pretty:
            print(json.dumps(result, indent=2))
        else:
            print(json.dumps(result))
        
        # Set exit code based on results
        if result.get("status") == "error" or result.get("comparison_status") == "error":
            sys.exit(1)
        elif result.get("checksums_match") is False:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        error_result = {
            "status": "error",
            "error_message": f"Unexpected error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
        
        if args.pretty:
            print(json.dumps(error_result, indent=2))
        else:
            print(json.dumps(error_result))
        
        sys.exit(1)


if __name__ == "__main__":
    main()
