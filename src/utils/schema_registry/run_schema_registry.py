#!/usr/bin/env python3
"""
Standalone Schema Registry Runner
Location: src/utilities/schema_registry/run_schema_validation.py

Run schema validation independently without Prefect orchestration.
Useful for testing, debugging, and manual validation runs.
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Tuple

# Add the src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from .core import SchemaRegistry
from .config import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SchemaValidationRunner:
    """Standalone runner for schema validation"""
    
    def __init__(self, config_path: str = None, data_db_path: str = None, 
                 schema_registry_db_path: str = None):
        """
        Initialise the standalone runner
        
        Args:
            config_path: Path to schema registry YAML config
            data_db_path: Path to DuckDB data database
            schema_registry_db_path: Path to schema registry database
        """
        self.config_path = config_path or "config/schema_registry_config.yml"
        self.data_db_path = data_db_path or "scopus_test.db"
        
        # Override schema registry DB path if specified
        if schema_registry_db_path:
            self.schema_registry_db_path = schema_registry_db_path
        else:
            # Use config default or fallback
            config_manager = ConfigManager(self.config_path)
            self.schema_registry_db_path = config_manager.config.database_path
        
        logger.info(f"Initialising schema registry runner")
        logger.info(f"Config: {self.config_path}")
        logger.info(f"Data DB: {self.data_db_path}")
        logger.info(f"Schema Registry DB: {self.schema_registry_db_path}")
        
        self.schema_registry = SchemaRegistry(
            db_path=self.schema_registry_db_path,
            data_db_path=self.data_db_path,
            config_path=self.config_path
        )
    
    def validate_table(self, data_source: str, table_name: str, 
                      load_id: str = None, save_report: bool = True,
                      print_summary: bool = True) -> Tuple[str, str]:
        """
        Validate a single table's schema
        
        Args:
            data_source: Name of the data source (e.g., 'scopus')
            table_name: Name of the table to validate
            load_id: Optional load ID for tracking
            save_report: Whether to save JSON report
            print_summary: Whether to print human-readable summary
            
        Returns:
            Tuple of (overall_status, report_file_path)
        """
        try:
            logger.info(f"🔍 Starting schema validation for {data_source}.{table_name}")
            
            # Run validation
            validation_report, report_file_path = self.schema_registry.validate_and_report(
                data_source=data_source,
                table_name=table_name,
                load_id=load_id,
                save_report=save_report
            )
            
            overall_status = validation_report['validation_results']['overall_status']
            
            if print_summary:
                summary = self.schema_registry.create_summary_report(validation_report)
                print(summary)
            
            logger.info(f"Schema validation completed: {overall_status}")
            
            if save_report and report_file_path:
                logger.info(f"📄 Report saved to: {report_file_path}")
            
            return overall_status, report_file_path or ""
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            raise
    
    def register_initial_schema(self, data_source: str, table_name: str,
                              save_report: bool = True) -> Tuple[str, str]:
        """Register a table's schema for the first time"""
        logger.info(f"📝 Registering initial schema for {data_source}.{table_name}")
        
        try:
            validation_report = self.schema_registry.register_initial_schema(data_source, table_name)
            
            report_file_path = None
            if save_report:
                report_file_path = self.schema_registry.save_validation_report(validation_report)
            
            logger.info(f"Initial schema registered successfully")
            return "registered", report_file_path or ""
            
        except Exception as e:
            logger.error(f"Initial schema registration failed: {e}")
            raise
    
    def get_schema_history(self, data_source: str, table_name: str, limit: int = 10) -> None:
        """Display schema history for a table"""
        logger.info(f"Retrieving schema history for {data_source}.{table_name}")
        
        try:
            history = self.schema_registry.get_schema_history(data_source, table_name, limit)
            
            if not history:
                print(f"No schema history found for {data_source}.{table_name}")
                return
            
            print(f"\nSCHEMA HISTORY: {data_source}.{table_name}")
            print("=" * 60)
            
            for i, version in enumerate(history):
                print(f"\n{i+1}. Version: {version['schema_version']}")
                print(f"   Schema ID: {version['schema_id']}")
                print(f"   Detected: {version['detected_at']}")
                print(f"   Hash: {version['schema_hash_value'][:16]}...")
                print(f"   Columns: {len(version['schema_report'].get('columns', []))}")
                print(f"   Current: {'Yes' if version['is_current'] else 'No'}")
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema history: {e}")
            raise
    
    def get_recent_changes(self, data_source: str = None, limit: int = 20) -> None:
        """Display recent schema changes"""
        logger.info(f"Retrieving recent schema changes")
        
        try:
            changes = self.schema_registry.get_recent_changes(data_source, limit)
            
            if not changes:
                print("No recent schema changes found")
                return
            
            print(f"\nRECENT SCHEMA CHANGES")
            if data_source:
                print(f"Data Source: {data_source}")
            print("=" * 60)
            
            for i, change in enumerate(changes):
                print(f"\n{i+1}. {change.get('data_source', 'Unknown')}.{change.get('table_name', 'Unknown')}")
                print(f"   Change ID: {change['change_id']}")
                print(f"   Type: {change['change_type']}")
                print(f"   Compatibility: {change['compatibility_level']}")
                print(f"   Action: {change['auto_action']}")
                print(f"   Detected: {change['detected_at']}")
                if change.get('load_id'):
                    print(f"   Load ID: {change['load_id']}")
            
        except Exception as e:
            logger.error(f"Failed to retrieve recent changes: {e}")
            raise
    
    def validate_config(self) -> None:
        """Validate schema registry configuration and rules"""
        logger.info("🔧 Validating schema registry configuration")
        
        try:
            # Validate compatibility rules
            rules_validation = self.schema_registry.validate_compatibility_rules()
            
            print("\n🔧 SCHEMA REGISTRY CONFIGURATION VALIDATION")
            print("=" * 60)
            print(f"Total Rules: {rules_validation['total_rules']}")
            print(f"Active Rules: {rules_validation['active_rules']}")
            
            print("\nRules by Type:")
            for rule_type, count in rules_validation['rules_by_type'].items():
                print(f"  {rule_type}: {count}")
            
            if rules_validation['potential_issues']:
                print("\nPotential Issues:")
                for issue in rules_validation['potential_issues']:
                    print(f"  - {issue}")
            else:
                print("\nNo configuration issues detected")
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise


def main():
    """Main CLI interface for standalone schema validation"""
    parser = argparse.ArgumentParser(
        description="Standalone Schema Registry Validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate a table
  python run_schema_validation.py validate scopus silver_stg_scopus --load-id LOAD123

  # Register initial schema
  python run_schema_validation.py register scopus silver_stg_scopus

  # View schema history
  python run_schema_validation.py history scopus silver_stg_scopus

  # View recent changes
  python run_schema_validation.py changes --data-source scopus

  # Validate configuration
  python run_schema_validation.py config
        """
    )
    
    parser.add_argument(
        'command',
        choices=['validate', 'register', 'history', 'changes', 'config'],
        help='Command to execute'
    )
    
    parser.add_argument(
        'data_source',
        nargs='?',
        help='Data source name (e.g., scopus)'
    )
    
    parser.add_argument(
        'table_name',
        nargs='?',
        help='Table name to validate (e.g., silver_stg_scopus)'
    )
    
    parser.add_argument(
        '--load-id',
        help='Load ID for tracking'
    )
    
    parser.add_argument(
        '--config',
        help='Path to schema registry config YAML file'
    )
    
    parser.add_argument(
        '--data-db',
        help='Path to DuckDB data database'
    )
    
    parser.add_argument(
        '--schema-db',
        help='Path to schema registry database'
    )
    
    parser.add_argument(
        '--no-report',
        action='store_true',
        help='Skip saving JSON report'
    )
    
    parser.add_argument(
        '--no-summary',
        action='store_true',
        help='Skip printing summary'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Limit for history/changes queries'
    )
    
    args = parser.parse_args()
    
    # Validate required arguments
    if args.command in ['validate', 'register', 'history'] and not (args.data_source and args.table_name):
        parser.error(f"Command '{args.command}' requires data_source and table_name")
    
    try:
        # Initialise runner
        runner = SchemaValidationRunner(
            config_path=args.config,
            data_db_path=args.data_db,
            schema_registry_db_path=args.schema_db
        )
        
        # Execute command
        if args.command == 'validate':
            status, report_path = runner.validate_table(
                data_source=args.data_source,
                table_name=args.table_name,
                load_id=args.load_id,
                save_report=not args.no_report,
                print_summary=not args.no_summary
            )
            
            print(f"\nValidation Status: {status}")
            if report_path:
                print(f"📄 Report: {report_path}")
            
            # Exit with appropriate code
            exit_codes = {'safe': 0, 'warning': 1, 'breaking': 2}
            sys.exit(exit_codes.get(status, 3))
        
        elif args.command == 'register':
            status, report_path = runner.register_initial_schema(
                data_source=args.data_source,
                table_name=args.table_name,
                save_report=not args.no_report
            )
            print(f"\nSchema registration completed")
            if report_path:
                print(f"📄 Report: {report_path}")
        
        elif args.command == 'history':
            runner.get_schema_history(args.data_source, args.table_name, args.limit)
        
        elif args.command == 'changes':
            runner.get_recent_changes(args.data_source, args.limit)
        
        elif args.command == 'config':
            runner.validate_config()
        
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()