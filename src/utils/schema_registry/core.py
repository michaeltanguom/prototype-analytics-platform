"""
Core schema registry implementation
Location: src/utilities/schema_registry/core.py
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path

from .detector import SchemaDetector
from .comparator import SchemaComparator
from .rules_engine import CompatibilityRulesEngine
from .database import SchemaRegistryDB
from .config import ConfigManager

logger = logging.getLogger(__name__)


class SchemaRegistryException(Exception):
    """Custom exception for schema registry operations"""
    pass


class SchemaRegistry:
    """Main schema registry class coordinating all operations"""
    
    def __init__(self, db_path: str, data_db_path: str, config_path: str = None):
        """
        Initialise schema registry
        
        Args:
            db_path: Path to schema registry database
            data_db_path: Path to data database (DuckDB)
            config_path: Path to configuration file
        """
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.config
        
        # Override database path if specified
        if db_path:
            self.config.database_path = db_path
        
        # Initialise components
        self.schema_db = SchemaRegistryDB(self.config.database_path)
        self.detector = SchemaDetector(data_db_path)
        self.comparator = SchemaComparator()
        self.rules_engine = CompatibilityRulesEngine(self.schema_db)
        
        logger.info(f"🔧 Schema registry initialised with database: {self.config.database_path}")
    
    def validate_schema(self, data_source: str, table_name: str, load_id: str = None) -> Dict[str, Any]:
        """
        Main validation method - detects current schema and validates against registry
        
        Args:
            data_source: Name of the data source (e.g., 'scopus')
            table_name: Name of the table to validate
            load_id: Optional load ID for tracking (if None, will attempt to extract from table)
            
        Returns:
            Validation results with compatibility assessment
        """
        logger.info(f"🔍 Starting schema validation for {data_source}.{table_name}")
        
        try:
            # Step 1: Extract load_id if not provided
            if load_id is None:
                extracted_load_id = self.detector.extract_load_id(table_name)
                if extracted_load_id:
                    load_id = extracted_load_id
                    logger.info(f"🔗 Using extracted load_id: {load_id}")
                else:
                    logger.warning("⚠️ No load_id provided or extracted - using timestamp-based ID")
                    load_id = f"AUTO_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Step 2: Detect current schema
            current_schema = self._detect_current_schema(data_source, table_name)
            
            # Step 3: Get previous schema from registry
            previous_schema = self.schema_db.get_current_schema(data_source, table_name)
            
            # Step 4: Compare schemas and detect changes
            if previous_schema:
                changes = self.comparator.compare_schemas(
                    previous_schema['schema_report'], 
                    current_schema
                )
                
                # Check if schema hash is identical (no actual changes)
                current_hash = current_schema['schema_hash']
                previous_hash = previous_schema['schema_hash_value']
                
                if current_hash == previous_hash:
                    logger.info("✅ Schema unchanged - same hash detected")
                    changes['has_changes'] = False
                    changes['detailed_changes'] = []
                
            else:
                # First time seeing this schema
                changes = {'has_changes': False, 'detailed_changes': []}
                logger.info("📋 No previous schema found - treating as initial registration")
            
            # Step 5: Evaluate changes against compatibility rules
            if changes.get('has_changes', False):
                evaluation = self.rules_engine.evaluate_changes(changes)
                logger.info(f"⚖️ Schema evaluation: {evaluation['overall_compatibility']}")
            else:
                evaluation = {
                    'overall_compatibility': 'safe',
                    'overall_action': 'proceed',
                    'evaluated_changes': [],
                    'rule_matches': []
                }
            
            # Step 6: Determine schema version and ID
            if previous_schema and not changes.get('has_changes', False):
                # No changes - reuse existing version
                schema_id = previous_schema['schema_id']
                current_version = previous_schema['schema_version']
                logger.info(f"📋 No schema changes detected - keeping version {current_version}")
            else:
                # Changes detected or first registration - create new version
                new_version = self._determine_version(
                    previous_schema.get('schema_version') if previous_schema else None,
                    evaluation['overall_compatibility']
                )
                
                # Save new schema version
                schema_id = self._save_schema_version(
                    data_source, table_name, new_version, current_schema
                )
                current_version = new_version
                logger.info(f"📋 Created new schema version: {current_version}")
            
            # Step 7: Log changes if any
            change_id = None
            if changes.get('has_changes', False):
                change_id = self._log_schema_changes(
                    previous_schema, schema_id, changes, evaluation, load_id
                )
            
            # Step 8: Create validation report
            validation_report = self._create_validation_report(
                data_source, table_name, schema_id, changes, evaluation, 
                previous_schema, current_schema, load_id, change_id
            )
            
            logger.info(f"✅ Schema validation completed: {evaluation['overall_compatibility']}")
            return validation_report
            
        except Exception as e:
            logger.error(f"❌ Schema validation failed: {e}")
            raise SchemaRegistryException(f"Schema validation failed: {e}")
    
    def _detect_current_schema(self, data_source: str, table_name: str) -> Dict[str, Any]:
        """Detect the current schema of a table"""
        if not self.detector.validate_table_exists(table_name):
            raise SchemaRegistryException(f"Table {table_name} does not exist")
        
        return self.detector.extract_table_schema(table_name)
    
    def _determine_version(self, previous_version: str, compatibility_level: str) -> str:
        """Determine the new version number based on compatibility level"""
        if not previous_version:
            return "1.0.0"  # Initial version
        
        if not self.config.enable_auto_versioning:
            # Manual versioning - increment patch
            return self._increment_version(previous_version, "patch")
        
        # Auto versioning based on compatibility rules
        version_type = self.config.version_increment_rules.get(compatibility_level, "patch")
        return self._increment_version(previous_version, version_type)
    
    def _increment_version(self, version: str, increment_type: str) -> str:
        """Increment version number according to semantic versioning"""
        try:
            major, minor, patch = map(int, version.split('.'))
            
            if increment_type == "major":
                return f"{major + 1}.0.0"
            elif increment_type == "minor":
                return f"{major}.{minor + 1}.0"
            else:  # patch
                return f"{major}.{minor}.{patch + 1}"
                
        except ValueError:
            logger.warning(f"Invalid version format: {version}, defaulting to 1.0.0")
            return "1.0.0"
    
    def _save_schema_version(self, data_source: str, table_name: str, 
                           version: str, schema_report: Dict[str, Any]) -> str:
        """Save new schema version to registry"""
        schema_data = {
            'data_source': data_source,
            'table_name': table_name,
            'schema_version': version,
            'schema_hash_value': schema_report['schema_hash'],
            'schema_report': schema_report,
            'detection_method': 'auto',
            'metadata': {
                'validation_timestamp': datetime.now().isoformat(),
                'row_count': schema_report.get('metadata', {}).get('row_count', 0)
            }
        }
        
        return self.schema_db.save_schema_version(schema_data)
    
    def _log_schema_changes(self, previous_schema: Dict[str, Any], new_schema_id: str,
                          changes: Dict[str, Any], evaluation: Dict[str, Any], 
                          load_id: str = None) -> str:
        """Log schema changes in evolution log"""
        change_data = {
            'schema_id_from': previous_schema['schema_id'] if previous_schema else None,
            'schema_id_to': new_schema_id,
            'change_type': 'schema_evolution',
            'change_details': {
                'changes': changes,
                'evaluation': evaluation,
                'change_count': len(changes.get('detailed_changes', []))
            },
            'compatibility_level': evaluation['overall_compatibility'],
            'auto_action': evaluation['overall_action'],
            'load_id': load_id,
            'validation_status': 'completed'
        }
        
        return self.schema_db.log_schema_change(change_data)
    
    def _create_validation_report(self, data_source: str, table_name: str, schema_id: str,
                                changes: Dict[str, Any], evaluation: Dict[str, Any],
                                previous_schema: Dict[str, Any], current_schema: Dict[str, Any],
                                load_id: str, change_id: str) -> Dict[str, Any]:
        """Create comprehensive validation report"""
        notification_config = self.config_manager.get_notification_config(
            evaluation['overall_compatibility']
        )
        
        return {
            'metadata': {
                'validation_id': f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'validation_timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'table_name': table_name,
                'load_id': load_id,
                'schema_registry_version': "1.0.0"
            },
            'schema_info': {
                'schema_id': schema_id,
                'schema_version': schema_id.split('_')[-1],  # Extract version from ID
                'schema_hash': current_schema['schema_hash'],
                'column_count': len(current_schema.get('columns', [])),
                'table_row_count': current_schema.get('metadata', {}).get('row_count', 0)
            },
            'validation_results': {
                'overall_status': evaluation['overall_compatibility'],
                'recommended_action': evaluation['overall_action'],
                'has_changes': changes.get('has_changes', False),
                'change_count': len(changes.get('detailed_changes', [])),
                'breaking_changes': len([c for c in evaluation.get('evaluated_changes', []) 
                                       if c.get('compatibility_level') == 'breaking']),
                'should_halt_pipeline': notification_config.halt_pipeline
            },
            'changes': changes,
            'evaluation': evaluation,
            'change_log': {
                'change_id': change_id,
                'previous_schema_id': previous_schema['schema_id'] if previous_schema else None,
                'current_schema_id': schema_id
            },
            'notifications': {
                'method': notification_config.method,
                'level': notification_config.level,
                'email_required': notification_config.email,
                'halt_pipeline': notification_config.halt_pipeline
            },
            'audit_data': {
                'audit_schema_version': schema_id.split('_')[-1],
                'audit_schema_hash_value': current_schema['schema_hash'],
                'schema_validation_status': evaluation['overall_compatibility'],
                'schema_validation_timestamp': datetime.now().isoformat()
            }
        }
    
    def save_validation_report(self, validation_report: Dict[str, Any]) -> str:
        """Save validation report to JSON file with specified naming pattern"""
        reports_dir = Path(self.config.reports_directory)
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        metadata = validation_report['metadata']
        load_id = metadata.get('load_id', 'UNKNOWN')
        table_name = metadata.get('table_name', 'UNKNOWN')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create filename: schema_validation_{load_id}_{table_name}_{timestamp}.json
        filename = f"schema_validation_{load_id}_{table_name}_{timestamp}.json"
        file_path = reports_dir / filename
        
        # Handle multiple reports at same time
        counter = 1
        while file_path.exists():
            filename = f"schema_validation_{load_id}_{table_name}_{timestamp}_{counter:02d}.json"
            file_path = reports_dir / filename
            counter += 1
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(validation_report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📄 Schema validation report saved: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to save validation report: {e}")
            raise
    
    def extract_audit_data(self, validation_report: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data needed to populate the audit table"""
        return validation_report.get('audit_data', {})
    
    def validate_and_report(self, data_source: str, table_name: str, 
                          load_id: str = None, save_report: bool = True) -> Tuple[Dict[str, Any], str]:
        """
        Main method: validate schema and save report
        
        Returns:
            Tuple of (validation_report, report_file_path)
        """
        # Perform validation
        validation_report = self.validate_schema(data_source, table_name, load_id)
        
        # Save report if requested
        report_file_path = None
        if save_report:
            report_file_path = self.save_validation_report(validation_report)
        
        return validation_report, report_file_path
    
    def get_schema_history(self, data_source: str, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get schema version history for a table"""
        return self.schema_db.get_schema_history(data_source, table_name, limit)
    
    def get_recent_changes(self, data_source: str = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent schema changes across all or specific data sources"""
        return self.schema_db.get_recent_changes(data_source, limit)
    
    def register_initial_schema(self, data_source: str, table_name: str) -> Dict[str, Any]:
        """Register a table's schema for the first time"""
        logger.info(f"📝 Registering initial schema for {data_source}.{table_name}")
        
        # This is essentially a validation but we know it's the first time
        return self.validate_schema(data_source, table_name)
    
    def force_schema_update(self, data_source: str, table_name: str, 
                          new_version: str = None) -> Dict[str, Any]:
        """Force update schema version (manual override)"""
        logger.info(f"🔄 Force updating schema for {data_source}.{table_name}")
        
        current_schema = self._detect_current_schema(data_source, table_name)
        
        if not new_version:
            # Get current version and increment patch
            previous_schema = self.schema_db.get_current_schema(data_source, table_name)
            previous_version = previous_schema['schema_version'] if previous_schema else "0.0.0"
            new_version = self._increment_version(previous_version, "patch")
        
        schema_id = self._save_schema_version(data_source, table_name, new_version, current_schema)
        
        return {
            'schema_id': schema_id,
            'schema_version': new_version,
            'status': 'force_updated',
            'timestamp': datetime.now().isoformat()
        }
    
    def validate_compatibility_rules(self) -> Dict[str, Any]:
        """Validate that compatibility rules are properly configured"""
        rules = self.schema_db.get_compatibility_rules()
        
        validation = {
            'total_rules': len(rules),
            'active_rules': len([r for r in rules if r.get('is_active', True)]),
            'rules_by_type': {},
            'potential_issues': []
        }
        
        # Group by change type
        for rule in rules:
            change_type = rule['change_type']
            if change_type not in validation['rules_by_type']:
                validation['rules_by_type'][change_type] = 0
            validation['rules_by_type'][change_type] += 1
        
        # Check for missing common rules
        expected_types = ['add_column', 'remove_column', 'change_datatype', 'change_nullable']
        for expected_type in expected_types:
            if expected_type not in validation['rules_by_type']:
                validation['potential_issues'].append(f"No rules defined for {expected_type}")
        
        return validation
    
    def create_summary_report(self, validation_result: Dict[str, Any]) -> str:
        """Create human-readable summary of validation results"""
        metadata = validation_result['metadata']
        validation_results = validation_result['validation_results']
        schema_info = validation_result['schema_info']
        
        summary = f"""
🔍 SCHEMA REGISTRY VALIDATION REPORT
{'='*50}
📅 Validation Time: {metadata['validation_timestamp']}
🗄️  Data Source: {metadata['data_source']}
📊 Table: {metadata['table_name']}
🔗 Load ID: {metadata.get('load_id', 'Not specified')}

📋 SCHEMA INFORMATION:
{'='*30}
🆔 Schema ID: {schema_info['schema_id']}
📌 Version: {schema_info['schema_version']}
🔢 Hash: {schema_info['schema_hash'][:16]}...
📊 Columns: {schema_info['column_count']}
📈 Rows: {schema_info['table_row_count']:,}

🎯 VALIDATION RESULTS:
{'='*30}
✅ Status: {validation_results['overall_status'].upper()}
🎬 Action: {validation_results['recommended_action']}
🔄 Has Changes: {'Yes' if validation_results['has_changes'] else 'No'}
📊 Total Changes: {validation_results['change_count']}
⚠️  Breaking Changes: {validation_results['breaking_changes']}
🚫 Halt Pipeline: {'Yes' if validation_results['should_halt_pipeline'] else 'No'}
"""
        
        # Add change details if any
        if validation_results['has_changes']:
            changes = validation_result['changes']
            summary += f"""
📝 CHANGE SUMMARY:
{'='*25}
➕ Columns Added: {changes['change_summary']['columns_added']}
➖ Columns Removed: {changes['change_summary']['columns_removed']}
🔄 Columns Modified: {changes['change_summary']['columns_modified']}
🔀 Columns Reordered: {changes['change_summary']['columns_reordered']}
"""
            
            # Show first few changes
            if changes.get('detailed_changes'):
                summary += f"\n📋 RECENT CHANGES:\n{'-'*25}\n"
                for i, change in enumerate(changes['detailed_changes'][:3]):
                    summary += f"• {change.get('description', 'Unknown change')}\n"
                
                if len(changes['detailed_changes']) > 3:
                    summary += f"... and {len(changes['detailed_changes']) - 3} more changes\n"
        
        # Add recommendations
        summary += f"""
💡 RECOMMENDATIONS:
{'='*25}
"""
        if validation_results['overall_status'] == 'breaking':
            summary += "🚨 IMMEDIATE ACTION REQUIRED: Breaking changes detected\n"
            summary += "   - Review changes before proceeding\n"
            summary += "   - Consider rollback if data integrity at risk\n"
        elif validation_results['overall_status'] == 'warning':
            summary += "⚠️  REVIEW RECOMMENDED: Warning-level changes detected\n"
            summary += "   - Monitor for downstream impacts\n"
            summary += "   - Update documentation as needed\n"
        else:
            summary += "✅ PROCEED: Schema changes are compatible\n"
            summary += "   - Safe to continue pipeline\n"
        
        return summary