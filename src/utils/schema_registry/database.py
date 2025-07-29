"""
Database operations for schema registry
Location: src/utilities/schema_registry/database.py
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class SchemaRegistryDB:
    """Handles all database operations for schema registry"""
    
    def __init__(self, db_path: str = "schema_registry.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialise schema registry database with required tables"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Schema versions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    schema_id TEXT PRIMARY KEY,
                    data_source TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    schema_hash_value TEXT NOT NULL,
                    schema_report JSON NOT NULL,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_current BOOLEAN DEFAULT TRUE,
                    detection_method TEXT DEFAULT 'auto',
                    metadata JSON
                )
            """)
            
            # Schema evolution log
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_evolution_log (
                    change_id TEXT PRIMARY KEY,
                    schema_id_from TEXT,
                    schema_id_to TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    change_details JSON NOT NULL,
                    compatibility_level TEXT NOT NULL,
                    auto_action TEXT NOT NULL,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    validation_status TEXT DEFAULT 'pending',
                    load_id TEXT,
                    FOREIGN KEY (schema_id_from) REFERENCES schema_versions (schema_id),
                    FOREIGN KEY (schema_id_to) REFERENCES schema_versions (schema_id)
                )
            """)
            
            # Schema compatibility rules
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_compatibility_rules (
                    rule_id TEXT PRIMARY KEY,
                    change_type TEXT NOT NULL,
                    from_constraint TEXT,
                    to_constraint TEXT,
                    data_type_category TEXT,
                    compatibility_level TEXT NOT NULL,
                    auto_action TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    rule_config JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schema_versions_current ON schema_versions (data_source, table_name, is_current)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_evolution_log_from_to ON schema_evolution_log (schema_id_from, schema_id_to)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_compatibility_rules_active ON schema_compatibility_rules (change_type, is_active)")
            
            # Insert default compatibility rules
            self._insert_default_rules(conn)
    
    def _insert_default_rules(self, conn: sqlite3.Connection) -> None:
        """Insert default compatibility rules"""
        default_rules = [
            ("R001", "add_column", "any", "nullable", "any", "safe", "proceed"),
            ("R002", "add_column", "any", "not_null", "any", "breaking", "halt"),
            ("R003", "remove_column", "any", "any", "any", "breaking", "halt"),
            ("R004", "change_datatype", "any", "any", "string_to_numeric", "breaking", "halt"),
            ("R005", "change_datatype", "varchar_small", "varchar_large", "string", "safe", "proceed"),
            ("R006", "change_datatype", "varchar_large", "varchar_small", "string", "warning", "warn_proceed"),
            ("R007", "change_datatype", "integer", "bigint", "numeric", "safe", "proceed"),
            ("R008", "change_nullable", "not_null", "nullable", "any", "safe", "proceed"),
            ("R009", "change_nullable", "nullable", "not_null", "any", "breaking", "halt"),
            ("R010", "reorder_columns", "any", "any", "any", "safe", "proceed"),
            ("R011", "add_index", "any", "any", "any", "safe", "proceed"),
            ("R012", "change_primary_key", "any", "any", "any", "breaking", "halt")
        ]
        
        for rule in default_rules:
            conn.execute("""
                INSERT OR IGNORE INTO schema_compatibility_rules 
                (rule_id, change_type, from_constraint, to_constraint, 
                 data_type_category, compatibility_level, auto_action, rule_config)
                VALUES (?, ?, ?, ?, ?, ?, ?, '{}')
            """, rule)
        
        conn.commit()
        logger.info(f"Initialised schema registry database with {len(default_rules)} default rules")
    
    def get_current_schema(self, data_source: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get the current schema for a data source and table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM schema_versions 
                WHERE data_source = ? AND table_name = ? AND is_current = TRUE
                ORDER BY detected_at DESC LIMIT 1
            """, (data_source, table_name))
            
            row = cursor.fetchone()
            if row:
                result = dict(row)
                # Parse JSON fields
                result['schema_report'] = json.loads(result['schema_report'])
                if result['metadata']:
                    result['metadata'] = json.loads(result['metadata'])
                return result
            return None
    
    def save_schema_version(self, schema_data: Dict[str, Any]) -> str:
        """Save a new schema version"""
        schema_id = f"{schema_data['data_source']}_{schema_data['table_name']}_{schema_data['schema_version']}"
        
        with sqlite3.connect(self.db_path) as conn:
            # Mark previous versions as not current
            conn.execute("""
                UPDATE schema_versions 
                SET is_current = FALSE 
                WHERE data_source = ? AND table_name = ?
            """, (schema_data['data_source'], schema_data['table_name']))
            
            # Insert new schema version
            conn.execute("""
                INSERT INTO schema_versions 
                (schema_id, data_source, table_name, schema_version, 
                 schema_hash_value, schema_report, detection_method, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                schema_id,
                schema_data['data_source'],
                schema_data['table_name'], 
                schema_data['schema_version'],
                schema_data['schema_hash_value'],
                json.dumps(schema_data['schema_report']),
                schema_data.get('detection_method', 'auto'),
                json.dumps(schema_data.get('metadata', {}))
            ))
            
            conn.commit()
            logger.info(f"Saved new schema version: {schema_id}")
        
        return schema_id
    
    def log_schema_change(self, change_data: Dict[str, Any]) -> str:
        """Log a schema change in the evolution log"""
        change_id = f"CHG_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{change_data.get('load_id', 'UNKNOWN')}"
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO schema_evolution_log 
                (change_id, schema_id_from, schema_id_to, change_type, 
                 change_details, compatibility_level, auto_action, load_id, validation_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                change_id,
                change_data.get('schema_id_from'),
                change_data['schema_id_to'],
                change_data['change_type'],
                json.dumps(change_data['change_details']),
                change_data['compatibility_level'],
                change_data['auto_action'],
                change_data.get('load_id'),
                change_data.get('validation_status', 'pending')
            ))
            
            conn.commit()
            logger.info(f"Logged schema change: {change_id}")
        
        return change_id
    
    def get_compatibility_rules(self, change_type: str = None) -> List[Dict[str, Any]]:
        """Get compatibility rules, optionally filtered by change type"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if change_type:
                cursor = conn.execute("""
                    SELECT * FROM schema_compatibility_rules 
                    WHERE change_type = ? AND is_active = TRUE
                    ORDER BY created_at ASC
                """, (change_type,))
            else:
                cursor = conn.execute("""
                    SELECT * FROM schema_compatibility_rules 
                    WHERE is_active = TRUE
                    ORDER BY change_type, created_at ASC
                """)
            
            rows = cursor.fetchall()
            rules = []
            for row in rows:
                rule = dict(row)
                rule['rule_config'] = json.loads(rule['rule_config']) if rule['rule_config'] else {}
                rules.append(rule)
            
            return rules
    
    def get_schema_history(self, data_source: str, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get schema version history for a table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM schema_versions 
                WHERE data_source = ? AND table_name = ?
                ORDER BY detected_at DESC LIMIT ?
            """, (data_source, table_name, limit))
            
            rows = cursor.fetchall()
            history = []
            for row in rows:
                version = dict(row)
                version['schema_report'] = json.loads(version['schema_report'])
                if version['metadata']:
                    version['metadata'] = json.loads(version['metadata'])
                history.append(version)
            
            return history
    
    def get_recent_changes(self, data_source: str = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent schema changes"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if data_source:
                # Join with schema_versions to filter by data_source
                cursor = conn.execute("""
                    SELECT sel.*, sv.data_source, sv.table_name 
                    FROM schema_evolution_log sel
                    JOIN schema_versions sv ON sel.schema_id_to = sv.schema_id
                    WHERE sv.data_source = ?
                    ORDER BY sel.detected_at DESC LIMIT ?
                """, (data_source, limit))
            else:
                cursor = conn.execute("""
                    SELECT sel.*, sv.data_source, sv.table_name 
                    FROM schema_evolution_log sel
                    JOIN schema_versions sv ON sel.schema_id_to = sv.schema_id
                    ORDER BY sel.detected_at DESC LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
            changes = []
            for row in rows:
                change = dict(row)
                change['change_details'] = json.loads(change['change_details'])
                changes.append(change)
            
            return changes
    
    def update_change_validation_status(self, change_id: str, status: str) -> None:
        """Update the validation status of a schema change"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE schema_evolution_log 
                SET validation_status = ? 
                WHERE change_id = ?
            """, (status, change_id))
            conn.commit()
            logger.info(f"Updated change {change_id} validation status to {status}")
    
    def add_compatibility_rule(self, rule_data: Dict[str, Any]) -> str:
        """Add a new compatibility rule"""
        rule_id = rule_data.get('rule_id') or f"R{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO schema_compatibility_rules 
                (rule_id, change_type, from_constraint, to_constraint, 
                 data_type_category, compatibility_level, auto_action, rule_config)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule_id,
                rule_data['change_type'],
                rule_data.get('from_constraint'),
                rule_data.get('to_constraint'),
                rule_data.get('data_type_category'),
                rule_data['compatibility_level'],
                rule_data['auto_action'],
                json.dumps(rule_data.get('rule_config', {}))
            ))
            conn.commit()
            logger.info(f"Added new compatibility rule: {rule_id}")
        
        return rule_id