"""
Schema detection logic for extracting schemas from database tables
Location: src/utilities/schema_registry/detector.py
"""

import duckdb
import hashlib
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class SchemaDetector:
    """Detects and extracts schema information from database tables"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def extract_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Extract complete schema information from a DuckDB table
        
        Args:
            table_name: Name of the table to analyse
            
        Returns:
            Dictionary containing schema information
        """
        try:
            logger.info(f"🔍 Extracting schema from table: {table_name}")
            
            with duckdb.connect(self.db_path) as conn:
                # Get column information
                columns_info = self._get_columns_info(conn, table_name)
                
                # Get table metadata
                table_metadata = self._get_table_metadata(conn, table_name)
                
                # Generate schema hash
                schema_hash = self._generate_schema_hash(columns_info)
                
                schema_report = {
                    'table_name': table_name,
                    'columns': columns_info,
                    'metadata': table_metadata,
                    'schema_hash': schema_hash,
                    'extraction_timestamp': self._get_timestamp()
                }
                
                logger.info(f"✅ Schema extracted: {len(columns_info)} columns, hash: {schema_hash[:8]}...")
                return schema_report
                
        except Exception as e:
            logger.error(f"❌ Failed to extract schema from {table_name}: {e}")
            raise
    
    def _get_columns_info(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> List[Dict[str, Any]]:
        """Extract detailed column information"""
        # Get column details from information_schema
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        result = conn.execute(query).fetchall()
        columns = []
        
        for row in result:
            column_name, data_type, is_nullable, column_default, ordinal_position = row
            
            column_info = {
                'name': column_name,
                'data_type': data_type,
                'is_nullable': is_nullable == 'YES',
                'default_value': column_default,
                'position': ordinal_position,
                'constraints': self._detect_column_constraints(data_type, is_nullable, column_default)
            }
            
            # Add additional type categorisation
            column_info['type_category'] = self._categorise_data_type(data_type)
            
            columns.append(column_info)
        
        return columns
    
    def _get_table_metadata(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, Any]:
        """Extract table-level metadata"""
        try:
            # Get row count
            row_count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = row_count_result[0] if row_count_result else 0
            
            # Get table size (approximate)
            size_query = f"SELECT pg_total_relation_size('{table_name}') as size"
            try:
                size_result = conn.execute(size_query).fetchone()
                table_size = size_result[0] if size_result else None
            except:
                table_size = None  # DuckDB might not support this function
            
            # Detect primary key constraints (basic detection)
            primary_key_columns = self._detect_primary_key(conn, table_name)
            
            metadata = {
                'row_count': row_count,
                'table_size_bytes': table_size,
                'primary_key_columns': primary_key_columns,
                'has_data': row_count > 0
            }
            
            return metadata
            
        except Exception as e:
            logger.warning(f"⚠️ Could not extract all table metadata: {e}")
            return {'row_count': 0, 'has_data': False}
    
    def _detect_primary_key(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
        """Attempt to detect primary key columns"""
        try:
            # Try to get constraint information (may not be fully supported in DuckDB)
            pk_query = f"""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_name = '{table_name}' 
            AND constraint_name LIKE '%PRIMARY%'
            """
            
            result = conn.execute(pk_query).fetchall()
            return [row[0] for row in result]
            
        except:
            # Fallback: look for columns that might be primary keys
            # This is heuristic-based detection
            return self._heuristic_primary_key_detection(conn, table_name)
    
    def _heuristic_primary_key_detection(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
        """Heuristic detection of likely primary key columns"""
        potential_pk_columns = []
        
        try:
            # Look for columns with 'id' in the name that are unique
            id_columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND (LOWER(column_name) LIKE '%id%' OR LOWER(column_name) = 'key')
            """
            
            id_columns = conn.execute(id_columns_query).fetchall()
            
            for column_row in id_columns:
                column_name = column_row[0]
                # Check if column has unique values
                uniqueness_query = f"""
                SELECT COUNT(*) as total, COUNT(DISTINCT {column_name}) as unique_count
                FROM {table_name}
                """
                
                result = conn.execute(uniqueness_query).fetchone()
                if result and result[0] == result[1] and result[0] > 0:
                    potential_pk_columns.append(column_name)
                    
        except Exception as e:
            logger.debug(f"Heuristic PK detection failed: {e}")
        
        return potential_pk_columns
    
    def _detect_column_constraints(self, data_type: str, is_nullable: str, default_value: Any) -> Dict[str, Any]:
        """Detect and categorise column constraints"""
        constraints = {
            'nullable': is_nullable == 'YES',
            'has_default': default_value is not None,
            'default_value': default_value
        }
        
        # Add type-specific constraints
        if 'VARCHAR' in data_type.upper():
            # Extract length constraint if present
            if '(' in data_type and ')' in data_type:
                try:
                    length_str = data_type.split('(')[1].split(')')[0]
                    constraints['max_length'] = int(length_str)
                except:
                    pass
        
        return constraints
    
    def _categorise_data_type(self, data_type: str) -> str:
        """Categorise data types into broader categories for rule matching"""
        data_type_upper = data_type.upper()
        
        if any(t in data_type_upper for t in ['VARCHAR', 'TEXT', 'CHAR', 'STRING']):
            return 'string'
        elif any(t in data_type_upper for t in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']):
            return 'integer'
        elif any(t in data_type_upper for t in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL']):
            return 'numeric'
        elif any(t in data_type_upper for t in ['BOOLEAN', 'BOOL']):
            return 'boolean'
        elif any(t in data_type_upper for t in ['DATE', 'TIME', 'TIMESTAMP']):
            return 'datetime'
        elif 'JSON' in data_type_upper:
            return 'json'
        else:
            return 'other'
    
    def _generate_schema_hash(self, columns_info: List[Dict[str, Any]]) -> str:
        """Generate a hash representing the schema structure"""
        # Create a normalised representation for hashing
        hash_data = []
        
        for col in sorted(columns_info, key=lambda x: x['position']):
            # Include key characteristics that define schema compatibility
            col_signature = {
                'name': col['name'],
                'data_type': col['data_type'],
                'nullable': col['is_nullable'],
                'position': col['position']
            }
            hash_data.append(col_signature)
        
        # Create hash from JSON representation
        hash_string = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def validate_table_exists(self, table_name: str) -> bool:
        """Check if table exists in the database"""
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                """).fetchone()
                
                return result[0] > 0 if result else False
                
        except Exception as e:
            logger.error(f"❌ Error checking table existence: {e}")
            return False
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from table for analysis"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # Get column names first
                columns_result = conn.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    ORDER BY ordinal_position
                """).fetchall()
                
                column_names = [row[0] for row in columns_result]
                
                # Get sample data
                result = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()
                
                # Convert to list of dictionaries
                sample_data = []
                for row in result:
                    row_dict = {}
                    for i, value in enumerate(row):
                        if i < len(column_names):
                            row_dict[column_names[i]] = value
                    sample_data.append(row_dict)
                
                return sample_data
                
        except Exception as e:
            logger.warning(f"⚠️ Could not extract sample data: {e}")
            return []
    
    def extract_load_id(self, table_name: str) -> Optional[str]:
        """Extract load_id from staging table"""
        try:
            logger.info(f"🔍 Extracting load_id from staging table: {table_name}")
            
            with duckdb.connect(self.db_path) as conn:
                # Check if load_id column exists
                columns_result = conn.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND LOWER(column_name) = 'load_id'
                """).fetchall()
                
                if not columns_result:
                    logger.warning(f"⚠️ No load_id column found in table {table_name}")
                    return None
                
                # Extract load_id
                query = f"SELECT DISTINCT load_id FROM {table_name} WHERE load_id IS NOT NULL LIMIT 1"
                result = conn.execute(query).fetchone()
                
                if result and result[0] is not None:
                    load_id = str(result[0])
                    logger.info(f"🔗 Successfully extracted load_id: {load_id}")
                    return load_id
                else:
                    logger.warning(f"⚠️ No load_id found in staging table {table_name}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Failed to extract load_id from {table_name}: {e}")
            return None
                