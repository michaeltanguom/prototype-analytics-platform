"""
Schema comparison logic for detecting changes between schema versions
Location: src/utilities/schema_registry/comparator.py
"""

from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class SchemaComparator:
    """Compares schemas and detects changes between versions"""
    
    def __init__(self):
        pass
    
    def compare_schemas(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two schemas and detect all changes
        
        Args:
            old_schema: Previous schema version
            new_schema: Current schema version
            
        Returns:
            Dictionary containing detected changes
        """
        logger.info("🔍 Comparing schemas for changes...")
        
        changes = {
            'has_changes': False,
            'change_summary': {
                'columns_added': 0,
                'columns_removed': 0,
                'columns_modified': 0,
                'columns_reordered': 0
            },
            'detailed_changes': []
        }
        
        # Extract column information
        old_columns = {col['name']: col for col in old_schema.get('columns', [])}
        new_columns = {col['name']: col for col in new_schema.get('columns', [])}
        
        # Detect column additions
        added_columns = set(new_columns.keys()) - set(old_columns.keys())
        for col_name in added_columns:
            change = self._create_column_addition_change(col_name, new_columns[col_name])
            changes['detailed_changes'].append(change)
            changes['change_summary']['columns_added'] += 1
            changes['has_changes'] = True
        
        # Detect column removals
        removed_columns = set(old_columns.keys()) - set(new_columns.keys())
        for col_name in removed_columns:
            change = self._create_column_removal_change(col_name, old_columns[col_name])
            changes['detailed_changes'].append(change)
            changes['change_summary']['columns_removed'] += 1
            changes['has_changes'] = True
        
        # Detect column modifications
        common_columns = set(old_columns.keys()) & set(new_columns.keys())
        for col_name in common_columns:
            old_col = old_columns[col_name]
            new_col = new_columns[col_name]
            
            column_changes = self._compare_column_properties(old_col, new_col)
            if column_changes:
                changes['detailed_changes'].extend(column_changes)
                changes['change_summary']['columns_modified'] += 1
                changes['has_changes'] = True
        
        # Detect column reordering
        reorder_changes = self._detect_column_reordering(old_columns, new_columns)
        if reorder_changes:
            changes['detailed_changes'].extend(reorder_changes)
            changes['change_summary']['columns_reordered'] += len(reorder_changes)
            changes['has_changes'] = True
        
        logger.info(f"📊 Schema comparison complete: {len(changes['detailed_changes'])} changes detected")
        return changes
    
    def _create_column_addition_change(self, col_name: str, column_info: Dict[str, Any]) -> Dict[str, Any]:
        """Create change record for column addition"""
        return {
            'change_type': 'add_column',
            'column_name': col_name,
            'description': f"Added column '{col_name}'",
            'details': {
                'column_name': col_name,
                'data_type': column_info.get('data_type'),
                'is_nullable': column_info.get('is_nullable'),
                'default_value': column_info.get('default_value'),
                'position': column_info.get('position')
            },
            'from_value': None,
            'to_value': column_info
        }
    
    def _create_column_removal_change(self, col_name: str, column_info: Dict[str, Any]) -> Dict[str, Any]:
        """Create change record for column removal"""
        return {
            'change_type': 'remove_column',
            'column_name': col_name,
            'description': f"Removed column '{col_name}'",
            'details': {
                'column_name': col_name,
                'data_type': column_info.get('data_type'),
                'was_nullable': column_info.get('is_nullable'),
                'had_default': column_info.get('default_value') is not None
            },
            'from_value': column_info,
            'to_value': None
        }
    
    def _compare_column_properties(self, old_col: Dict[str, Any], new_col: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compare properties of a single column between versions"""
        changes = []
        col_name = old_col['name']
        
        # Check data type changes
        if old_col.get('data_type') != new_col.get('data_type'):
            changes.append({
                'change_type': 'change_datatype',
                'column_name': col_name,
                'description': f"Changed data type of column '{col_name}' from {old_col.get('data_type')} to {new_col.get('data_type')}",
                'details': {
                    'column_name': col_name,
                    'old_data_type': old_col.get('data_type'),
                    'new_data_type': new_col.get('data_type'),
                    'old_type_category': old_col.get('type_category'),
                    'new_type_category': new_col.get('type_category')
                },
                'from_value': old_col.get('data_type'),
                'to_value': new_col.get('data_type')
            })
        
        # Check nullable changes
        if old_col.get('is_nullable') != new_col.get('is_nullable'):
            changes.append({
                'change_type': 'change_nullable',
                'column_name': col_name,
                'description': f"Changed nullable constraint of column '{col_name}' from {old_col.get('is_nullable')} to {new_col.get('is_nullable')}",
                'details': {
                    'column_name': col_name,
                    'old_nullable': old_col.get('is_nullable'),
                    'new_nullable': new_col.get('is_nullable')
                },
                'from_value': old_col.get('is_nullable'),
                'to_value': new_col.get('is_nullable')
            })
        
        # Check default value changes
        if old_col.get('default_value') != new_col.get('default_value'):
            changes.append({
                'change_type': 'change_default',
                'column_name': col_name,
                'description': f"Changed default value of column '{col_name}'",
                'details': {
                    'column_name': col_name,
                    'old_default': old_col.get('default_value'),
                    'new_default': new_col.get('default_value')
                },
                'from_value': old_col.get('default_value'),
                'to_value': new_col.get('default_value')
            })
        
        # Check position changes (for reordering detection)
        if old_col.get('position') != new_col.get('position'):
            # This is handled separately in reordering detection
            pass
        
        # Check constraint changes (if available)
        old_constraints = old_col.get('constraints', {})
        new_constraints = new_col.get('constraints', {})
        
        constraint_changes = self._compare_constraints(col_name, old_constraints, new_constraints)
        changes.extend(constraint_changes)
        
        return changes
    
    def _compare_constraints(self, col_name: str, old_constraints: Dict[str, Any], new_constraints: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compare column constraints between versions"""
        changes = []
        
        # Check length constraints for string types
        old_max_length = old_constraints.get('max_length')
        new_max_length = new_constraints.get('max_length')
        
        if old_max_length != new_max_length:
            changes.append({
                'change_type': 'change_constraint',
                'column_name': col_name,
                'description': f"Changed max length constraint of column '{col_name}' from {old_max_length} to {new_max_length}",
                'details': {
                    'column_name': col_name,
                    'constraint_type': 'max_length',
                    'old_value': old_max_length,
                    'new_value': new_max_length
                },
                'from_value': old_max_length,
                'to_value': new_max_length
            })
        
        return changes
    
    def _detect_column_reordering(self, old_columns: Dict[str, Dict], new_columns: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Detect if columns have been reordered"""
        changes = []
        
        # Get common columns that exist in both schemas
        common_columns = set(old_columns.keys()) & set(new_columns.keys())
        
        # Create position maps
        old_positions = {name: old_columns[name].get('position', 0) for name in common_columns}
        new_positions = {name: new_columns[name].get('position', 0) for name in common_columns}
        
        # Check if any columns changed position
        reordered_columns = []
        for col_name in common_columns:
            if old_positions[col_name] != new_positions[col_name]:
                reordered_columns.append({
                    'column_name': col_name,
                    'old_position': old_positions[col_name],
                    'new_position': new_positions[col_name]
                })
        
        if reordered_columns:
            changes.append({
                'change_type': 'reorder_columns',
                'column_name': None,  # Multiple columns affected
                'description': f"Reordered {len(reordered_columns)} columns",
                'details': {
                    'reordered_columns': reordered_columns,
                    'total_columns_reordered': len(reordered_columns)
                },
                'from_value': old_positions,
                'to_value': new_positions
            })
        
        return changes
    
    def get_change_severity(self, change: Dict[str, Any]) -> str:
        """
        Determine the severity level of a schema change
        This is a basic implementation - will be enhanced by rules engine
        """
        change_type = change.get('change_type')
        
        # Basic severity mapping
        severity_map = {
            'add_column': 'safe',      # Usually safe if nullable
            'remove_column': 'breaking',
            'change_datatype': 'breaking',  # Usually breaking
            'change_nullable': 'warning',   # Depends on direction
            'change_default': 'safe',
            'change_constraint': 'warning',
            'reorder_columns': 'safe'
        }
        
        base_severity = severity_map.get(change_type, 'warning')
        
        # Apply specific logic for certain change types
        if change_type == 'add_column':
            # Adding non-nullable column without default is breaking
            details = change.get('details', {})
            if not details.get('is_nullable', True) and details.get('default_value') is None:
                return 'breaking'
        
        elif change_type == 'change_nullable':
            # Making column nullable is safe, making it non-nullable is breaking
            details = change.get('details', {})
            if not details.get('old_nullable', True) and details.get('new_nullable', True):
                return 'safe'  # non-null -> nullable
            elif details.get('old_nullable', True) and not details.get('new_nullable', True):
                return 'breaking'  # nullable -> non-null
        
        return base_severity
    
    def summarise_changes_by_severity(self, changes: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Group changes by severity level"""
        severity_groups = {
            'safe': [],
            'warning': [],
            'breaking': []
        }
        
        for change in changes.get('detailed_changes', []):
            severity = self.get_change_severity(change)
            severity_groups[severity].append(change)
        
        return severity_groups
    
    def has_breaking_changes(self, changes: Dict[str, Any]) -> bool:
        """Check if any changes are considered breaking"""
        severity_groups = self.summarise_changes_by_severity(changes)
        return len(severity_groups['breaking']) > 0
    
    def get_change_impact_summary(self, changes: Dict[str, Any]) -> str:
        """Generate human-readable summary of change impact"""
        if not changes.get('has_changes', False):
            return "No schema changes detected"
        
        severity_groups = self.summarise_changes_by_severity(changes)
        
        summary_parts = []
        
        if severity_groups['breaking']:
            summary_parts.append(f"{len(severity_groups['breaking'])} breaking changes")
        
        if severity_groups['warning']:
            summary_parts.append(f"{len(severity_groups['warning'])} warning-level changes")
        
        if severity_groups['safe']:
            summary_parts.append(f"{len(severity_groups['safe'])} safe changes")
        
        return ", ".join(summary_parts)