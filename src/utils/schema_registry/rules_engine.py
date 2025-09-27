"""
Compatibility rules engine for evaluating schema changes
Location: src/utilities/schema_registry/rules_engine.py
"""

from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CompatibilityRulesEngine:
    """Evaluates schema changes against compatibility rules"""
    
    def __init__(self, schema_db):
        self.schema_db = schema_db
        self._rule_cache = {}
        self._load_rules()
    
    def _load_rules(self) -> None:
        """Load compatibility rules from database"""
        try:
            rules = self.schema_db.get_compatibility_rules()
            self._rule_cache = {rule['rule_id']: rule for rule in rules}
            logger.info(f"📋 Loaded {len(self._rule_cache)} compatibility rules")
        except Exception as e:
            logger.error(f"Failed to load compatibility rules: {e}")
            self._rule_cache = {}
    
    def evaluate_changes(self, changes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate schema changes against compatibility rules
        
        Args:
            changes: Schema changes from comparator
            
        Returns:
            Evaluation results with compatibility levels and actions
        """
        logger.info("⚖️ Evaluating schema changes against compatibility rules...")
        
        evaluation = {
            'overall_compatibility': 'safe',
            'overall_action': 'proceed',
            'evaluated_changes': [],
            'rule_matches': [],
            'unmatched_changes': []
        }
        
        for change in changes.get('detailed_changes', []):
            change_evaluation = self._evaluate_single_change(change)
            evaluation['evaluated_changes'].append(change_evaluation)
            
            # Update overall compatibility (most restrictive wins)
            if self._is_more_restrictive(change_evaluation['compatibility_level'], evaluation['overall_compatibility']):
                evaluation['overall_compatibility'] = change_evaluation['compatibility_level']
                evaluation['overall_action'] = change_evaluation['recommended_action']
        
        logger.info(f"📊 Evaluation complete: {evaluation['overall_compatibility']} ({len(evaluation['evaluated_changes'])} changes)")
        return evaluation
    
    def _evaluate_single_change(self, change: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate a single schema change against rules"""
        change_type = change.get('change_type')
        
        # Find matching rules
        matching_rules = self._find_matching_rules(change)
        
        if matching_rules:
            # Use the most specific rule (first match for now)
            applied_rule = matching_rules[0]
            
            return {
                'change': change,
                'applied_rule': applied_rule,
                'compatibility_level': applied_rule['compatibility_level'],
                'recommended_action': applied_rule['auto_action'],
                'rule_confidence': 'high',
                'evaluation_notes': f"Matched rule {applied_rule['rule_id']}"
            }
        else:
            # No matching rule - use default heuristics
            default_evaluation = self._apply_default_heuristics(change)
            
            return {
                'change': change,
                'applied_rule': None,
                'compatibility_level': default_evaluation['compatibility_level'],
                'recommended_action': default_evaluation['recommended_action'],
                'rule_confidence': 'low',
                'evaluation_notes': 'No specific rule found, using default heuristics'
            }
    
    def _find_matching_rules(self, change: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find rules that match a specific change"""
        change_type = change.get('change_type')
        matching_rules = []
        
        for rule in self._rule_cache.values():
            if not rule.get('is_active', True):
                continue
                
            if rule['change_type'] != change_type and rule['change_type'] != 'any':
                continue
            
            # Check additional constraints
            if self._rule_matches_change_constraints(rule, change):
                matching_rules.append(rule)
        
        # Sort by specificity (more specific rules first)
        matching_rules.sort(key=lambda r: self._calculate_rule_specificity(r), reverse=True)
        
        return matching_rules
    
    def _rule_matches_change_constraints(self, rule: Dict[str, Any], change: Dict[str, Any]) -> bool:
        """Check if a rule matches the specific constraints of a change"""
        change_details = change.get('details', {})
        
        # Check from_constraint
        from_constraint = rule.get('from_constraint')
        if from_constraint and from_constraint != 'any':
            if not self._constraint_matches(from_constraint, change_details, 'from'):
                return False
        
        # Check to_constraint
        to_constraint = rule.get('to_constraint')
        if to_constraint and to_constraint != 'any':
            if not self._constraint_matches(to_constraint, change_details, 'to'):
                return False
        
        # Check data_type_category
        data_type_category = rule.get('data_type_category')
        if data_type_category and data_type_category != 'any':
            if not self._data_type_matches(data_type_category, change_details):
                return False
        
        return True
    
    def _constraint_matches(self, constraint: str, change_details: Dict[str, Any], direction: str) -> bool:
        """Check if a constraint matches the change details"""
        if constraint == 'any':
            return True
        
        if constraint == 'nullable':
            if direction == 'from':
                return change_details.get('old_nullable', False)
            else:  # to
                return change_details.get('new_nullable', False) or change_details.get('is_nullable', False)
        
        elif constraint == 'not_null':
            if direction == 'from':
                return not change_details.get('old_nullable', True)
            else:  # to
                return not (change_details.get('new_nullable', True) or change_details.get('is_nullable', True))
        
        elif constraint == 'varchar_small':
            # Heuristic: VARCHAR with length <= 50
            if direction == 'from':
                old_type = change_details.get('old_data_type', '').upper()
            else:
                old_type = change_details.get('new_data_type', '').upper()
            
            if 'VARCHAR' in old_type and '(' in old_type:
                try:
                    length = int(old_type.split('(')[1].split(')')[0])
                    return length <= 50
                except:
                    return False
        
        elif constraint == 'varchar_large':
            # Heuristic: VARCHAR with length > 50
            if direction == 'from':
                old_type = change_details.get('old_data_type', '').upper()
            else:
                old_type = change_details.get('new_data_type', '').upper()
            
            if 'VARCHAR' in old_type and '(' in old_type:
                try:
                    length = int(old_type.split('(')[1].split(')')[0])
                    return length > 50
                except:
                    return False
        
        return False
    
    def _data_type_matches(self, category: str, change_details: Dict[str, Any]) -> bool:
        """Check if data type category matches"""
        if category == 'any':
            return True
        
        # Get type categories from change details
        old_category = change_details.get('old_type_category')
        new_category = change_details.get('new_type_category')
        current_category = change_details.get('type_category')  # For additions
        
        target_category = new_category or current_category or old_category
        
        if category == 'string_to_numeric':
            return old_category == 'string' and new_category in ['integer', 'numeric']
        elif category == 'numeric_to_string':
            return old_category in ['integer', 'numeric'] and new_category == 'string'
        else:
            return target_category == category
    
    def _calculate_rule_specificity(self, rule: Dict[str, Any]) -> int:
        """Calculate rule specificity score (higher = more specific)"""
        specificity = 0
        
        # More specific change types get higher scores
        if rule.get('change_type') != 'any':
            specificity += 10
        
        # Specific constraints increase specificity
        if rule.get('from_constraint') and rule.get('from_constraint') != 'any':
            specificity += 5
        
        if rule.get('to_constraint') and rule.get('to_constraint') != 'any':
            specificity += 5
        
        if rule.get('data_type_category') and rule.get('data_type_category') != 'any':
            specificity += 3
        
        return specificity
    
    def _apply_default_heuristics(self, change: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default heuristics when no rules match"""
        change_type = change.get('change_type')
        
        # Conservative default heuristics
        heuristics = {
            'add_column': {
                'compatibility_level': 'warning',
                'recommended_action': 'warn_proceed'
            },
            'remove_column': {
                'compatibility_level': 'breaking',
                'recommended_action': 'halt'
            },
            'change_datatype': {
                'compatibility_level': 'breaking',
                'recommended_action': 'halt'
            },
            'change_nullable': {
                'compatibility_level': 'warning',
                'recommended_action': 'warn_proceed'
            },
            'change_default': {
                'compatibility_level': 'safe',
                'recommended_action': 'proceed'
            },
            'reorder_columns': {
                'compatibility_level': 'safe',
                'recommended_action': 'proceed'
            }
        }
        
        return heuristics.get(change_type, {
            'compatibility_level': 'warning',
            'recommended_action': 'warn_proceed'
        })
    
    def _is_more_restrictive(self, level1: str, level2: str) -> bool:
        """Check if level1 is more restrictive than level2"""
        hierarchy = {'safe': 0, 'warning': 1, 'breaking': 2}
        return hierarchy.get(level1, 1) > hierarchy.get(level2, 1)
    
    def add_custom_rule(self, rule_data: Dict[str, Any]) -> str:
        """Add a custom compatibility rule"""
        try:
            rule_id = self.schema_db.add_compatibility_rule(rule_data)
            self._load_rules()  # Reload rules cache
            logger.info(f"Added custom rule: {rule_id}")
            return rule_id
        except Exception as e:
            logger.error(f"Failed to add custom rule: {e}")
            raise
    
    def get_rule_recommendations(self, changes: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get recommendations for creating new rules based on unmatched changes"""
        recommendations = []
        
        for change in changes.get('detailed_changes', []):
            matching_rules = self._find_matching_rules(change)
            
            if not matching_rules:
                # Suggest a rule for this unmatched change
                recommendation = self._generate_rule_recommendation(change)
                recommendations.append(recommendation)
        
        return recommendations
    
    def _generate_rule_recommendation(self, change: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a rule recommendation for an unmatched change"""
        change_type = change.get('change_type')
        details = change.get('details', {})
        
        # Create a suggested rule based on the change pattern
        suggested_rule = {
            'change_type': change_type,
            'from_constraint': 'any',
            'to_constraint': 'any',
            'data_type_category': 'any',
            'compatibility_level': 'warning',  # Conservative default
            'auto_action': 'warn_proceed',
            'rule_config': {
                'generated_from_change': True,
                'original_change': change
            }
        }
        
        # Refine rule based on change specifics
        if change_type == 'add_column':
            if details.get('is_nullable', True):
                suggested_rule['to_constraint'] = 'nullable'
                suggested_rule['compatibility_level'] = 'safe'
                suggested_rule['auto_action'] = 'proceed'
            else:
                suggested_rule['to_constraint'] = 'not_null'
                suggested_rule['compatibility_level'] = 'breaking'
                suggested_rule['auto_action'] = 'halt'
        
        return {
            'change': change,
            'suggested_rule': suggested_rule,
            'confidence': 'medium',
            'rationale': f"Based on {change_type} pattern analysis"
        }