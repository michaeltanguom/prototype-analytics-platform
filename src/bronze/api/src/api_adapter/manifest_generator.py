"""
ManifestGenerator module for API processing summary generation
"""
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import uuid


class ManifestGenerator:
    """Generates processing summaries and manifests for API ingestion runs"""
    
    def generate_manifest_data(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate complete manifest with summary statistics
        
        Args:
            processing_results: Dictionary containing processing results with structure:
                {
                    'load_id': str,
                    'data_source': str,
                    'entities': List[Dict] with keys: entity_id, status, pages_processed, 
                               total_results, errors, start_time, end_time
                    'start_time': datetime,
                    'end_time': datetime
                }
        
        Returns:
            Dict containing complete manifest data
        """
        if not processing_results or not processing_results.get('entities'):
            return self._generate_empty_manifest()
        
        summary_stats = self.calculate_summary_statistics(processing_results)
        
        manifest = {
            'manifest_id': str(uuid.uuid4()),
            'load_id': processing_results.get('load_id', ''),
            'data_source': processing_results.get('data_source', ''),
            'generated_timestamp': datetime.now(timezone.utc).isoformat(),
            'processing_summary': summary_stats,
            'entity_details': self._format_entity_details(processing_results.get('entities', [])),
            'metadata': {
                'manifest_version': '1.0',
                'generator': 'APIOrchestrator',
                'total_entities_requested': len(processing_results.get('entities', [])),
                'processing_start': processing_results.get('start_time').isoformat() if processing_results.get('start_time') else None,
                'processing_end': processing_results.get('end_time').isoformat() if processing_results.get('end_time') else None
            }
        }
        
        return manifest
    
    def calculate_summary_statistics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate summary statistics from processing results
        
        Args:
            results: Processing results dictionary
            
        Returns:
            Dictionary containing calculated statistics
        """
        entities = results.get('entities', [])
        
        if not entities:
            return self._get_empty_statistics()
        
        # Count successful and failed entities
        successful_entities = [e for e in entities if e.get('status') == 'completed']
        failed_entities = [e for e in entities if e.get('status') == 'failed']
        partial_entities = [e for e in entities if e.get('status') == 'partial']
        
        total_entities = len(entities)
        successful_count = len(successful_entities)
        failed_count = len(failed_entities)
        partial_count = len(partial_entities)
        
        # Calculate success rate
        success_rate = (successful_count / total_entities * 100) if total_entities > 0 else 0.0
        
        # Calculate total responses/pages processed
        total_responses = sum(e.get('pages_processed', 0) for e in entities)
        total_results_retrieved = sum(e.get('total_results', 0) for e in entities)
        
        # Calculate processing duration
        duration_stats = self._calculate_duration_statistics(results, entities)
        
        # Calculate error statistics
        error_stats = self._calculate_error_statistics(entities)
        
        return {
            'total_entities': total_entities,
            'successful_entities': successful_count,
            'failed_entities': failed_count,
            'partial_entities': partial_count,
            'success_rate_percent': round(success_rate, 2),
            'total_api_responses': total_responses,
            'total_results_retrieved': total_results_retrieved,
            'average_results_per_entity': round(total_results_retrieved / total_entities, 2) if total_entities > 0 else 0.0,
            'processing_duration': duration_stats,
            'error_summary': error_stats
        }
    
    def _generate_empty_manifest(self) -> Dict[str, Any]:
        """Generate manifest structure for empty processing results"""
        return {
            'manifest_id': str(uuid.uuid4()),
            'load_id': '',
            'data_source': '',
            'generated_timestamp': datetime.now(timezone.utc).isoformat(),
            'processing_summary': self._get_empty_statistics(),
            'entity_details': [],
            'metadata': {
                'manifest_version': '1.0',
                'generator': 'APIOrchestrator',
                'total_entities_requested': 0,
                'processing_start': None,
                'processing_end': None
            }
        }
    
    def _get_empty_statistics(self) -> Dict[str, Any]:
        """Return empty statistics structure"""
        return {
            'total_entities': 0,
            'successful_entities': 0,
            'failed_entities': 0,
            'partial_entities': 0,
            'success_rate_percent': 0.0,
            'total_api_responses': 0,
            'total_results_retrieved': 0,
            'average_results_per_entity': 0.0,
            'processing_duration': {
                'total_seconds': 0.0,
                'average_seconds_per_entity': 0.0,
                'start_time': None,
                'end_time': None
            },
            'error_summary': {
                'total_errors': 0,
                'error_types': {},
                'entities_with_errors': 0
            }
        }
    
    def _format_entity_details(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format entity details for manifest output"""
        formatted_entities = []
        
        for entity in entities:
            formatted_entity = {
                'entity_id': entity.get('entity_id', ''),
                'status': entity.get('status', 'unknown'),
                'pages_processed': entity.get('pages_processed', 0),
                'total_results': entity.get('total_results', 0),
                'error_count': len(entity.get('errors', [])),
                'processing_time_seconds': self._calculate_entity_duration(entity)
            }
            
            # Include error details if present
            if entity.get('errors'):
                formatted_entity['errors'] = [
                    {
                        'error_type': error.get('type', 'Unknown'),
                        'error_message': error.get('message', ''),
                        'page_number': error.get('page_num')
                    }
                    for error in entity.get('errors', [])
                ]
            
            formatted_entities.append(formatted_entity)
        
        return formatted_entities
    
    def _calculate_duration_statistics(self, results: Dict[str, Any], entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate processing duration statistics"""
        start_time = results.get('start_time')
        end_time = results.get('end_time')
        
        duration_stats = {
            'total_seconds': 0.0,
            'average_seconds_per_entity': 0.0,
            'start_time': start_time.isoformat() if start_time else None,
            'end_time': end_time.isoformat() if end_time else None
        }
        
        if start_time and end_time:
            total_duration = (end_time - start_time).total_seconds()
            duration_stats['total_seconds'] = round(total_duration, 2)
            
            if entities:
                duration_stats['average_seconds_per_entity'] = round(
                    total_duration / len(entities), 2
                )
        
        return duration_stats
    
    def _calculate_entity_duration(self, entity: Dict[str, Any]) -> float:
        """Calculate processing duration for a single entity"""
        start_time = entity.get('start_time')
        end_time = entity.get('end_time')
        
        if start_time and end_time:
            return round((end_time - start_time).total_seconds(), 2)
        
        return 0.0
    
    def _calculate_error_statistics(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate error statistics across all entities"""
        total_errors = 0
        error_types = {}
        entities_with_errors = 0
        
        for entity in entities:
            entity_errors = entity.get('errors', [])
            if entity_errors:
                entities_with_errors += 1
                total_errors += len(entity_errors)
                
                # Count error types
                for error in entity_errors:
                    error_type = error.get('type', 'Unknown')
                    error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'total_errors': total_errors,
            'error_types': error_types,
            'entities_with_errors': entities_with_errors
        }