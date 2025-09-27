"""
Test suite for ManifestGenerator component
Following TDD approach with AAA pattern and descriptive naming
"""

import pytest
from datetime import datetime, timezone, timedelta
from api_adapter.manifest_generator import ManifestGenerator


class TestManifestGenerator:
    """Test suite for ManifestGenerator processing summary functionality"""
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.manifest_generator = ManifestGenerator()
        self.base_time = datetime.now(timezone.utc)
    
    def test_generate_manifest_data_with_complete_results_returns_full_manifest(self):
        """
        Test that complete processing results generate a full manifest with all sections
        """
        # Arrange
        processing_results = {
            'load_id': 'test_load_123',
            'data_source': 'scopus_search',
            'start_time': self.base_time,
            'end_time': self.base_time + timedelta(minutes=30),
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': [],
                    'start_time': self.base_time,
                    'end_time': self.base_time + timedelta(minutes=10)
                },
                {
                    'entity_id': 'author_002',
                    'status': 'completed',
                    'pages_processed': 3,
                    'total_results': 75,
                    'errors': [],
                    'start_time': self.base_time + timedelta(minutes=10),
                    'end_time': self.base_time + timedelta(minutes=20)
                }
            ]
        }
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(processing_results)
        
        # Assert
        assert manifest['load_id'] == 'test_load_123'
        assert manifest['data_source'] == 'scopus_search'
        assert 'manifest_id' in manifest
        assert 'generated_timestamp' in manifest
        assert 'processing_summary' in manifest
        assert 'entity_details' in manifest
        assert 'metadata' in manifest
        
        # Check metadata structure
        metadata = manifest['metadata']
        assert metadata['manifest_version'] == '1.0'
        assert metadata['generator'] == 'APIOrchestrator'
        assert metadata['total_entities_requested'] == 2
        assert metadata['processing_start'] is not None
        assert metadata['processing_end'] is not None
    
    def test_generate_manifest_data_with_empty_results_returns_empty_manifest(self):
        """
        Test that empty processing results return a manifest with zero counts
        """
        # Arrange
        empty_results = {}
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(empty_results)
        
        # Assert
        assert manifest['load_id'] == ''
        assert manifest['data_source'] == ''
        assert manifest['processing_summary']['total_entities'] == 0
        assert manifest['processing_summary']['successful_entities'] == 0
        assert manifest['entity_details'] == []
        assert manifest['metadata']['total_entities_requested'] == 0
        assert manifest['metadata']['processing_start'] is None
        assert manifest['metadata']['processing_end'] is None
    
    def test_generate_manifest_data_with_no_entities_returns_empty_manifest(self):
        """
        Test that results with empty entities list return empty manifest
        """
        # Arrange
        results_no_entities = {
            'load_id': 'test_load_empty',
            'data_source': 'scopus_search',
            'entities': []
        }
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(results_no_entities)
        
        # Assert
        assert manifest['processing_summary']['total_entities'] == 0
        assert manifest['entity_details'] == []
        assert manifest['metadata']['total_entities_requested'] == 0
    
    def test_calculate_summary_statistics_with_all_successful_entities_returns_100_percent_success(self):
        """
        Test that all successful entities result in 100% success rate
        """
        # Arrange
        results = {
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': []
                },
                {
                    'entity_id': 'author_002',
                    'status': 'completed',
                    'pages_processed': 3,
                    'total_results': 75,
                    'errors': []
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        assert stats['total_entities'] == 2
        assert stats['successful_entities'] == 2
        assert stats['failed_entities'] == 0
        assert stats['partial_entities'] == 0
        assert stats['success_rate_percent'] == 100.0
        assert stats['total_api_responses'] == 8  # 5 + 3
        assert stats['total_results_retrieved'] == 200  # 125 + 75
        assert stats['average_results_per_entity'] == 100.0  # 200 / 2
    
    def test_calculate_summary_statistics_with_mixed_results_calculates_correct_percentages(self):
        """
        Test that mixed successful and failed entities calculate correct success rate
        """
        # Arrange
        results = {
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': []
                },
                {
                    'entity_id': 'author_002',
                    'status': 'failed',
                    'pages_processed': 2,
                    'total_results': 50,
                    'errors': [{'type': 'APIError', 'message': 'Rate limit exceeded'}]
                },
                {
                    'entity_id': 'author_003',
                    'status': 'partial',
                    'pages_processed': 3,
                    'total_results': 75,
                    'errors': [{'type': 'TimeoutError', 'message': 'Request timeout'}]
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        assert stats['total_entities'] == 3
        assert stats['successful_entities'] == 1
        assert stats['failed_entities'] == 1
        assert stats['partial_entities'] == 1
        assert stats['success_rate_percent'] == 33.33  # 1/3 * 100, rounded to 2 dp
        assert stats['total_api_responses'] == 10  # 5 + 2 + 3
        assert stats['total_results_retrieved'] == 250  # 125 + 50 + 75
    
    def test_calculate_summary_statistics_with_no_entities_returns_zero_statistics(self):
        """
        Test that empty entities list returns zero statistics
        """
        # Arrange
        results = {'entities': []}
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        assert stats['total_entities'] == 0
        assert stats['successful_entities'] == 0
        assert stats['failed_entities'] == 0
        assert stats['success_rate_percent'] == 0.0
        assert stats['total_api_responses'] == 0
        assert stats['total_results_retrieved'] == 0
        assert stats['average_results_per_entity'] == 0.0
    
    def test_calculate_summary_statistics_with_processing_duration_calculates_time_correctly(self):
        """
        Test that processing duration is calculated correctly from start and end times
        """
        # Arrange
        start_time = self.base_time
        end_time = self.base_time + timedelta(minutes=30)  # 30 minutes = 1800 seconds
        
        results = {
            'start_time': start_time,
            'end_time': end_time,
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 2,
                    'total_results': 50,
                    'errors': []
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        duration = stats['processing_duration']
        assert duration['total_seconds'] == 1800.0
        assert duration['average_seconds_per_entity'] == 1800.0  # 1800 / 1 entity
        assert duration['start_time'] == start_time.isoformat()
        assert duration['end_time'] == end_time.isoformat()
    
    def test_calculate_summary_statistics_with_multiple_entities_calculates_average_duration(self):
        """
        Test that average processing time per entity is calculated correctly
        """
        # Arrange
        start_time = self.base_time
        end_time = self.base_time + timedelta(minutes=60)  # 60 minutes = 3600 seconds
        
        results = {
            'start_time': start_time,
            'end_time': end_time,
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 2,
                    'total_results': 50,
                    'errors': []
                },
                {
                    'entity_id': 'author_002',
                    'status': 'completed',
                    'pages_processed': 3,
                    'total_results': 75,
                    'errors': []
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        duration = stats['processing_duration']
        assert duration['total_seconds'] == 3600.0
        assert duration['average_seconds_per_entity'] == 1800.0  # 3600 / 2 entities
    
    def test_calculate_summary_statistics_with_errors_calculates_error_statistics(self):
        """
        Test that error statistics are calculated correctly
        """
        # Arrange
        results = {
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': []
                },
                {
                    'entity_id': 'author_002',
                    'status': 'failed',
                    'pages_processed': 2,
                    'total_results': 50,
                    'errors': [
                        {'type': 'APIError', 'message': 'Rate limit exceeded'},
                        {'type': 'APIError', 'message': 'Invalid response'}
                    ]
                },
                {
                    'entity_id': 'author_003',
                    'status': 'partial',
                    'pages_processed': 3,
                    'total_results': 75,
                    'errors': [
                        {'type': 'TimeoutError', 'message': 'Request timeout'}
                    ]
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        error_summary = stats['error_summary']
        assert error_summary['total_errors'] == 3
        assert error_summary['entities_with_errors'] == 2
        assert error_summary['error_types']['APIError'] == 2
        assert error_summary['error_types']['TimeoutError'] == 1
    
    def test_calculate_summary_statistics_with_no_errors_returns_empty_error_statistics(self):
        """
        Test that entities with no errors return empty error statistics
        """
        # Arrange
        results = {
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': []
                }
            ]
        }
        
        # Act
        stats = self.manifest_generator.calculate_summary_statistics(results)
        
        # Assert
        error_summary = stats['error_summary']
        assert error_summary['total_errors'] == 0
        assert error_summary['entities_with_errors'] == 0
        assert error_summary['error_types'] == {}
    
    def test_entity_details_formatting_includes_processing_time_and_errors(self):
        """
        Test that entity details are formatted correctly with processing time and error details
        """
        # Arrange
        entity_start = self.base_time
        entity_end = self.base_time + timedelta(minutes=15)  # 15 minutes = 900 seconds
        
        processing_results = {
            'load_id': 'test_load_123',
            'data_source': 'scopus_search',
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': [],
                    'start_time': entity_start,
                    'end_time': entity_end
                },
                {
                    'entity_id': 'author_002',
                    'status': 'failed',
                    'pages_processed': 2,
                    'total_results': 50,
                    'errors': [
                        {
                            'type': 'APIError',
                            'message': 'Rate limit exceeded',
                            'page_num': 3
                        }
                    ],
                    'start_time': entity_start,
                    'end_time': entity_end
                }
            ]
        }
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(processing_results)
        
        # Assert
        entity_details = manifest['entity_details']
        assert len(entity_details) == 2
        
        # Check first entity (successful)
        entity1 = entity_details[0]
        assert entity1['entity_id'] == 'author_001'
        assert entity1['status'] == 'completed'
        assert entity1['pages_processed'] == 5
        assert entity1['total_results'] == 125
        assert entity1['error_count'] == 0
        assert entity1['processing_time_seconds'] == 900.0
        assert 'errors' not in entity1  # No errors section for successful entity
        
        # Check second entity (failed with errors)
        entity2 = entity_details[1]
        assert entity2['entity_id'] == 'author_002'
        assert entity2['status'] == 'failed'
        assert entity2['error_count'] == 1
        assert 'errors' in entity2
        assert len(entity2['errors']) == 1
        assert entity2['errors'][0]['error_type'] == 'APIError'
        assert entity2['errors'][0]['error_message'] == 'Rate limit exceeded'
        assert entity2['errors'][0]['page_number'] == 3
    
    def test_entity_duration_calculation_with_missing_timestamps_returns_zero(self):
        """
        Test that entities without start/end times return zero processing duration
        """
        # Arrange
        processing_results = {
            'load_id': 'test_load_123',
            'data_source': 'scopus_search',
            'entities': [
                {
                    'entity_id': 'author_001',
                    'status': 'completed',
                    'pages_processed': 5,
                    'total_results': 125,
                    'errors': []
                    # Missing start_time and end_time
                }
            ]
        }
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(processing_results)
        
        # Assert
        entity_details = manifest['entity_details']
        assert entity_details[0]['processing_time_seconds'] == 0.0
    
    def test_manifest_contains_valid_uuid_and_timestamp(self):
        """
        Test that generated manifest contains valid UUID and ISO timestamp
        """
        # Arrange
        processing_results = {
            'load_id': 'test_load_123',
            'data_source': 'scopus_search',
            'entities': []
        }
        
        # Act
        manifest = self.manifest_generator.generate_manifest_data(processing_results)
        
        # Assert
        # Check UUID format (36 characters with hyphens)
        manifest_id = manifest['manifest_id']
        assert len(manifest_id) == 36
        assert manifest_id.count('-') == 4
        
        # Check ISO timestamp format
        timestamp = manifest['generated_timestamp']
        assert 'T' in timestamp
        assert timestamp.endswith('Z') or '+' in timestamp or '-' in timestamp[-6:]
        
        # Verify it can be parsed as datetime
        parsed_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        assert isinstance(parsed_time, datetime)