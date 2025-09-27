"""
StateManager module for handling processing state persistence and recovery
"""

import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field
from api_adapter.database_manager import DatabaseManager


@dataclass
class ProcessingState:
    """State information for resumable processing"""
    entity_id: str
    load_id: str
    data_source: str = ""
    last_page: int = 0
    last_start_index: int = 0
    total_results: Optional[int] = None
    pages_processed: int = 0
    completed: bool = False
    processed_items: List[str] = field(default_factory=list)
    file_names: List[str] = field(default_factory=list)
    file_hashes: Dict[str, str] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    created_timestamp: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)


class StateManager:
    """Manages processing state persistence and recovery using DuckDB"""
    
    def __init__(self, database_manager: DatabaseManager):
        self.db_manager = database_manager
    
    def load_state(self, entity_id: str, load_id: str) -> Optional[ProcessingState]:
        """
        Load existing processing state for entity and load_id
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            
        Returns:
            ProcessingState object if found, None otherwise
            
        Raises:
            Exception: If no active database connection
        """
        if not self.db_manager._connection:
            raise Exception("No active database connection")
        
        query = """
        SELECT * FROM processing_state 
        WHERE load_id = ? AND entity_id = ?
        """
        
        result = self.db_manager._connection.execute(query, (load_id, entity_id))
        row = result.fetchone()
        
        if row is None:
            return None
        
        # Convert row to ProcessingState object
        return ProcessingState(
            load_id=row[0],
            entity_id=row[1],
            data_source=row[2],
            last_page=row[3],
            last_start_index=row[4],
            total_results=row[5],
            pages_processed=row[6],
            completed=row[7],
            processed_items=row[8] if row[8] else [],
            file_names=row[9] if row[9] else [],
            file_hashes=row[10] if row[10] else {},
            errors=row[11] if row[11] else [],
            created_timestamp=datetime.fromisoformat(row[12]) if isinstance(row[12], str) else row[12],
            last_updated=datetime.fromisoformat(row[13]) if isinstance(row[13], str) else row[13]
        )
    
    def save_state(self, state: ProcessingState) -> None:
        """
        Save or update processing state using UPSERT pattern
        
        Args:
            state: ProcessingState object to save
        """
        # Update timestamp
        state.last_updated = datetime.now()
        
        upsert_sql = """
        INSERT INTO processing_state (
            load_id, entity_id, data_source, last_page, last_start_index,
            total_results, pages_processed, completed, processed_items,
            file_names, file_hashes, errors, created_timestamp, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (load_id, entity_id) DO UPDATE SET
            data_source = EXCLUDED.data_source,
            last_page = EXCLUDED.last_page,
            last_start_index = EXCLUDED.last_start_index,
            total_results = EXCLUDED.total_results,
            pages_processed = EXCLUDED.pages_processed,
            completed = EXCLUDED.completed,
            processed_items = EXCLUDED.processed_items,
            file_names = EXCLUDED.file_names,
            file_hashes = EXCLUDED.file_hashes,
            errors = EXCLUDED.errors,
            last_updated = EXCLUDED.last_updated
        """
        
        params = (
            state.load_id,
            state.entity_id,
            state.data_source,
            state.last_page,
            state.last_start_index,
            state.total_results,
            state.pages_processed,
            state.completed,
            state.processed_items,
            state.file_names,
            state.file_hashes,
            state.errors,
            state.created_timestamp,
            state.last_updated
        )
        
        self.db_manager.execute_with_transaction(upsert_sql, params)
    
    def mark_completed(self, entity_id: str, load_id: str) -> None:
        """
        Mark entity processing as completed
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
        """
        update_sql = """
        UPDATE processing_state 
        SET completed = TRUE
        WHERE load_id = ? AND entity_id = ?
        """
        
        params = (load_id, entity_id)
        self.db_manager.execute_with_transaction(update_sql, params)
    
    def get_incomplete_entities(self, entity_ids: List[str], load_id: str) -> List[str]:
        """
        Identify which entities need processing for a specific load_id
        
        Args:
            entity_ids: List of all entity IDs to check
            load_id: Load identifier
            
        Returns:
            List of entity IDs that need processing
        """
        if not entity_ids:
            return []
        
        # Find entities that are marked as completed
        query = """
        SELECT entity_id FROM processing_state 
        WHERE load_id = ? AND completed = TRUE
        """
        
        result = self.db_manager._connection.execute(query, (load_id,))
        completed_entities = {row[0] for row in result.fetchall()}
        
        # Return entities that are not in the completed set
        incomplete_entities = [entity_id for entity_id in entity_ids if entity_id not in completed_entities]
        
        return incomplete_entities
    
    def update_page_progress(self, entity_id: str, load_id: str, page_num: int) -> None:
        """
        Update page processing progress for an entity
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            page_num: Current page number being processed
        """
        update_sql = """
        UPDATE processing_state 
        SET last_page = ?, pages_processed = ?
        WHERE load_id = ? AND entity_id = ?
        """
        
        params = (page_num, page_num, load_id, entity_id)
        self.db_manager.execute_with_transaction(update_sql, params)
    
    def add_processed_item(self, entity_id: str, load_id: str, identifier: str) -> None:
        """
        Add processed item identifier to the state
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            identifier: Item identifier (DOI, etc.) to add
        """
        # First load current state to get existing processed items
        current_state = self.load_state(entity_id, load_id)
        if current_state is None:
            # If no state exists, we can't update processed items
            return
        
        # Add new identifier if not already present
        if identifier not in current_state.processed_items:
            current_state.processed_items.append(identifier)
        
        # Update only the processed_items field
        update_sql = """
        UPDATE processing_state 
        SET processed_items = ?, last_updated = ?
        WHERE load_id = ? AND entity_id = ?
        """
        
        params = (current_state.processed_items, datetime.now(), load_id, entity_id)
        self.db_manager.execute_with_transaction(update_sql, params)
    
    def record_file_hash(self, entity_id: str, load_id: str, filename: str, file_hash: str) -> None:
        """
        Record file hash for deduplication purposes
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            filename: Name of the file
            file_hash: Hash of the file content
        """
        # First load current state to get existing file hashes
        current_state = self.load_state(entity_id, load_id)
        if current_state is None:
            # If no state exists, we can't update file hashes
            return
        
        # Update file hash mapping
        current_state.file_hashes[filename] = file_hash
        
        # Update only the file_hashes field
        update_sql = """
        UPDATE processing_state 
        SET file_hashes = ?, last_updated = ?
        WHERE load_id = ? AND entity_id = ?
        """
        
        params = (current_state.file_hashes, datetime.now(), load_id, entity_id)
        self.db_manager.execute_with_transaction(update_sql, params)
    
    def log_processing_error(self, entity_id: str, load_id: str, error: Exception, page_num: int) -> None:
        """
        Log processing error for an entity
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            error: Exception that occurred
            page_num: Page number where error occurred
        """
        # First load current state to get existing errors
        current_state = self.load_state(entity_id, load_id)
        if current_state is None:
            # If no state exists, we can't update errors
            return
        
        # Create error entry
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "page_number": page_num,
            "error_type": type(error).__name__,
            "error_message": str(error)
        }
        
        # Add to errors list
        current_state.errors.append(error_entry)
        
        # Update only the errors field
        update_sql = """
        UPDATE processing_state 
        SET errors = ?, last_updated = ?
        WHERE load_id = ? AND entity_id = ?
        """
        
        params = (current_state.errors, datetime.now(), load_id, entity_id)
        self.db_manager.execute_with_transaction(update_sql, params)