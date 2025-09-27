"""
RecoveryManager module for handling partial failure restart logic
"""

import json
from pathlib import Path
from typing import List, Optional
from api_adapter.state_manager import StateManager
from api_adapter.file_hasher import FileHasher


class RecoveryManager:
    """Handles partial failure restart logic from original script"""
    
    def __init__(self, state_manager: StateManager, file_hasher: FileHasher):
        self.state_manager = state_manager
        self.file_hasher = file_hasher
    
    def identify_failed_pages(self, entity_id: str, load_id: str) -> List[int]:
        """
        Identify which pages failed during processing based on error log
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            
        Returns:
            List of page numbers that encountered errors
        """
        state = self.state_manager.load_state(entity_id, load_id)
        if not state or not state.errors:
            return []
        
        # Extract unique page numbers from error log
        failed_pages = set()
        for error in state.errors:
            if 'page_number' in error:
                failed_pages.add(error['page_number'])
        
        return sorted(list(failed_pages))
    
    def validate_existing_files(self, entity_id: str, load_id: str) -> bool:
        """
        Validate that all downloaded files exist and have correct integrity
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            
        Returns:
            True if all files are valid, False if any are missing or corrupted
        """
        state = self.state_manager.load_state(entity_id, load_id)
        if not state or not state.file_names:
            return True  # No files to validate
        
        # Check each file exists and has correct hash
        for file_name in state.file_names:
            file_path = Path(file_name)  # Assume file_name includes full path
            
            # Check file exists
            if not file_path.exists():
                return False
            
            # Check file integrity if hash available
            expected_hash = state.file_hashes.get(file_name)
            if expected_hash:
                if not self._validate_file_integrity(file_path, expected_hash):
                    return False
        
        return True
    
    def restart_from_page(self, entity_id: str, load_id: str, page_num: int) -> None:
        """
        Reset processing state to restart from a specific page
        
        Args:
            entity_id: Entity identifier
            load_id: Load identifier
            page_num: Page number to restart from
        """
        state = self.state_manager.load_state(entity_id, load_id)
        if not state:
            return  # No state to modify
        
        # Reset pagination state
        state.last_page = page_num - 1
        state.pages_processed = page_num - 1
        state.completed = False
        
        # Clear data from restart page onwards
        self._clear_data_from_page(state, page_num)
        
        # Save updated state
        self.state_manager.save_state(state)
    
    def _validate_file_integrity(self, file_path: Path, expected_hash: str) -> bool:
        """
        Validate that a file's content matches its expected hash
        
        Args:
            file_path: Path to the file to validate
            expected_hash: Expected hash value
            
        Returns:
            True if file integrity is valid, False otherwise
        """
        try:
            # Read file content
            with open(file_path, 'r') as f:
                file_content = json.load(f)
            
            # Generate hash of current content
            current_hash = self.file_hasher.generate_content_hash(file_content)
            
            # Compare with expected hash
            return current_hash == expected_hash
            
        except (OSError, json.JSONDecodeError, ValueError):
            # File unreadable or corrupted
            return False
    
    def _clear_data_from_page(self, state, restart_page: int) -> None:
        """
        Remove data associated with pages from restart_page onwards
        
        Args:
            state: ProcessingState object to modify
            restart_page: Page number to start clearing from
        """
        # Filter file names to keep only those before restart page
        # Assume file names contain page information that can be extracted
        valid_files = []
        valid_hashes = {}
        
        for file_name in state.file_names:
            # Extract page number from file name (assuming pattern like 'page{num}')
            page_num = self._extract_page_number_from_filename(file_name)
            if page_num is not None and page_num < restart_page:
                valid_files.append(file_name)
                if file_name in state.file_hashes:
                    valid_hashes[file_name] = state.file_hashes[file_name]
        
        state.file_names = valid_files
        state.file_hashes = valid_hashes
        
        # Filter errors to keep only those from valid pages
        valid_errors = []
        for error in state.errors:
            error_page = error.get('page_number')
            if error_page is not None and error_page < restart_page:
                valid_errors.append(error)
        
        state.errors = valid_errors
        
        # Note: processed_items might need more sophisticated filtering
        # depending on how items map to pages - for now, keep all items
        # as they might be from valid pages
    
    def _extract_page_number_from_filename(self, filename: str) -> Optional[int]:
        """
        Extract page number from filename
        
        Args:
            filename: Filename that may contain page information
            
        Returns:
            Page number if found, None otherwise
        """
        import re
        
        # Look for patterns like 'page3', 'page_3', 'p3', etc.
        patterns = [
            r'page(\d+)',
            r'page_(\d+)',
            r'p(\d+)',
            r'_(\d+)\.json',
            r'_(\d+)_'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename.lower())
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        
        return None