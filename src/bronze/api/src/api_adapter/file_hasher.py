"""
FileHasher module for content deduplication
"""
import hashlib
import json
from typing import Dict, Any


class FileHasher:
    """Utility class for generating consistent file hashes for deduplication"""
    
    @staticmethod
    def generate_content_hash(data: Dict[str, Any]) -> str:
        """Generate MD5 hash of content for deduplication"""
        content_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(content_str.encode('utf-8')).hexdigest()
    
    @staticmethod
    def compare_file_hashes(hash1: str, hash2: str) -> bool:
        """Compare two file hashes for equality"""
        if hash1 is None or hash2 is None:
            return False
        return hash1 == hash2