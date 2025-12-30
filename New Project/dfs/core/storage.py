"""Storage engine for local file operations."""

import os
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from dfs.utils.crypto import calculate_checksum, verify_checksum
from dfs.utils.logging import get_logger


@dataclass
class FileMetadata:
    """File metadata structure."""
    filename: str
    file_id: str
    size: int
    checksum: str
    created_at: float
    modified_at: float
    version: str
    replicas: List[str]
    metadata: Dict[str, str]


class StorageEngine:
    """Local storage engine for file operations."""
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path = self.storage_path / "metadata.json"
        self.files_path = self.storage_path / "files"
        self.files_path.mkdir(exist_ok=True)
        self.logger = get_logger("storage")
        
        # Load existing metadata
        self.metadata: Dict[str, FileMetadata] = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, FileMetadata]:
        """Load file metadata from disk."""
        if not self.metadata_path.exists():
            return {}
        
        try:
            with open(self.metadata_path, 'r') as f:
                data = json.load(f)
                return {
                    filename: FileMetadata(**meta_dict)
                    for filename, meta_dict in data.items()
                }
        except Exception as e:
            self.logger.error(f"Failed to load metadata: {e}")
            return {}
    
    def _save_metadata(self) -> None:
        """Save file metadata to disk."""
        try:
            data = {
                filename: asdict(metadata)
                for filename, metadata in self.metadata.items()
            }
            with open(self.metadata_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save metadata: {e}")
    
    def _get_file_path(self, file_id: str) -> Path:
        """Get the file path for a given file ID."""
        return self.files_path / file_id
    
    def store_file(self, filename: str, file_id: str, data: bytes, 
                   checksum: str, metadata: Dict[str, str] = None) -> bool:
        """Store a file locally."""
        try:
            # Verify checksum
            if not verify_checksum(data, checksum):
                self.logger.error(f"Checksum verification failed for {filename}")
                return False
            
            # Write file data
            file_path = self._get_file_path(file_id)
            with open(file_path, 'wb') as f:
                f.write(data)
            
            # Store metadata
            now = time.time()
            file_metadata = FileMetadata(
                filename=filename,
                file_id=file_id,
                size=len(data),
                checksum=checksum,
                created_at=now,
                modified_at=now,
                version="1",
                replicas=[],
                metadata=metadata or {}
            )
            
            self.metadata[filename] = file_metadata
            self._save_metadata()
            
            self.logger.info(f"Stored file {filename} with ID {file_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store file {filename}: {e}")
            return False
    
    def retrieve_file(self, filename: str) -> Optional[tuple[bytes, FileMetadata]]:
        """Retrieve a file by filename."""
        try:
            if filename not in self.metadata:
                return None
            
            file_metadata = self.metadata[filename]
            file_path = self._get_file_path(file_metadata.file_id)
            
            if not file_path.exists():
                self.logger.error(f"File data missing for {filename}")
                return None
            
            with open(file_path, 'rb') as f:
                data = f.read()
            
            # Verify integrity
            if not verify_checksum(data, file_metadata.checksum):
                self.logger.error(f"Data corruption detected for {filename}")
                return None
            
            return data, file_metadata
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve file {filename}: {e}")
            return None
    
    def delete_file(self, filename: str) -> bool:
        """Delete a file."""
        try:
            if filename not in self.metadata:
                return False
            
            file_metadata = self.metadata[filename]
            file_path = self._get_file_path(file_metadata.file_id)
            
            # Remove file data
            if file_path.exists():
                file_path.unlink()
            
            # Remove metadata
            del self.metadata[filename]
            self._save_metadata()
            
            self.logger.info(f"Deleted file {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete file {filename}: {e}")
            return False
    
    def list_files(self, prefix: str = "", limit: int = 100) -> List[FileMetadata]:
        """List files with optional prefix filter."""
        files = []
        for filename, metadata in self.metadata.items():
            if filename.startswith(prefix):
                files.append(metadata)
                if len(files) >= limit:
                    break
        return files
    
    def get_file_info(self, filename: str) -> Optional[FileMetadata]:
        """Get file metadata."""
        return self.metadata.get(filename)
    
    def file_exists(self, filename: str) -> bool:
        """Check if file exists."""
        return filename in self.metadata
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        total_files = len(self.metadata)
        total_size = sum(meta.size for meta in self.metadata.values())
        
        # Get available disk space
        stat = os.statvfs(self.storage_path)
        available_space = stat.f_bavail * stat.f_frsize
        
        return {
            "total_files": total_files,
            "total_size": total_size,
            "available_space": available_space,
            "storage_path": str(self.storage_path)
        }