"""Cryptographic utilities for data integrity."""

import hashlib
from typing import bytes as Bytes


def calculate_checksum(data: Bytes) -> str:
    """Calculate SHA-256 checksum of data."""
    return hashlib.sha256(data).hexdigest()


def verify_checksum(data: Bytes, expected_checksum: str) -> bool:
    """Verify data integrity using checksum."""
    actual_checksum = calculate_checksum(data)
    return actual_checksum == expected_checksum


def generate_file_id(filename: str, data: Bytes) -> str:
    """Generate unique file ID based on filename and content."""
    content = f"{filename}:{calculate_checksum(data)}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]