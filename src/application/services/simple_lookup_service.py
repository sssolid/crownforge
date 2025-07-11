# src/application/services/simple_lookup_service.py
"""
Simple application lookup service implementation.
"""

import json
import logging
from typing import List, Dict, Optional
from pathlib import Path

from ...domain.interfaces import LookupService
from ...domain.models import ApplicationLookupEntry

logger = logging.getLogger(__name__)


class SimpleApplicationLookupService(LookupService):
    """Simple lookup service for application processing."""

    def __init__(self, lookup_file_path: str):
        self.lookup_file_path = Path(lookup_file_path)
        self.lookup_entries: List[ApplicationLookupEntry] = []
        self.usage_stats: Dict[str, int] = {}

        if self.lookup_file_path.exists():
            self.load_lookup_data(str(self.lookup_file_path))
        else:
            logger.warning(f"Lookup file not found: {self.lookup_file_path}")

    def load_lookup_data(self, file_path: str) -> None:
        """Load lookup data from JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.lookup_entries = []
            if isinstance(data, dict):
                # Handle key-value format like in the original application
                for key, values in data.items():
                    if isinstance(values, list):
                        for value in values:
                            parts = value.split('|') if isinstance(value, str) else []
                            make = parts[0] if len(parts) > 0 else ""
                            code = parts[1] if len(parts) > 1 else ""
                            model = parts[2] if len(parts) > 2 else ""

                            entry = ApplicationLookupEntry(
                                original_text=key,
                                make=make,
                                model=model,
                                year_start=1900,  # Default values
                                year_end=2025,
                                code=code,
                                note=""
                            )
                            self.lookup_entries.append(entry)
            elif isinstance(data, list):
                # Handle list format
                for item in data:
                    entry = ApplicationLookupEntry(
                        original_text=item.get('original_text', ''),
                        make=item.get('make', ''),
                        model=item.get('model', ''),
                        year_start=item.get('year_start', 1900),
                        year_end=item.get('year_end', 2025),
                        code=item.get('code', ''),
                        note=item.get('note', '')
                    )
                    self.lookup_entries.append(entry)

            logger.info(f"Loaded {len(self.lookup_entries)} lookup entries from {file_path}")

        except Exception as e:
            logger.error(f"Failed to load lookup data from {file_path}: {e}")
            self.lookup_entries = []

    def find_matching_applications(self, search_text: str) -> List[ApplicationLookupEntry]:
        """Find matching applications for given text."""
        if not search_text or not search_text.strip():
            return []

        search_lower = search_text.lower().strip()
        matches = []

        for entry in self.lookup_entries:
            if search_lower.startswith(entry.original_text.lower()):
                entry.match_score = 1.0
                matches.append(entry)
                # Track usage
                self.usage_stats[entry.original_text] = self.usage_stats.get(entry.original_text, 0) + 1

        # Sort by match score and length (longer matches first)
        matches.sort(key=lambda x: (x.match_score, len(x.original_text)), reverse=True)
        return matches

    def get_best_match(self, search_text: str) -> Optional[ApplicationLookupEntry]:
        """Get the best matching application."""
        matches = self.find_matching_applications(search_text)
        return matches[0] if matches else None

    def get_usage_statistics(self) -> Dict[str, int]:
        """Get lookup usage statistics."""
        return {
            'total_lookups': sum(self.usage_stats.values()),
            'unique_keys_used': len(self.usage_stats),
            'total_keys_available': len(self.lookup_entries),
            'usage_details': dict(self.usage_stats)
        }