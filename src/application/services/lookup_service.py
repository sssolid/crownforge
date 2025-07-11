# src/application/services/lookup_service.py
"""
Application lookup service for matching vehicle application text.
Works with existing application processing service.
"""

import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from difflib import SequenceMatcher

from ...domain.interfaces import LookupService
from ...domain.models import ApplicationLookupEntry

logger = logging.getLogger(__name__)


@dataclass
class LookupStatistics:
    """Statistics for lookup operations."""
    total_lookups: int = 0
    successful_matches: int = 0
    partial_matches: int = 0
    no_matches: int = 0
    cache_hits: int = 0


class ApplicationLookupService(LookupService):
    """Service for matching vehicle application text against lookup data."""

    def __init__(self, lookup_file_path: str):
        self.lookup_file_path = Path(lookup_file_path)
        self.lookup_data: List[ApplicationLookupEntry] = []
        self.make_index: Dict[str, List[ApplicationLookupEntry]] = {}
        self.model_index: Dict[str, List[ApplicationLookupEntry]] = {}
        self.year_index: Dict[int, List[ApplicationLookupEntry]] = {}
        self.statistics = LookupStatistics()
        self._match_cache: Dict[str, List[ApplicationLookupEntry]] = {}

        # Load lookup data
        if self.lookup_file_path.exists():
            self.load_lookup_data(str(self.lookup_file_path))
        else:
            logger.warning(f"Lookup file not found: {self.lookup_file_path}")

    def load_lookup_data(self, file_path: str) -> None:
        """Load lookup data from JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.lookup_data = []
            for item in data:
                entry = ApplicationLookupEntry(
                    original_text=item.get('original_text', ''),
                    make=item.get('make', ''),
                    model=item.get('model', ''),
                    year_start=item.get('year_start', 0),
                    year_end=item.get('year_end', 0),
                    code=item.get('code', ''),
                    note=item.get('note', '')
                )
                self.lookup_data.append(entry)

            # Build indexes for faster searching
            self._build_indexes()

            logger.info(f"Loaded {len(self.lookup_data)} lookup entries from {file_path}")

        except Exception as e:
            logger.error(f"Failed to load lookup data from {file_path}: {e}")
            self.lookup_data = []

    def find_matching_applications(self, search_text: str) -> List[ApplicationLookupEntry]:
        """Find matching applications for given text."""
        if not search_text or not search_text.strip():
            return []

        search_text = search_text.strip()
        self.statistics.total_lookups += 1

        # Check cache first
        cache_key = search_text.lower()
        if cache_key in self._match_cache:
            self.statistics.cache_hits += 1
            return self._match_cache[cache_key].copy()

        matches = []

        # Extract potential components from search text
        parsed_components = self._parse_search_text(search_text)

        # Direct text matching
        exact_matches = self._find_exact_matches(search_text)
        if exact_matches:
            matches.extend(exact_matches)
            self.statistics.successful_matches += 1

        # Component-based matching
        if not matches and parsed_components:
            component_matches = self._find_component_matches(parsed_components)
            if component_matches:
                matches.extend(component_matches)
                self.statistics.partial_matches += 1

        # Fuzzy matching as fallback
        if not matches:
            fuzzy_matches = self._find_fuzzy_matches(search_text)
            if fuzzy_matches:
                matches.extend(fuzzy_matches)
                self.statistics.partial_matches += 1
            else:
                self.statistics.no_matches += 1

        # Sort by match score and limit results
        matches.sort(key=lambda x: x.match_score, reverse=True)
        top_matches = matches[:10]  # Limit to top 10 matches

        # Cache results
        self._match_cache[cache_key] = top_matches.copy()

        return top_matches

    def get_best_match(self, search_text: str) -> Optional[ApplicationLookupEntry]:
        """Get the best matching application."""
        matches = self.find_matching_applications(search_text)
        return matches[0] if matches else None

    def get_usage_statistics(self) -> Dict[str, int]:
        """Get lookup usage statistics."""
        return {
            'total_lookups': self.statistics.total_lookups,
            'successful_matches': self.statistics.successful_matches,
            'partial_matches': self.statistics.partial_matches,
            'no_matches': self.statistics.no_matches,
            'cache_hits': self.statistics.cache_hits,
            'cache_size': len(self._match_cache),
            'total_entries': len(self.lookup_data)
        }

    def _build_indexes(self) -> None:
        """Build search indexes for faster lookups."""
        self.make_index.clear()
        self.model_index.clear()
        self.year_index.clear()

        for entry in self.lookup_data:
            # Make index
            make_key = entry.make.lower().strip()
            if make_key:
                if make_key not in self.make_index:
                    self.make_index[make_key] = []
                self.make_index[make_key].append(entry)

            # Model index
            model_key = entry.model.lower().strip()
            if model_key:
                if model_key not in self.model_index:
                    self.model_index[model_key] = []
                self.model_index[model_key].append(entry)

            # Year index
            for year in range(entry.year_start, entry.year_end + 1):
                if year not in self.year_index:
                    self.year_index[year] = []
                self.year_index[year].append(entry)

    def _parse_search_text(self, text: str) -> Dict[str, Any]:
        """Parse search text to extract components."""
        components = {
            'years': [],
            'makes': [],
            'models': [],
            'keywords': []
        }

        # Extract years (4 digits)
        year_pattern = r'\b(19|20)\d{2}\b'
        years = re.findall(year_pattern, text)
        if years:
            components['years'] = [int(year) for year in years]

        # Extract year ranges (e.g., "1997-2002", "97-02")
        range_pattern = r'\b(?:(19|20)?(\d{2})[-–](?:(19|20)?(\d{2})))\b'
        year_ranges = re.findall(range_pattern, text)
        for match in year_ranges:
            start_prefix, start_year, end_prefix, end_year = match

            # Handle 2-digit years
            if not start_prefix and len(start_year) == 2:
                start_prefix = "20" if int(start_year) <= 30 else "19"
            if not end_prefix and len(end_year) == 2:
                end_prefix = "20" if int(end_year) <= 30 else "19"

            start = int(f"{start_prefix}{start_year}")
            end = int(f"{end_prefix}{end_year}")

            components['years'].extend(range(start, end + 1))

        # Extract known makes
        text_lower = text.lower()
        known_makes = set(self.make_index.keys())
        for make in known_makes:
            if make in text_lower:
                components['makes'].append(make)

        # Extract keywords (remaining words)
        words = re.findall(r'\b[a-zA-Z]+\b', text)
        components['keywords'] = [w.lower() for w in words if len(w) > 2]

        return components

    def _find_exact_matches(self, search_text: str) -> List[ApplicationLookupEntry]:
        """Find exact text matches."""
        matches = []
        search_lower = search_text.lower()

        for entry in self.lookup_data:
            if search_lower == entry.original_text.lower():
                entry.match_score = 1.0
                matches.append(entry)

        return matches

    def _find_component_matches(self, components: Dict[str, Any]) -> List[ApplicationLookupEntry]:
        """Find matches based on parsed components."""
        candidate_entries = set()

        # Find candidates based on makes
        for make in components['makes']:
            if make in self.make_index:
                candidate_entries.update(self.make_index[make])

        # Find candidates based on years
        for year in components['years']:
            if year in self.year_index:
                candidate_entries.update(self.year_index[year])

        # If no candidates found through indexes, use all entries
        if not candidate_entries:
            candidate_entries = set(self.lookup_data)

        matches = []
        for entry in candidate_entries:
            score = self._calculate_component_match_score(entry, components)
            if score > 0.3:  # Minimum threshold
                entry.match_score = score
                matches.append(entry)

        return matches

    def _find_fuzzy_matches(self, search_text: str) -> List[ApplicationLookupEntry]:
        """Find fuzzy text matches."""
        matches = []
        search_lower = search_text.lower()

        for entry in self.lookup_data:
            similarity = SequenceMatcher(None, search_lower, entry.original_text.lower()).ratio()
            if similarity > 0.6:  # Minimum similarity threshold
                entry.match_score = similarity
                matches.append(entry)

        return matches

    @staticmethod
    def _calculate_component_match_score(entry: ApplicationLookupEntry,
                                         components: Dict[str, Any]) -> float:
        """Calculate match score based on component matching."""
        score = 0.0
        weight_total = 0.0

        # Make matching (high weight)
        make_weight = 0.4
        weight_total += make_weight
        if components['makes']:
            if entry.make.lower() in [m.lower() for m in components['makes']]:
                score += make_weight

        # Year matching (medium weight)
        year_weight = 0.3
        weight_total += year_weight
        if components['years']:
            entry_years = set(range(entry.year_start, entry.year_end + 1))
            component_years = set(components['years'])
            if entry_years.intersection(component_years):
                overlap = len(entry_years.intersection(component_years))
                total_years = len(entry_years.union(component_years))
                year_score = overlap / total_years if total_years > 0 else 0
                score += year_weight * year_score

        # Model/keyword matching (medium weight)
        keyword_weight = 0.3
        weight_total += keyword_weight
        if components['keywords']:
            entry_text = f"{entry.model} {entry.original_text}".lower()
            keyword_matches = sum(1 for keyword in components['keywords'] if keyword in entry_text)
            keyword_score = keyword_matches / len(components['keywords'])
            score += keyword_weight * keyword_score

        return score / weight_total if weight_total > 0 else 0.0