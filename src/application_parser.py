"""
Application Parser Service - Refactored
Processes automotive parts application data with improved architecture
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import json

from .database import FilemakerService
from .utils import TextProcessor, DateProcessor, ValidationUtils, performance_monitor

logger = logging.getLogger(__name__)

# ==============================================================================
# Data Models
# ==============================================================================

@dataclass
class PartApplicationRecord:
    """Raw data record from database"""
    part_number: str
    part_application: str
    part_notes_new: Optional[str] = None
    part_notes_new: Optional[str] = None
    part_notes_extra: Optional[str] = None
    part_notes: Optional[str] = None

@dataclass
class VehicleApplication:
    """Parsed vehicle application"""
    part_number: str
    year_start: int
    year_end: int
    make: str
    code: str
    model: str
    note: str
    original: str
    is_correct: bool = True

@dataclass
class VehicleAttributes:
    """Extracted vehicle attributes"""
    liter: str = ""
    fuel: str = ""
    lhd: str = ""
    rhd: str = ""
    front_brake: str = ""
    rear_brake: str = ""
    manual_transmission: str = ""
    automatic_transmission: str = ""
    transmission: str = ""
    front_axle: str = ""
    rear_axle: str = ""
    doors: str = ""

@dataclass
class ProcessingResult:
    """Result of processing operations"""
    success: bool
    data: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

@dataclass
class ApplicationConfig:
    """Application parser configuration"""
    vehicle_start_year: int = 1900
    vehicle_end_year: int = field(default_factory=lambda: datetime.now().year + 1)
    desc_width: int = 30
    lookup_file: str = "applications/application_replacements.json"
    verification_file: str = "YMM_Lookup.xlsx"
    output_file: str = "application_data.xlsx"

# ==============================================================================
# Lookup Service
# ==============================================================================

class LookupService:
    """Handles vehicle application lookups and mappings"""

    def __init__(self, lookup_file_path: str):
        self.lookup_data = self._load_lookup_data(lookup_file_path)
        self.sorted_keys = sorted(self.lookup_data.keys(), key=len, reverse=True)
        self.key_occurrences = {}

    def _load_lookup_data(self, file_path: str) -> Dict:
        """Load lookup data from JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} lookup entries")
            return data
        except Exception as e:
            logger.error(f"Failed to load lookup data: {e}")
            return {}

    def find_match(self, application_text: str) -> Optional[tuple]:
        """Find matching lookup key for application text"""
        app_lower = TextProcessor.safe_lower(application_text)

        for key in self.sorted_keys:
            if app_lower.startswith(key.lower()):
                self.key_occurrences[key] = self.key_occurrences.get(key, 0) + 1
                return key, self.lookup_data[key]

        return None

    def get_missing_keys(self) -> List[str]:
        """Get lookup keys that were never matched"""
        all_keys = set(self.lookup_data.keys())
        found_keys = set(self.key_occurrences.keys())
        return list(all_keys - found_keys)

    def get_usage_statistics(self) -> Dict[str, int]:
        """Get key usage statistics"""
        return dict(self.key_occurrences)

# ==============================================================================
# Validation Service
# ==============================================================================

class ApplicationValidationService:
    """Handles application data validation operations"""

    def __init__(self, config: ApplicationConfig):
        self.config = config
        self.validation_results = {
            'invalid_lines': [],
            'invalid_years': [],
            'illegal_characters': [],
            'date_corrections': [],
            'validated_notes': []
        }

    def validate_year_range(self, start_year: int, end_year: int,
                           part_number: str, line: str) -> bool:
        """Validate year range"""
        if start_year > end_year:
            self.validation_results['invalid_years'].append({
                "PartNumber": part_number,
                "Line": line,
                "Reason": f"Start year {start_year} > end year {end_year}"
            })
            return False

        if not (self.config.vehicle_start_year <= start_year <= self.config.vehicle_end_year and
                self.config.vehicle_start_year <= end_year <= self.config.vehicle_end_year):
            self.validation_results['invalid_years'].append({
                "PartNumber": part_number,
                "Line": line,
                "Reason": f"Year range {start_year}-{end_year} out of bounds"
            })
            return False

        return True

    def validate_application_format(self, application: str, part_number: str) -> bool:
        """Validate application string format"""
        app_stripped = TextProcessor.safe_strip(application)
        if not app_stripped:
            self.validation_results['invalid_lines'].append({
                "PartNumber": part_number,
                "Line": application,
                "Reason": "Application line is empty"
            })
            return False

        if not TextProcessor.safe_endswith(application, ";"):
            self.validation_results['invalid_lines'].append({
                "PartNumber": part_number,
                "Line": application,
                "Reason": "Does not end with ';'"
            })
            return False

        if application.count(";") > 1:
            self.validation_results['invalid_lines'].append({
                "PartNumber": part_number,
                "Line": application,
                "Reason": "Contains multiple ';'"
            })
            return False

        return True

    def check_illegal_characters(self, text: str, part_number: str) -> List[tuple]:
        """Check for illegal characters in text"""
        illegal_chars = TextProcessor.find_illegal_characters(text)

        if illegal_chars:
            highlighted_text = TextProcessor.highlight_illegal_characters(text, illegal_chars)
            self.validation_results['illegal_characters'].append({
                "PartNumber": part_number,
                "OriginalPartApplication": text,
                "IllegalCharacters": ", ".join([f"'{char}' at {idx}" for char, idx in illegal_chars]),
                "HighlightedPartApplication": highlighted_text,
                "FixSuggestion": "Remove or replace the highlighted characters"
            })

        return illegal_chars

    def validate_fitment_note(self, fitment_note: str, part_number: str) -> bool:
        """Validate part fitment note"""
        if not fitment_note:
            return True

        # Check for invalid semicolons
        note_stripped = TextProcessor.safe_strip(fitment_note)
        semicolon_position = TextProcessor.safe_rstrip(note_stripped, ";")
        if ";" in semicolon_position:
            self.validation_results['invalid_lines'].append({
                "PartNumber": part_number,
                "Line": "",
                "Reason": "Semicolon found in an invalid position in part_notes_new"
            })
            return False

        # Check for carriage returns or newlines
        if "\r" in fitment_note or "\n" in fitment_note:
            self.validation_results['invalid_lines'].append({
                "PartNumber": part_number,
                "Line": "",
                "Reason": "Carriage return or newline found in part_notes_new"
            })
            return False

        return True

    def correct_dates_in_note(self, note: str, part_number: str) -> str:
        """Correct date formats in note and track corrections"""
        corrected_note, corrections = DateProcessor.correct_dates_in_text(note, part_number)
        self.validation_results['date_corrections'].extend(corrections)
        return corrected_note

    def validate_note_format(self, note: str) -> str:
        """Validate note format and return validity status"""
        # Define valid patterns for notes
        valid_patterns = [
            re.compile(r'w/ \d\.\dL( Diesel| Gas)? Engines?;'),
            re.compile(r'w/ Dana \d{2}( Rear| Front) Axles?;'),
            re.compile(r'w/ \d{3}mm( Rear| Front) Axles?;'),
            re.compile(r'w/ (\d\.\dL, )*\d\.\dL Engines?;'),
            re.compile(r'w/ [24]-Doors?;'),
            re.compile(r'w/ (?:[A-Z0-9]+|Automatic|Manual) Transmissions?;'),
            re.compile(r'w/ NV\d{3} Transfer Cases?;'),
            re.compile(r'\(Front or Rear Brakes\);'),
            re.compile(r'\(Front Brakes\);'),
            re.compile(r'\(Rear Brakes\);'),
            re.compile(r'\(Right\);'),
            re.compile(r'\(Right Rear\);'),
            re.compile(r'\(Right Front\);'),
            re.compile(r'\(Left\);'),
            re.compile(r'\(Left Rear\);'),
            re.compile(r'\(Left Front\);'),
            re.compile(r'\(Front\);'),
            re.compile(r'\(Rear\);'),
            re.compile(r'\(Front or Rear\);'),
            re.compile(r';')  # Blank
        ]

        return 'Valid' if any(pattern.fullmatch(note.strip()) for pattern in valid_patterns) else 'Invalid'

    def add_validated_note(self, part_number: str, note: str):
        """Add note to validated notes collection"""
        validity = self.validate_note_format(note)
        self.validation_results['validated_notes'].append({
            "PartNumber": part_number,
            "Note": note,
            "Notes_Validity": validity
        })

# ==============================================================================
# Attribute Extraction Service
# ==============================================================================

class AttributeExtractionService:
    """Extracts vehicle attributes from application notes"""

    def __init__(self):
        self.extraction_patterns = self._initialize_patterns()

    def _initialize_patterns(self) -> Dict:
        """Initialize regex patterns for attribute extraction"""
        return {
            'brakes': [
                (r"\(Front or Rear Brakes\)", {"Front Brake": "Yes", "Rear Brake": "Yes"}),
                (r"\(Front Brakes\)", {"Front Brake": "Yes"}),
                (r"\(Rear Brakes\)", {"Rear Brake": "Yes"}),
            ],
            'transmission': [
                (r"w/ Manual Transmission", {"Manual Transmission": "Yes"}),
                (r"w/ Automatic Transmission", {"Automatic Transmission": "Yes"}),
                (r"w/ ([A-Za-z0-9\- ]+) Transmission", {"Transmission": "match"}),
            ],
            'drive_type': [
                (r"w/ LHD", {"LHD": "Yes"}),
                (r"w/ RHD", {"RHD": "Yes"}),
            ],
            'doors': [
                (r"w/ (\d+)-Door", {"Doors": "match"}),
            ],
            'engines': [
                (r"(\d+\.\d+L)(?:\s*(Diesel|Gas)?)?", {"Liter": "match1", "Fuel": "match2"}),
            ],
            'axles': [
                (r"w/ ([A-Za-z0-9.\-\" ]+?) Front and Rear Axles?", {"Front Axle": "match", "Rear Axle": "match"}),
                (r"w/ ([A-Za-z0-9.\-\" ]+?) Front Axles?", {"Front Axle": "match"}),
                (r"w/ ([A-Za-z0-9.\-\" ]+?) Rear Axles?", {"Rear Axle": "match"}),
            ]
        }

    def extract_attributes(self, note: str, vehicle_app: VehicleApplication) -> List[VehicleApplication]:
        """Extract all attributes from note and return expanded applications"""
        # Start with base attributes
        attributes = VehicleAttributes()
        processed_note = note

        # Extract each type of attribute
        for category, patterns in self.extraction_patterns.items():
            processed_note, attributes = self._extract_category(
                processed_note, patterns, attributes, category
            )

        # Create expanded applications based on extracted attributes
        return self._expand_applications(vehicle_app, attributes, processed_note)

    def _extract_category(self, note: str, patterns: List[tuple],
                         attributes: VehicleAttributes, category: str) -> tuple:
        """Extract specific category of attributes"""
        for pattern, field_mapping in patterns:
            matches = re.findall(pattern, note, re.IGNORECASE)

            if matches:
                # Update attributes based on matches
                for field, value in field_mapping.items():
                    if value == "match" and matches:
                        setattr(attributes, field.lower().replace(" ", "_"), matches[0])
                    elif value == "match1" and matches and isinstance(matches[0], tuple):
                        setattr(attributes, field.lower().replace(" ", "_"), matches[0][0])
                    elif value == "match2" and matches and isinstance(matches[0], tuple):
                        setattr(attributes, field.lower().replace(" ", "_"), matches[0][1] or "Gas")
                    elif value != "match":
                        setattr(attributes, field.lower().replace(" ", "_"), value)

                # Remove matched pattern from note
                note = re.sub(pattern, "", note, flags=re.IGNORECASE).strip()

        return note, attributes

    def _expand_applications(self, base_app: VehicleApplication,
                           attributes: VehicleAttributes, cleaned_note: str) -> List[VehicleApplication]:
        """Expand application based on extracted attributes"""
        # For now, return single application with attributes
        # This could be expanded to handle multiple combinations
        expanded_app = VehicleApplication(
            part_number=base_app.part_number,
            year_start=base_app.year_start,
            year_end=base_app.year_end,
            make=base_app.make,
            code=base_app.code,
            model=base_app.model,
            note=cleaned_note.strip().rstrip(";").strip(),
            original=base_app.original,
            is_correct=base_app.is_correct
        )

        return [expanded_app]

# ==============================================================================
# Main Application Parser Service
# ==============================================================================

class ApplicationParserService:
    """Main application parser service using new architecture"""

    def __init__(self, filemaker_service: FilemakerService, config: ApplicationConfig):
        self.filemaker_service = filemaker_service
        self.config = config
        self.lookup_service = LookupService(config.lookup_file)
        self.validation_service = ApplicationValidationService(config)
        self.extraction_service = AttributeExtractionService()

        self.results = {
            'correct_applications': [],
            'incorrect_applications': [],
            'invalid_lines': [],
            'invalid_years': [],
            'illegal_characters': [],
            'date_corrections': [],
            'discrepancies': [],
            'validated_notes': []
        }

    def process_all(self) -> ProcessingResult:
        """Main processing pipeline"""
        try:
            logger.info("Starting application processing pipeline")

            with performance_monitor("Application Processing") as monitor:
                # 1. Fetch raw data using the service
                raw_records = self._fetch_application_data()
                if not raw_records:
                    return ProcessingResult(False, errors=["No data fetched"])

                monitor.increment_processed(len(raw_records))

                # 2. Process each record
                processed_count = 0
                for record in raw_records:
                    if self._process_single_record(record):
                        processed_count += 1

                # 3. Generate additional outputs
                self._generate_output()

                # 4. Copy validation results
                self._copy_validation_results()

                logger.info(f"Successfully processed {processed_count} records")
                return ProcessingResult(True, data=[{"processed_count": processed_count}])

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return ProcessingResult(False, errors=[str(e)])

    def _fetch_application_data(self) -> List[Dict[str, Any]]:
        """Fetch application data using FilemakerService"""
        try:
            # Use the service to get application data
            data = self.filemaker_service.get_application_data(active_only=True)

            # Convert to the format expected by the parser
            records = []
            for record in data:
                app_record = PartApplicationRecord(
                    part_number=record.get('AS400_NumberStripped', ''),
                    part_application=record.get('PartApplication', ''),
                    part_notes_new=record.get('PartNotes_NEW'),
                    part_notes_extra=record.get('PartNotesExtra'),
                    part_notes=record.get('PartNotes')
                )
                records.append(app_record)

            logger.info(f"Fetched {len(records)} application records")
            return records

        except Exception as e:
            logger.error(f"Failed to fetch application data: {e}")
            return []

    def _process_single_record(self, record: PartApplicationRecord) -> bool:
        """Process a single database record"""
        try:
            if not record.part_application:
                self.validation_service.validation_results['invalid_lines'].append({
                    "PartNumber": record.part_number,
                    "Line": "",
                    "Reason": "No part application to process"
                })
                return False

            # Validate illegal characters
            self.validation_service.check_illegal_characters(
                record.part_application, record.part_number
            )

            # Validate fitment note
            if not self.validation_service.validate_fitment_note(
                record.part_notes_new, record.part_number
            ):
                return False

            # Split applications by newlines and process each
            applications = record.part_application.replace("\r\n", "\n").replace("\r", "\n").split("\n")

            for app_line in applications:
                self._process_application_line(app_line.strip(), record)

            return True

        except Exception as e:
            logger.error(f"Error processing record {record.part_number}: {e}")
            return False

    def _process_application_line(self, app_line: str, record: PartApplicationRecord):
        """Process a single application line"""
        if not app_line:
            return

        # Handle universal applications
        if app_line.lower().startswith("universal"):
            vehicle_app = VehicleApplication(
                part_number=record.part_number,
                year_start=0,
                year_end=0,
                make="Universal",
                code="",
                model="",
                note="",
                original=app_line
            )
            self.results['correct_applications'].append(vehicle_app)
            self.validation_service.add_validated_note(record.part_number, "")
            return

        # Parse year range
        year_match = re.match(r"^(\d{4})-(\d{4})", app_line)
        if not year_match:
            self.validation_service.validation_results['invalid_years'].append({
                "PartNumber": record.part_number,
                "Line": app_line,
                "Reason": "Does not start with YEAR-YEAR"
            })
            return

        start_year, end_year = map(int, year_match.groups())
        app_without_years = app_line[len(year_match.group(0)):].strip()

        # Validate year range
        if not self.validation_service.validate_year_range(
            start_year, end_year, record.part_number, app_line
        ):
            return

        # Validate format
        if not self.validation_service.validate_application_format(
            app_without_years, record.part_number
        ):
            return

        # Find lookup match
        match_result = self.lookup_service.find_match(app_without_years)
        if not match_result:
            self.validation_service.validation_results['invalid_lines'].append({
                "PartNumber": record.part_number,
                "Line": app_line,
                "Reason": "No matching key found"
            })
            return

        key, values = match_result

        # Extract remaining note
        remaining_note = re.sub(re.escape(key), "", app_without_years, flags=re.IGNORECASE).strip()
        note = self._build_note(remaining_note, record.part_notes_new)

        # Correct dates in note
        note = self.validation_service.correct_dates_in_note(note, record.part_number)

        # Validate note format and add to validated notes
        self.validation_service.add_validated_note(record.part_number, note)

        # Create vehicle applications for each value
        for value in values:
            make, code, model = self._parse_value(value)

            vehicle_app = VehicleApplication(
                part_number=record.part_number,
                year_start=start_year,
                year_end=end_year,
                make=make,
                code=code,
                model=model,
                note=note,
                original=app_line,
                is_correct=self._is_note_correct(note)
            )

            # Add to appropriate results
            if vehicle_app.is_correct:
                self.results['correct_applications'].append(vehicle_app)
            else:
                self.results['incorrect_applications'].append(vehicle_app)

    def _build_note(self, remaining_note: str, fitment_note: Optional[str]) -> str:
        """Build final note from remaining text and fitment note"""
        note = remaining_note.rstrip(";").strip() if remaining_note else ""

        if note and fitment_note:
            note = f"{note.rstrip(';')} {fitment_note.rstrip(';')};".strip()
        elif not note and fitment_note:
            note = f"{fitment_note.rstrip(';')};"
        else:
            note = f"{note.rstrip(';')};"

        return note

    def _is_note_correct(self, note: str) -> bool:
        """Check if note format is correct"""
        note_stripped = note.strip()
        note_lower = note_stripped.lower()

        # Special case: uppercase W/ is incorrect
        if note_stripped.startswith("W/"):
            return False

        valid_prefixes = [
            "w/ ", "- ", "w/o ", "(", "lhd", "rhd", ";", "after ", "before ",
            "front", "rear", "tagged", "non-export", "2-door", "4-door",
            "< ", "2.0l", "2.5l", "2.8l", "4.0l", "except ", "instrument",
            "thru ", "up to ", "usa", "for us", "germany", "fits ", "export"
        ]

        return any(note_lower.startswith(prefix) for prefix in valid_prefixes)

    def _parse_value(self, value: str) -> tuple:
        """Parse lookup value into make, code, model"""
        parts = value.split("|", 2)
        make = parts[0] if len(parts) > 0 else ""
        code = parts[1] if len(parts) > 1 else ""
        model = parts[2] if len(parts) > 2 else ""
        return make, code, model

    def _generate_output(self):
        """Generate additional output formats"""
        self._generate_expanded_applications()
        self._generate_application_review()
        self._generate_reconstructed_applications()
        self._generate_key_occurrences()
        self._generate_unique_words()

    def _generate_expanded_applications(self):
        """Generate year-expanded applications"""
        expanded_data = []

        for app in self.results['correct_applications']:
            try:
                start_year = int(app.year_start) if app.year_start else 0
                end_year = int(app.year_end) if app.year_end else 0

                if start_year > 0 and end_year > 0:
                    for year in range(start_year, end_year + 1):
                        expanded_row = {
                            "PartNumber": app.part_number,
                            "Make": app.make,
                            "Code": app.code,
                            "Model": app.model,
                            "Year": year,
                            "Original Note": app.note,
                            "Note": app.note,
                            "Liter": "",
                            "LHD": "",
                            "RHD": "",
                            "Front Brake": "",
                            "Rear Brake": "",
                            "Manual Transmission": "",
                            "Automatic Transmission": "",
                            "Transmission": "",
                            "Front Axle": "",
                            "Rear Axle": "",
                            "Fuel": "",
                            "Doors": ""
                        }
                        expanded_data.append(expanded_row)
            except (ValueError, TypeError):
                continue

        self.results['expanded_applications'] = expanded_data

    def _generate_application_review(self):
        """Generate application review format"""
        review_data = []

        for app in self.results['correct_applications']:
            review_row = {
                "PartNumber": app.part_number,
                "Make": app.make,
                "Code": app.code,
                "Model": app.model,
                "Year": "",
                "Original Note": app.note,
                "Note": app.note,
                "Liter": "",
                "LHD": "",
                "RHD": "",
                "Front Brake": "",
                "Rear Brake": "",
                "Manual Transmission": "",
                "Automatic Transmission": "",
                "Transmission": "",
                "Front Axle": "",
                "Rear Axle": "",
                "Fuel": "",
                "Doors": ""
            }
            review_data.append(review_row)

        self.results['application_review'] = review_data

    def _generate_reconstructed_applications(self):
        """Generate reconstructed application strings"""
        from collections import defaultdict

        # All applications
        all_applications = defaultdict(list)
        jeep_applications = defaultdict(list)

        for app in self.results['correct_applications'] + self.results['incorrect_applications']:
            # Reconstruct application line
            if app.make.lower() == "universal":
                application_line = "Universal;"
            else:
                year_range = f"{app.year_start}-{app.year_end}"
                application_line = f"{year_range} {app.make} {app.code} {app.model} {app.note}".strip()
                # Clean up formatting
                application_line = re.sub(r'\s+', ' ', application_line).rstrip(' ;') + ";"

            all_applications[app.part_number].append(application_line)

            # Jeep-specific applications
            if app.make == "Jeep":
                jeep_applications[app.part_number].append(application_line)

        # Convert to list format
        self.results['all_applications'] = [
            {"PartNumber": part_number, "Application": "\n".join(applications)}
            for part_number, applications in all_applications.items()
        ]

        self.results['jeep_applications'] = [
            {"PartNumber": part_number, "Application": "\n".join(applications)}
            for part_number, applications in jeep_applications.items()
        ]

    def _generate_key_occurrences(self):
        """Generate lookup key usage statistics"""
        self.results['key_occurrences'] = self.lookup_service.get_usage_statistics()

    def _generate_unique_words(self):
        """Generate unique words analysis"""
        from collections import defaultdict

        word_to_part_numbers = defaultdict(set)
        word_pattern = r"[^\s;]+"

        # Process all applications to extract words
        for app in self.results['correct_applications'] + self.results['incorrect_applications']:
            note = app.note
            part_number = app.part_number

            # Extract words from the note
            words = re.findall(word_pattern, note)
            for word in words:
                word_to_part_numbers[word].add(part_number)

        # Convert to list format
        self.results['unique_words'] = [
            {"Word": word, "PartNumbers": ", ".join(sorted(part_numbers))}
            for word, part_numbers in word_to_part_numbers.items()
        ]

    def _copy_validation_results(self):
        """Copy validation results to main results"""
        validation_results = self.validation_service.validation_results
        for key, value in validation_results.items():
            self.results[key] = value

# ==============================================================================
# Factory Functions
# ==============================================================================

def create_application_parser_service(filemaker_service: FilemakerService,
                                     config: Dict[str, Any]) -> ApplicationParserService:
    """Factory function to create application parser service"""
    app_config = ApplicationConfig(
        vehicle_start_year=config.get('vehicle_start_year', 1900),
        vehicle_end_year=config.get('vehicle_end_year', datetime.now().year + 1),
        desc_width=config.get('desc_width', 30),
        lookup_file=config.get('lookup_file', 'applications/application_replacements.json'),
        verification_file=config.get('verification_file', 'YMM_Lookup.xlsx'),
        output_file=config.get('output_file', 'application_data.xlsx')
    )

    return ApplicationParserService(filemaker_service, app_config)