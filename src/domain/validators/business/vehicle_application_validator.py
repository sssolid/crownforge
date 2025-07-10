# src/domain/validators/business/vehicle_application_validator.py
"""
Business logic validator for vehicle applications across all systems.
"""

import logging
import re
from typing import Set, List, Dict, Any
from dataclasses import dataclass

from ...models import VehicleApplication, ValidationResult, YearRange
from ..base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class VehicleApplicationValidationConfig(ValidationConfig):
    """Vehicle application business validation configuration."""
    min_year: int = 1900
    max_year: int = 2030
    max_year_range: int = 50
    allowed_special_chars: Set[str] = None
    valid_note_prefixes: List[str] = None
    validate_make_model_consistency: bool = True
    validate_year_logic: bool = True

    def __post_init__(self):
        if self.allowed_special_chars is None:
            self.allowed_special_chars = {";", "-", "/", "(", ")", "&", "'", '"', "."}

        if self.valid_note_prefixes is None:
            self.valid_note_prefixes = [
                "w/ ", "- ", "w/o ", "(", "lhd", "rhd", ";", "after ", "before ",
                "front", "rear", "tagged", "non-export", "2-door", "4-door",
                "< ", "except ", "instrument", "thru ", "up to ", "usa", "for us",
                "germany", "fits ", "export", "all countries", "all markets"
            ]


class VehicleApplicationBusinessValidator(BaseValidator[VehicleApplication]):
    """Business logic validator for vehicle applications."""

    def __init__(self, config: VehicleApplicationValidationConfig):
        super().__init__(config)
        self.app_config = config
        self._known_makes = self._initialize_known_makes()

    def _perform_validation(self, application: VehicleApplication) -> ValidationResult:
        """Validate vehicle application business logic."""
        result = ValidationResult(is_valid=True)

        # Year range validation
        if self.app_config.validate_year_logic:
            year_validation = self._validate_business_year_range(application.year_range)
            if not year_validation.is_valid:
                result.errors.extend([f"BIZ-APP-001: {error}" for error in year_validation.errors])
            result.warnings.extend([f"BIZ-APP-002: {warning}" for warning in year_validation.warnings])

        # Make validation
        make_validation = self._validate_vehicle_make(application.make)
        if not make_validation.is_valid:
            result.errors.extend([f"BIZ-APP-003: {error}" for error in make_validation.errors])
        result.warnings.extend([f"BIZ-APP-004: {warning}" for warning in make_validation.warnings])

        # Model validation
        model_validation = self._validate_vehicle_model(application.model, application.make)
        if not model_validation.is_valid:
            result.errors.extend([f"BIZ-APP-005: {error}" for error in model_validation.errors])
        result.warnings.extend([f"BIZ-APP-006: {warning}" for warning in model_validation.warnings])

        # Note format validation
        note_validation = self._validate_application_note_format(application.note)
        if not note_validation.is_valid:
            result.errors.extend([f"BIZ-APP-007: {error}" for error in note_validation.errors])
        result.warnings.extend([f"BIZ-APP-008: {warning}" for warning in note_validation.warnings])

        # Make-Model consistency validation
        if self.app_config.validate_make_model_consistency:
            consistency_validation = self._validate_make_model_consistency(application.make, application.model,
                                                                           application.year_range)
            result.warnings.extend([f"BIZ-APP-009: {warning}" for warning in consistency_validation.warnings])

        # Universal application validation
        universal_validation = self._validate_universal_application(application)
        result.warnings.extend([f"BIZ-APP-010: {warning}" for warning in universal_validation.warnings])

        return result

    def _validate_business_year_range(self, year_range: YearRange) -> ValidationResult:
        """Validate year range from business perspective."""
        result = ValidationResult(is_valid=True)

        # Check bounds
        if year_range.start_year < self.app_config.min_year:
            result.add_error(f"Start year {year_range.start_year} below minimum {self.app_config.min_year}")

        if year_range.end_year > self.app_config.max_year:
            result.add_error(f"End year {year_range.end_year} above maximum {self.app_config.max_year}")

        # Check range size
        if year_range.year_count() > self.app_config.max_year_range:
            result.add_error(
                f"Year range too large: {year_range.year_count()} years (max {self.app_config.max_year_range})")
        elif year_range.year_count() > 20:
            result.add_warning(
                f"Large year range: {year_range.year_count()} years - consider breaking into smaller ranges")

        # Business logic checks
        if year_range.start_year > 2025:  # Future years
            result.add_warning(f"Application starts in future year: {year_range.start_year}")

        return result

    def _validate_vehicle_make(self, make: str) -> ValidationResult:
        """Validate vehicle make."""
        result = ValidationResult(is_valid=True)

        if not make or not make.strip():
            result.add_error("Vehicle make is required")
            return result

        make_clean = make.strip()

        # Length validation
        if len(make_clean) > 50:
            result.add_error(f"Vehicle make too long: {len(make_clean)} characters")
        elif len(make_clean) < 2:
            result.add_error(f"Vehicle make too short: {len(make_clean)} characters")

        # Check against known makes
        if make_clean.lower() not in self._known_makes:
            result.add_warning(f"Unknown vehicle make: '{make_clean}'")

        # Format validation
        if not make_clean.replace(' ', '').replace('-', '').isalnum():
            result.add_warning(f"Vehicle make contains unusual characters: '{make_clean}'")

        return result

    def _validate_vehicle_model(self, model: str, make: str) -> ValidationResult:
        """Validate vehicle model."""
        result = ValidationResult(is_valid=True)

        if not model or not model.strip():
            result.add_error("Vehicle model is required")
            return result

        model_clean = model.strip()

        # Length validation
        if len(model_clean) > 50:
            result.add_error(f"Vehicle model too long: {len(model_clean)} characters")
        elif len(model_clean) < 1:
            result.add_error("Vehicle model cannot be empty")

        # Check for obvious errors
        if model_clean.lower() == make.lower():
            result.add_warning("Model name same as make name")

        return result

    def _validate_application_note_format(self, note: str) -> ValidationResult:
        """Validate application note format."""
        result = ValidationResult(is_valid=True)

        if not note:
            return result  # Empty notes are allowed

        note_stripped = note.strip()
        note_lower = note_stripped.lower()

        # Check for invalid uppercase prefixes
        if note_stripped.startswith("W/"):
            result.add_error("Note should use lowercase 'w/' not uppercase 'W/'")

        # Check for valid prefixes
        has_valid_prefix = any(note_lower.startswith(prefix) for prefix in self.app_config.valid_note_prefixes)
        if not has_valid_prefix and note_stripped:
            result.add_warning(f"Note may have invalid format: '{note_stripped[:20]}...'")

        # Check for proper semicolon ending
        if note_stripped and not note_stripped.endswith(';'):
            result.add_warning("Note should end with semicolon")

        # Check for multiple semicolons
        if note.count(';') > 1:
            result.add_error("Note contains multiple semicolons")

        # Check for common formatting issues
        if '  ' in note:
            result.add_warning("Note contains multiple consecutive spaces")

        return result

    def _validate_make_model_consistency(self, make: str, model: str, year_range: YearRange) -> ValidationResult:
        """Validate make-model consistency against known vehicle data."""
        result = ValidationResult(is_valid=True)

        # This would contain business logic to validate known make/model/year combinations
        # For now, implementing basic checks

        # Jeep-specific validations
        if make.lower() == 'jeep':
            common_jeep_models = ['wrangler', 'cherokee', 'grand cherokee', 'compass', 'patriot', 'renegade',
                                  'gladiator']
            if not any(jeep_model in model.lower() for jeep_model in common_jeep_models):
                result.add_warning(f"Uncommon Jeep model: '{model}'")

        # Ford-specific validations
        elif make.lower() == 'ford':
            if 'mustang' in model.lower() and year_range.start_year < 1964:
                result.add_warning("Mustang model before 1964 introduction year")

        return result

    def _validate_universal_application(self, application: VehicleApplication) -> ValidationResult:
        """Validate universal applications."""
        result = ValidationResult(is_valid=True)

        if application.is_universal():
            if application.code or application.model:
                result.add_warning("Universal application should not have specific code or model")
            if application.note and application.note != ";":
                result.add_warning("Universal application with specific note may not be truly universal")

        return result

    def _initialize_known_makes(self) -> Set[str]:
        """Initialize set of known vehicle makes."""
        return {
            'acura', 'alfa romeo', 'amc', 'aston martin', 'audi', 'bentley', 'bmw',
            'buick', 'cadillac', 'chevrolet', 'chrysler', 'daewoo', 'daihatsu',
            'dodge', 'eagle', 'ferrari', 'fiat', 'ford', 'geo', 'gmc', 'honda',
            'hummer', 'hyundai', 'infiniti', 'isuzu', 'jaguar', 'jeep', 'kia',
            'lamborghini', 'land rover', 'lexus', 'lincoln', 'lotus', 'maserati',
            'mazda', 'mclaren', 'mercedes-benz', 'mercury', 'mini', 'mitsubishi',
            'nissan', 'oldsmobile', 'peugeot', 'plymouth', 'pontiac', 'porsche',
            'ram', 'rolls-royce', 'saab', 'saturn', 'scion', 'subaru', 'suzuki',
            'tesla', 'toyota', 'volkswagen', 'volvo', 'universal'
        }