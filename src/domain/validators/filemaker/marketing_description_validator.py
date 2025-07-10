# src/domain/validators/filemaker/marketing_description_validator.py
"""
Filemaker marketing description validator with detailed validation logic.
"""

import logging
from typing import List, Dict, Any
from dataclasses import dataclass

from ...models import MarketingDescription, ValidationResult, ValidationStatus
from ..base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class FilemakerMarketingDescriptionValidationConfig(ValidationConfig):
    """Filemaker marketing description validation configuration."""
    require_jeep_description: bool = True
    require_non_jeep_description: bool = False
    max_description_length: int = 500
    min_description_length: int = 10
    validate_content_quality: bool = True
    check_placeholder_text: bool = True


class FilemakerMarketingDescriptionValidator(BaseValidator[MarketingDescription]):
    """Filemaker marketing description validator implementation."""

    def __init__(self, config: FilemakerMarketingDescriptionValidationConfig):
        super().__init__(config)
        self.fm_marketing_config = config
        self._missing_descriptions: List[str] = []
        self._invalid_descriptions: List[MarketingDescription] = []
        self._fallback_required: List[str] = []

    def _perform_validation(self, description: MarketingDescription) -> ValidationResult:
        """Validate Filemaker marketing description."""
        result = ValidationResult(is_valid=True)

        # Validate terminology ID
        if not description.part_terminology_id:
            result.add_error("FMM001: Part terminology ID is required")
            return result

        # Validate Jeep description
        if self.fm_marketing_config.require_jeep_description:
            if not description.has_jeep_description():
                result.add_error("FMM002: Jeep marketing description is required but missing")
                self._missing_descriptions.append(description.part_terminology_id)
                description.validation_status = ValidationStatus.MISSING
            else:
                jeep_validation = self._validate_filemaker_description_content(description.jeep_description, "Jeep")
                if not jeep_validation.is_valid:
                    result.errors.extend([f"FMM003: {error}" for error in jeep_validation.errors])
                    result.warnings.extend([f"FMM004: {warning}" for warning in jeep_validation.warnings])
                    description.validation_status = ValidationStatus.INVALID
                    self._invalid_descriptions.append(description)
                else:
                    description.validation_status = ValidationStatus.VALID

        # Validate non-Jeep description if provided
        if description.non_jeep_description:
            non_jeep_validation = self._validate_filemaker_description_content(description.non_jeep_description,
                                                                               "Non-Jeep")
            if not non_jeep_validation.is_valid:
                result.warnings.extend([f"FMM005: Non-Jeep - {error}" for error in non_jeep_validation.errors])
                description.non_jeep_validation_status = ValidationStatus.INVALID
            else:
                description.non_jeep_validation_status = ValidationStatus.VALID
        elif self.fm_marketing_config.require_non_jeep_description:
            result.add_warning("FMM006: Non-Jeep description is recommended but missing")
            description.non_jeep_validation_status = ValidationStatus.MISSING

        # Track fallback requirements for SDC template
        if description.requires_fallback():
            self._fallback_required.append(description.part_terminology_id)
            result.add_warning("FMM007: Will use RTOffRoadAdCopy fallback for SDC template")

        # Validate review notes
        if description.review_notes:
            review_validation = self._validate_review_notes(description.review_notes)
            result.warnings.extend([f"FMM008: {warning}" for warning in review_validation.warnings])

        # Check needs to be added flag
        if description.needs_to_be_added:
            result.add_warning("FMM009: Marketing description flagged as needing to be added to system")

        return result

    def _validate_filemaker_description_content(self, description: str, description_type: str) -> ValidationResult:
        """Validate Filemaker marketing description content quality."""
        result = ValidationResult(is_valid=True)

        if not description or not description.strip():
            result.add_error(f"{description_type} description is empty")
            return result

        # Length validation
        desc_length = len(description.strip())
        if desc_length < self.fm_marketing_config.min_description_length:
            result.add_error(
                f"{description_type} description too short ({desc_length} chars, minimum {self.fm_marketing_config.min_description_length})")
        elif desc_length > self.fm_marketing_config.max_description_length:
            result.add_error(
                f"{description_type} description too long ({desc_length} chars, maximum {self.fm_marketing_config.max_description_length})")

        # Content quality checks
        if self.fm_marketing_config.validate_content_quality:
            content_validation = self._validate_content_quality(description, description_type)
            result.errors.extend(content_validation.errors)
            result.warnings.extend(content_validation.warnings)

        return result

    def _validate_content_quality(self, description: str, description_type: str) -> ValidationResult:
        """Validate content quality specific to Filemaker marketing descriptions."""
        result = ValidationResult(is_valid=True)

        desc_lower = description.lower().strip()

        # Check for placeholder text
        if self.fm_marketing_config.check_placeholder_text:
            placeholder_patterns = ['tbd', 'to be determined', 'pending', 'n/a', 'coming soon', 'placeholder']
            for pattern in placeholder_patterns:
                if pattern in desc_lower:
                    result.add_error(f"{description_type} description contains placeholder text: '{pattern}'")

        # Check for formatting issues
        if description.count('  ') > 0:
            result.add_warning(f"{description_type} description contains multiple consecutive spaces")

        if not description.strip().endswith('.'):
            result.add_warning(f"{description_type} description should end with a period")

        if description != description.strip():
            result.add_warning(f"{description_type} description has leading/trailing whitespace")

        # Check for common marketing description issues
        if len(desc_lower.split()) < 3:
            result.add_warning(f"{description_type} description is very short and may not be descriptive enough")

        # Check for brand-specific requirements
        if 'jeep' in description_type.lower():
            if 'jeep' not in desc_lower and 'wrangler' not in desc_lower:
                result.add_warning(f"{description_type} description may not be Jeep-specific enough")

        return result

    def _validate_review_notes(self, review_notes: str) -> ValidationResult:
        """Validate review notes quality."""
        result = ValidationResult(is_valid=True)

        if len(review_notes.strip()) < 5:
            result.add_warning("Review notes are too short to be meaningful")

        if review_notes.lower().strip() in ['ok', 'good', 'fine', 'approved']:
            result.add_warning("Review notes are not descriptive enough")

        return result

    def get_filemaker_validation_summary(self) -> Dict[str, Any]:
        """Get Filemaker-specific validation summary."""
        base_summary = super().get_validation_summary([])

        return {
            **base_summary,
            'filemaker_missing_descriptions': len(self._missing_descriptions),
            'filemaker_invalid_descriptions': len(self._invalid_descriptions),
            'filemaker_fallback_required': len(self._fallback_required),
            'missing_terminology_ids': self._missing_descriptions.copy(),
            'fallback_terminology_ids': self._fallback_required.copy(),
            'validation_system': 'Filemaker'
        }