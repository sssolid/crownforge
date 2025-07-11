# src/domain/validators/filemaker/filemaker_data_validator.py
"""
Filemaker-specific data validation for master data integrity.
"""

import logging
from dataclasses import dataclass

from ...models import ValidationResult
from ..base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class FilemakerDataValidationConfig(ValidationConfig):
    """Filemaker data validation configuration."""
    validate_part_numbers: bool = True
    validate_upc_codes: bool = True
    validate_descriptions: bool = True
    validate_measurements: bool = True
    require_sdc_fields: bool = True


@dataclass
class FilemakerMasterRecord:
    """Filemaker master data record for validation."""
    part_number: str
    upc_code: str
    part_brand: str
    part_description: str
    sdc_part_type: str
    sdc_terminology_id: str
    length: float
    width: float
    height: float
    weight: float


class FilemakerDataValidator(BaseValidator[FilemakerMasterRecord]):
    """Validator for Filemaker master data records."""

    def __init__(self, config: FilemakerDataValidationConfig):
        super().__init__(config)
        self.fm_config = config

    def _perform_validation(self, record: FilemakerMasterRecord) -> ValidationResult:
        """Validate Filemaker master data record."""
        result = ValidationResult(is_valid=True)

        # Part number validation
        if self.fm_config.validate_part_numbers:
            if not record.part_number or not record.part_number.strip():
                result.add_error("FM001: Part number is required")
            elif len(record.part_number) > 50:
                result.add_error(f"FM002: Part number too long: {len(record.part_number)} characters")

        # UPC validation
        if self.fm_config.validate_upc_codes:
            if record.upc_code:
                upc_validation = self._validate_filemaker_upc(record.upc_code)
                if not upc_validation.is_valid:
                    result.errors.extend([f"FM003: {error}" for error in upc_validation.errors])

        # Description validation
        if self.fm_config.validate_descriptions:
            if not record.part_description or not record.part_description.strip():
                result.add_error("FM004: Part description is required")
            elif len(record.part_description) > 255:
                result.add_warning(f"FM005: Part description very long: {len(record.part_description)} characters")

        # SDC fields validation
        if self.fm_config.require_sdc_fields:
            if not record.sdc_part_type:
                result.add_error("FM006: SDC Part Type is required")
            if not record.sdc_terminology_id:
                result.add_error("FM007: SDC Terminology ID is required")

        # Measurement validation
        if self.fm_config.validate_measurements:
            measurement_validation = self._validate_filemaker_measurements(record)
            if not measurement_validation.is_valid:
                result.warnings.extend([f"FM008: {warning}" for warning in measurement_validation.warnings])

        return result

    @staticmethod
    def _validate_filemaker_upc(upc_code: str) -> ValidationResult:
        """Validate UPC code from Filemaker."""
        result = ValidationResult(is_valid=True)

        # Remove non-numeric characters
        clean_upc = ''.join(c for c in str(upc_code) if c.isdigit())

        if not clean_upc:
            result.add_error("UPC contains no digits")
            return result

        # Check valid lengths for Filemaker UPCs
        if len(clean_upc) not in [12, 13, 14]:
            result.add_error(f"Invalid UPC length: {len(clean_upc)} (expected 12, 13, or 14)")

        return result

    @staticmethod
    def _validate_filemaker_measurements(record: FilemakerMasterRecord) -> ValidationResult:
        """Validate measurements from Filemaker."""
        result = ValidationResult(is_valid=True)

        # Check for negative values
        if record.length and record.length < 0:
            result.add_warning("Length cannot be negative")
        if record.width and record.width < 0:
            result.add_warning("Width cannot be negative")
        if record.height and record.height < 0:
            result.add_warning("Height cannot be negative")
        if record.weight and record.weight < 0:
            result.add_warning("Weight cannot be negative")

        # Check for unreasonable values
        if record.length and record.length > 240:  # 20 feet
            result.add_warning(f"Length seems unreasonable: {record.length} inches")
        if record.weight and record.weight > 1000:  # 1000 lbs
            result.add_warning(f"Weight seems unreasonable: {record.weight} pounds")

        return result
