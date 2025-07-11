# src/domain/validators/iseries/measurement_validator.py
"""
Iseries measurement validation for comparison with Filemaker data.
"""

import logging
from typing import Optional
from dataclasses import dataclass

from ...models import ValidationResult
from ..base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class IseriesMeasurementValidationConfig(ValidationConfig):
    """Iseries measurement validation configuration."""
    max_length_inches: float = 240.0  # 20 feet
    max_width_inches: float = 120.0  # 10 feet
    max_height_inches: float = 120.0  # 10 feet
    max_weight_pounds: float = 1000.0  # 1000 lbs
    tolerance_percentage: float = 5.0
    validate_dimensional_weight: bool = True


@dataclass
class IseriesMeasurementRecord:
    """Iseries measurement record for validation."""
    part_number: str
    description: str
    length: Optional[float]
    width: Optional[float]
    height: Optional[float]
    weight: Optional[float]


class IseriesMeasurementValidator(BaseValidator[IseriesMeasurementRecord]):
    """Validator for Iseries measurement data."""

    def __init__(self, config: IseriesMeasurementValidationConfig):
        super().__init__(config)
        self.as400_measurement_config = config

    def _perform_validation(self, record: IseriesMeasurementRecord) -> ValidationResult:
        """Validate Iseries measurement record."""
        result = ValidationResult(is_valid=True)

        # Part number validation
        if not record.part_number or not record.part_number.strip():
            result.add_error("AS400-MEAS-001: Part number is required")
            return result

        # Validate each measurement
        measurement_validations = [
            ("Length", record.length, self.as400_measurement_config.max_length_inches),
            ("Width", record.width, self.as400_measurement_config.max_width_inches),
            ("Height", record.height, self.as400_measurement_config.max_height_inches),
            ("Weight", record.weight, self.as400_measurement_config.max_weight_pounds)
        ]

        for measurement_name, value, max_value in measurement_validations:
            validation = self._validate_single_iseries_measurement(measurement_name, value, max_value)
            if not validation.is_valid:
                result.errors.extend([f"AS400-MEAS-002: {error}" for error in validation.errors])
            result.warnings.extend([f"AS400-MEAS-003: {warning}" for warning in validation.warnings])

        # Dimensional weight validation
        if self.as400_measurement_config.validate_dimensional_weight:
            dim_weight_validation = self._validate_iseries_dimensional_weight(record)
            result.warnings.extend([f"AS400-MEAS-004: {warning}" for warning in dim_weight_validation.warnings])

        return result

    @staticmethod
    def _validate_single_iseries_measurement(name: str, value: Optional[float],
                                             max_value: float) -> ValidationResult:
        """Validate a single Iseries measurement."""
        result = ValidationResult(is_valid=True)

        if value is None:
            result.add_warning(f"{name} is not provided")
            return result

        if value < 0:
            result.add_error(f"{name} cannot be negative: {value}")
        elif value == 0:
            result.add_warning(f"{name} is zero")
        elif value > max_value:
            result.add_error(f"{name} exceeds maximum ({max_value}): {value}")

        return result

    @staticmethod
    def _validate_iseries_dimensional_weight(record: IseriesMeasurementRecord) -> ValidationResult:
        """Validate dimensional weight calculations for Iseries data."""
        result = ValidationResult(is_valid=True)

        if all(dim is not None and dim > 0 for dim in [record.length, record.width, record.height]):
            # Calculate dimensional weight (L × W × H / 166)
            dim_weight = (record.length * record.width * record.height) / 166.0

            if record.weight and record.weight > 0:
                # Compare dimensional weight to actual weight
                if dim_weight > record.weight * 1.5:  # Dimensional weight significantly higher
                    result.add_warning(
                        f"Dimensional weight ({dim_weight:.1f} lbs) much higher than actual weight ({record.weight:.1f} lbs)")
                elif record.weight > dim_weight * 2:  # Actual weight significantly higher
                    result.add_warning(
                        f"Actual weight ({record.weight:.1f} lbs) much higher than dimensional weight ({dim_weight:.1f} lbs)")

        return result