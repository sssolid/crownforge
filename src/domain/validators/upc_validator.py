# src/domain/validators/upc_validator.py
"""
UPC code validator implementation.
"""

import logging
from typing import List
from dataclasses import dataclass

from ..models import UpcCode, ValidationResult
from .base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class UpcValidationConfig(ValidationConfig):
    """UPC validation configuration."""
    validate_check_digit: bool = True
    check_duplicates: bool = True
    allowed_lengths: List[int] = None

    def __post_init__(self):
        if self.allowed_lengths is None:
            self.allowed_lengths = [12, 13, 14]


class UpcCodeValidator(BaseValidator[UpcCode]):
    """Validator for UPC codes."""

    def __init__(self, config: UpcValidationConfig):
        super().__init__(config)
        self.upc_config = config
        self._seen_upcs: set = set()

    def _perform_validation(self, upc: UpcCode) -> ValidationResult:
        """Validate UPC code."""
        result = ValidationResult(is_valid=True)

        # Length validation
        if not upc.is_valid_length():
            result.add_error(f"Invalid UPC length: {len(upc.value)} (allowed: {self.upc_config.allowed_lengths})")
            return result

        # Check digit validation
        if self.upc_config.validate_check_digit and len(upc.value) >= 12:
            check_digit_validation = self._validate_check_digit(upc)
            if not check_digit_validation.is_valid:
                result.errors.extend(check_digit_validation.errors)

        # Duplicate check
        if self.upc_config.check_duplicates:
            if upc.value in self._seen_upcs:
                result.add_warning(f"Duplicate UPC detected: {upc.value}")
            else:
                self._seen_upcs.add(upc.value)

        return result

    @staticmethod
    def _validate_check_digit(upc: UpcCode) -> ValidationResult:
        """Validate UPC check digit."""
        result = ValidationResult(is_valid=True)

        calculated_check_digit = upc.calculate_check_digit()
        if calculated_check_digit is None:
            result.add_error("Unable to calculate check digit")
            return result

        actual_check_digit = int(upc.value[-1])
        if calculated_check_digit != actual_check_digit:
            result.add_error(
                f"Invalid UPC check digit: expected {calculated_check_digit}, got {actual_check_digit}"
            )

        return result