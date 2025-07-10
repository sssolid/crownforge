# src/domain/validators/base_validator.py
"""
Base validator implementation that the existing validators can inherit from.
"""

import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Dict, Any
from dataclasses import dataclass

from ..models import ValidationResult
from ..interfaces import Validator

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class ValidationConfig:
    """Base configuration for validators."""
    strict_mode: bool = False
    collect_warnings: bool = True
    max_errors: int = 100


class BaseValidator(Generic[T], Validator):
    """Base validator with common functionality for existing validators to inherit from."""

    def __init__(self, config: ValidationConfig):
        self.config = config
        self._validation_count = 0
        self._error_count = 0
        self._warning_count = 0

    def validate(self, entity: T) -> ValidationResult:
        """Validate entity with error handling and statistics."""
        self._validation_count += 1

        try:
            result = self._perform_validation(entity)

            # Update statistics
            if result.errors:
                self._error_count += len(result.errors)
            if result.warnings:
                self._warning_count += len(result.warnings)

            # Check max errors limit
            if self.config.strict_mode and len(result.errors) > 0:
                result.is_valid = False

            return result

        except Exception as e:
            logger.error(f"Validation error for {type(entity).__name__}: {e}")
            error_result = ValidationResult(is_valid=False)
            error_result.add_error(f"Validation exception: {e}")
            return error_result

    @abstractmethod
    def _perform_validation(self, entity: T) -> ValidationResult:
        """Perform the actual validation logic - to be implemented by existing validators."""
        pass

    def get_validation_summary(self, entities: List[T] = None) -> Dict[str, Any]:
        """Get validation summary statistics."""
        return {
            'total_validations': self._validation_count,
            'total_errors': self._error_count,
            'total_warnings': self._warning_count,
            'error_rate': (self._error_count / self._validation_count * 100) if self._validation_count > 0 else 0,
            'warning_rate': (self._warning_count / self._validation_count * 100) if self._validation_count > 0 else 0
        }

    def reset_statistics(self) -> None:
        """Reset validation statistics."""
        self._validation_count = 0
        self._error_count = 0
        self._warning_count = 0