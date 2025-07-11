# src/domain/validators/iseries/iseries_data_validator.py
"""
AS400/Iseries-specific data validation for sales and inventory data.
"""

import logging
from dataclasses import dataclass

from ...models import ValidationResult
from ..base_validator import BaseValidator, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class IseriesDataValidationConfig(ValidationConfig):
    """Iseries data validation configuration."""
    validate_sales_data: bool = True
    validate_cost_data: bool = True
    validate_inventory_levels: bool = True
    max_reasonable_price: float = 10000.0
    max_reasonable_cost: float = 5000.0
    warn_on_zero_inventory: bool = True


@dataclass
class IseriesSalesRecord:
    """Iseries sales record for validation."""
    part_number: str
    description: str
    units_sold: int
    revenue: float
    cost: float
    stock_level: int
    allocated: int
    jobber_price: float


class IseriesDataValidator(BaseValidator[IseriesSalesRecord]):
    """Validator for Iseries sales and inventory data."""

    def __init__(self, config: IseriesDataValidationConfig):
        super().__init__(config)
        self.as400_config = config

    def _perform_validation(self, record: IseriesSalesRecord) -> ValidationResult:
        """Validate Iseries sales record."""
        result = ValidationResult(is_valid=True)

        # Part number validation
        if not record.part_number or not record.part_number.strip():
            result.add_error("AS400-001: Part number is required")
            return result

        # Sales data validation
        if self.as400_config.validate_sales_data:
            sales_validation = self._validate_iseries_sales_data(record)
            if not sales_validation.is_valid:
                result.errors.extend([f"AS400-002: {error}" for error in sales_validation.errors])
            result.warnings.extend([f"AS400-003: {warning}" for warning in sales_validation.warnings])

        # Cost data validation
        if self.as400_config.validate_cost_data:
            cost_validation = self._validate_iseries_cost_data(record)
            if not cost_validation.is_valid:
                result.errors.extend([f"AS400-004: {error}" for error in cost_validation.errors])
            result.warnings.extend([f"AS400-005: {warning}" for warning in cost_validation.warnings])

        # Inventory validation
        if self.as400_config.validate_inventory_levels:
            inventory_validation = self._validate_iseries_inventory_levels(record)
            result.warnings.extend([f"AS400-006: {warning}" for warning in inventory_validation.warnings])

        return result

    @staticmethod
    def _validate_iseries_sales_data(record: IseriesSalesRecord) -> ValidationResult:
        """Validate Iseries sales data."""
        result = ValidationResult(is_valid=True)

        # Units sold validation
        if record.units_sold < 0:
            result.add_error("Units sold cannot be negative")
        elif record.units_sold == 0:
            result.add_warning("No units sold for this part")

        # Revenue validation
        if record.revenue < 0:
            result.add_error("Revenue cannot be negative")
        elif record.revenue == 0 and record.units_sold > 0:
            result.add_warning("Units sold but no revenue recorded")

        # Revenue consistency check
        if record.units_sold > 0 and record.jobber_price > 0:
            expected_revenue = record.units_sold * record.jobber_price
            revenue_difference = abs(record.revenue - expected_revenue)
            if revenue_difference > (expected_revenue * 0.1):  # 10% tolerance
                result.add_warning(f"Revenue inconsistency: expected {expected_revenue:.2f}, got {record.revenue:.2f}")

        return result

    def _validate_iseries_cost_data(self, record: IseriesSalesRecord) -> ValidationResult:
        """Validate Iseries cost data."""
        result = ValidationResult(is_valid=True)

        # Cost validation
        if record.cost < 0:
            result.add_error("Cost cannot be negative")
        elif record.cost == 0:
            result.add_warning("Cost is zero - may indicate missing data")
        elif record.cost > self.as400_config.max_reasonable_cost:
            result.add_warning(f"Cost seems high: ${record.cost:.2f}")

        # Price validation
        if record.jobber_price < 0:
            result.add_error("Jobber price cannot be negative")
        elif record.jobber_price == 0:
            result.add_warning("Jobber price is zero")
        elif record.jobber_price > self.as400_config.max_reasonable_price:
            result.add_warning(f"Jobber price seems high: ${record.jobber_price:.2f}")

        # Margin validation
        if record.jobber_price > 0 and record.cost > 0:
            margin = ((record.jobber_price - record.cost) / record.jobber_price) * 100
            if margin < 0:
                result.add_warning(f"Negative margin: {margin:.1f}%")
            elif margin > 90:
                result.add_warning(f"Very high margin: {margin:.1f}%")

        return result

    def _validate_iseries_inventory_levels(self, record: IseriesSalesRecord) -> ValidationResult:
        """Validate Iseries inventory levels."""
        result = ValidationResult(is_valid=True)

        # Stock level validation
        if record.stock_level < 0:
            result.add_warning("Negative stock level")
        elif record.stock_level == 0 and self.as400_config.warn_on_zero_inventory:
            result.add_warning("Zero inventory level")

        # Allocated inventory validation
        if record.allocated < 0:
            result.add_warning("Negative allocated inventory")
        elif record.allocated > record.stock_level:
            result.add_warning(f"Allocated ({record.allocated}) exceeds stock level ({record.stock_level})")

        # Available inventory
        available = record.stock_level - record.allocated
        if available < 0:
            result.add_warning("Negative available inventory")
        elif available == 0 and record.units_sold > 0:
            result.add_warning("No available inventory but recent sales recorded")

        return result