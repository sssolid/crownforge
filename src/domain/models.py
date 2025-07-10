# src/domain/models.py
"""
Core domain models for the automotive parts data processing application.
"""

import logging
from enum import Enum
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation status enumeration."""
    VALID = "valid"
    INVALID = "invalid"
    MISSING = "missing"
    NEEDS_REVIEW = "needs_review"


class PopularityCode(Enum):
    """Popularity code enumeration."""
    A = "A"  # Top 60%
    B = "B"  # Next 20%
    C = "C"  # Next 15%
    D = "D"  # Last 5%


@dataclass
class PartNumber:
    """Value object representing a part number."""
    value: str

    def __post_init__(self):
        if not self.value or not self.value.strip():
            raise ValueError("Part number cannot be empty")
        self.value = self.value.strip()

    def __str__(self) -> str:
        return self.value


@dataclass
class YearRange:
    """Value object representing a vehicle year range."""
    start_year: int
    end_year: int

    def __post_init__(self):
        if self.start_year > self.end_year:
            raise ValueError(f"Start year {self.start_year} cannot be greater than end year {self.end_year}")

    def year_count(self) -> int:
        """Get the number of years in the range."""
        return self.end_year - self.start_year + 1

    def contains_year(self, year: int) -> bool:
        """Check if year is within the range."""
        return self.start_year <= year <= self.end_year

    def __str__(self) -> str:
        if self.start_year == self.end_year:
            return str(self.start_year)
        return f"{self.start_year}-{self.end_year}"


@dataclass
class Measurement:
    """Value object for product measurements."""
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    weight: Optional[float] = None

    def is_complete(self) -> bool:
        """Check if all measurements are provided."""
        return all(x is not None for x in [self.length, self.width, self.height, self.weight])

    def calculate_dimensional_weight(self, divisor: float = 166.0) -> Optional[float]:
        """Calculate dimensional weight (L×W×H/divisor)."""
        if self.length and self.width and self.height:
            return (self.length * self.width * self.height) / divisor
        return None


@dataclass
class VehicleApplication:
    """Domain model for vehicle applications."""
    part_number: PartNumber
    year_range: YearRange
    make: str
    code: str
    model: str
    note: str
    original_text: str
    validation_errors: List[str] = field(default_factory=list)

    def is_universal(self) -> bool:
        """Check if this is a universal application."""
        return self.make.lower() == "universal"

    def has_note(self) -> bool:
        """Check if application has a note."""
        return bool(self.note and self.note.strip() and self.note != ";")


@dataclass
class MarketingDescription:
    """Domain model for marketing descriptions."""
    part_terminology_id: str
    jeep_description: Optional[str] = None
    non_jeep_description: Optional[str] = None
    jeep_result: Optional[str] = None
    non_jeep_result: Optional[str] = None
    validation_status: ValidationStatus = ValidationStatus.MISSING
    non_jeep_validation_status: ValidationStatus = ValidationStatus.MISSING
    review_notes: Optional[str] = None
    needs_to_be_added: bool = False

    def has_jeep_description(self) -> bool:
        """Check if Jeep description exists and is valid."""
        return bool(self.jeep_description and self.jeep_description.strip())

    def has_non_jeep_description(self) -> bool:
        """Check if non-Jeep description exists and is valid."""
        return bool(self.non_jeep_description and self.non_jeep_description.strip())

    def requires_fallback(self) -> bool:
        """Check if this description requires fallback to RTOffRoadAdCopy."""
        return (not self.has_jeep_description() or
                self.validation_status in [ValidationStatus.INVALID, ValidationStatus.MISSING])


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Add an error and mark as invalid."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning."""
        self.warnings.append(warning)

    def has_issues(self) -> bool:
        """Check if there are any errors or warnings."""
        return bool(self.errors or self.warnings)


@dataclass
class ProcessingResult:
    """Result of a processing operation."""
    success: bool
    items_processed: int = 0
    items_failed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: Optional[float] = None

    def add_error(self, error: str) -> None:
        """Add an error."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str) -> None:
        """Add a warning."""
        self.warnings.append(warning)


@dataclass
class WorkflowStep:
    """Definition of a workflow step."""
    name: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    timeout_seconds: Optional[int] = None
    retry_attempts: int = 0

    def has_dependencies(self) -> bool:
        """Check if step has dependencies."""
        return bool(self.dependencies)


@dataclass
class WorkflowExecution:
    """Tracks workflow execution state."""
    started_at: datetime
    completed_at: Optional[datetime] = None
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    step_results: Dict[str, ProcessingResult] = field(default_factory=dict)
    overall_success: bool = False

    def is_running(self) -> bool:
        """Check if workflow is currently running."""
        return self.started_at is not None and self.completed_at is None

    def get_duration_seconds(self) -> float:
        """Get execution duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        elif self.started_at:
            return (datetime.now() - self.started_at).total_seconds()
        return 0.0

    def get_overall_success_rate(self) -> float:
        """Get overall success rate percentage."""
        total_steps = len(self.completed_steps) + len(self.failed_steps)
        if total_steps == 0:
            return 0.0
        return (len(self.completed_steps) / total_steps) * 100.0


@dataclass
class UpcCode:
    """Value object for UPC codes."""
    value: str

    def __post_init__(self):
        # Clean and validate UPC
        self.value = ''.join(c for c in str(self.value) if c.isdigit())
        if not self.value:
            raise ValueError("UPC must contain digits")

    def is_valid_length(self) -> bool:
        """Check if UPC has valid length."""
        return len(self.value) in [12, 13, 14]

    def calculate_check_digit(self) -> Optional[int]:
        """Calculate UPC check digit."""
        if len(self.value) < 11:
            return None

        # UPC-A check digit calculation
        digits = [int(d) for d in self.value[:11]]
        odd_sum = sum(digits[i] for i in range(0, 11, 2))
        even_sum = sum(digits[i] for i in range(1, 11, 2))
        total = (odd_sum * 3) + even_sum
        return (10 - (total % 10)) % 10


@dataclass
class SalesData:
    """Sales data for popularity calculations."""
    part_number: PartNumber
    units_sold: int
    revenue: float
    cost: float
    period_start: datetime
    period_end: datetime

    def get_profit_margin(self) -> float:
        """Calculate profit margin percentage."""
        if self.revenue <= 0:
            return 0.0
        return ((self.revenue - self.cost) / self.revenue) * 100.0


@dataclass
class ProductMaster:
    """Master product data."""
    part_number: PartNumber
    description: str
    brand: str
    upc_code: Optional[UpcCode] = None
    measurements: Optional[Measurement] = None
    price: Optional[float] = None
    cost: Optional[float] = None
    active: bool = True

    def is_kit(self) -> bool:
        """Check if product is sold as a kit."""
        # This would be determined by business rules
        return "kit" in self.description.lower()


@dataclass
class ApplicationLookupEntry:
    """Entry in application lookup data."""
    original_text: str
    make: str
    model: str
    year_start: int
    year_end: int
    code: str
    note: str
    match_score: float = 0.0

    def get_year_range(self) -> YearRange:
        """Get year range object."""
        return YearRange(self.year_start, self.year_end)