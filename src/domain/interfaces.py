# src/domain/interfaces.py
"""
Domain interfaces for the automotive parts data processing application.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from .models import (
    VehicleApplication, MarketingDescription, ValidationResult, ProcessingResult,
    PartNumber, ApplicationLookupEntry, UpcCode, Measurement, SalesData
)


# Repository Interfaces

class Repository(ABC):
    """Base repository interface."""

    @abstractmethod
    def find_by_id(self, entity_id: str) -> Optional[Any]:
        """Find entity by ID."""
        pass

    @abstractmethod
    def find_all(self) -> List[Any]:
        """Find all entities."""
        pass

    @abstractmethod
    def save(self, entity: Any) -> None:
        """Save entity."""
        pass

    @abstractmethod
    def delete(self, entity_id: str) -> None:
        """Delete entity by ID."""
        pass


class ApplicationRepository(Repository):
    """Repository interface for vehicle applications."""

    @abstractmethod
    def find_by_part_number(self, part_number: PartNumber) -> List[VehicleApplication]:
        """Find applications by part number."""
        pass

    @abstractmethod
    def find_by_make(self, make: str) -> List[VehicleApplication]:
        """Find applications by vehicle make."""
        pass

    @abstractmethod
    def get_raw_application_data_for_processing(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get raw application data for processing pipeline."""
        pass


class MarketingDescriptionRepository(Repository):
    """Repository interface for marketing descriptions."""

    @abstractmethod
    def find_by_terminology_id(self, terminology_id: str) -> Optional[MarketingDescription]:
        """Find marketing description by terminology ID."""
        pass

    @abstractmethod
    def find_missing_descriptions(self) -> List[str]:
        """Find terminology IDs without marketing descriptions."""
        pass

    @abstractmethod
    def get_master_data_with_descriptions_for_sdc(self) -> List[Dict[str, Any]]:
        """Get master data with marketing descriptions for SDC template."""
        pass


# Service Interfaces

class LookupService(ABC):
    """Service interface for application lookups."""

    @abstractmethod
    def find_matching_applications(self, search_text: str) -> List[ApplicationLookupEntry]:
        """Find matching applications for given text."""
        pass

    @abstractmethod
    def get_best_match(self, search_text: str) -> Optional[ApplicationLookupEntry]:
        """Get the best matching application."""
        pass

    @abstractmethod
    def load_lookup_data(self, file_path: str) -> None:
        """Load lookup data from file."""
        pass

    @abstractmethod
    def get_usage_statistics(self) -> Dict[str, int]:
        """Get lookup usage statistics."""
        pass


class ValidationService(ABC):
    """Service interface for validation operations."""

    @abstractmethod
    def validate(self, entity: Any) -> ValidationResult:
        """Validate an entity."""
        pass

    @abstractmethod
    def validate_batch(self, entities: List[Any]) -> List[ValidationResult]:
        """Validate a batch of entities."""
        pass


class ReportGenerator(ABC):
    """Interface for report generation."""

    @abstractmethod
    def generate_report(self, data: Dict[str, Any], output_path: str) -> ProcessingResult:
        """Generate report from data."""
        pass

    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Get supported output formats."""
        pass


# Configuration Interface

class ConfigurationProvider(ABC):
    """Interface for configuration management."""

    @abstractmethod
    def get_value(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value."""
        pass

    @abstractmethod
    def get_section(self, section_name: str) -> Dict[str, Any]:
        """Get configuration section."""
        pass

    @abstractmethod
    def has_key(self, key_path: str) -> bool:
        """Check if configuration key exists."""
        pass

    @abstractmethod
    def reload_configuration(self) -> None:
        """Reload configuration from source."""
        pass


# Database Connection Interface

class DatabaseConnection(ABC):
    """Interface for database connections."""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute SQL query and return results."""
        pass

    @abstractmethod
    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute non-query statement."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection."""
        pass


# Workflow Engine Interface

class WorkflowEngine(ABC):
    """Interface for workflow orchestration."""

    @abstractmethod
    def execute_workflow(self, requested_steps: Optional[List[str]] = None) -> ProcessingResult:
        """Execute workflow steps."""
        pass

    @abstractmethod
    def register_step_executor(self, step_name: str, executor: callable) -> None:
        """Register step executor function."""
        pass

    @abstractmethod
    def get_workflow_status(self) -> Dict[str, Any]:
        """Get current workflow status."""
        pass


# Event Publisher Interface

class EventPublisher(ABC):
    """Interface for publishing workflow events."""

    @abstractmethod
    def publish_step_started(self, step_name: str) -> None:
        """Publish step started event."""
        pass

    @abstractmethod
    def publish_step_completed(self, step_name: str, result: ProcessingResult) -> None:
        """Publish step completed event."""
        pass

    @abstractmethod
    def publish_step_failed(self, step_name: str, error: str) -> None:
        """Publish step failed event."""
        pass


# Validation Interfaces

class Validator(ABC):
    """Base validator interface."""

    @abstractmethod
    def validate(self, entity: Any) -> ValidationResult:
        """Validate entity."""
        pass


class ProgressTracker(ABC):
    """Interface for tracking operation progress."""

    @abstractmethod
    def start(self, total_items: int, description: str = "") -> None:
        """Start progress tracking."""
        pass

    @abstractmethod
    def update(self, items_completed: int) -> None:
        """Update progress."""
        pass

    @abstractmethod
    def finish(self, success: bool = True) -> None:
        """Finish progress tracking."""
        pass

    @abstractmethod
    def set_description(self, description: str) -> None:
        """Set current operation description."""
        pass