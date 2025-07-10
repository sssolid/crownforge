# src/application/services/application_processing_service.py
"""
Application processing service with improved separation of concerns.
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ...domain.models import VehicleApplication, ProcessingResult, PartNumber
from ...domain.validators.business.vehicle_application_validator import VehicleApplicationBusinessValidator
from ...domain.interfaces import ApplicationRepository, LookupService, ReportGenerator

logger = logging.getLogger(__name__)


@dataclass
class ApplicationProcessingConfig:
    """Configuration for application processing."""
    batch_size: int = 1000
    enable_parallel_processing: bool = False
    max_workers: int = 4


@dataclass
class ApplicationProcessingResults:
    """Results from application processing."""
    total_processed: int
    valid_applications: int
    invalid_applications: int
    correct_format_applications: List[VehicleApplication]
    incorrect_format_applications: List[VehicleApplication]
    validation_errors: List[Dict[str, Any]]
    lookup_statistics: Dict[str, int]


class ApplicationProcessingService:
    """Service for processing vehicle applications."""

    def __init__(
            self,
            repository: ApplicationRepository,
            validator: VehicleApplicationBusinessValidator,
            lookup_service: LookupService,
            report_generator: ReportGenerator,
            config: ApplicationProcessingConfig
    ):
        self.repository = repository
        self.validator = validator
        self.lookup_service = lookup_service
        self.report_generator = report_generator
        self.config = config

    def process_all_applications(self) -> ApplicationProcessingResults:
        """Process all applications in the system."""
        logger.info("Starting application processing")

        # Get raw application data
        raw_data = self.repository.get_raw_application_data(active_only=True)

        # Process applications
        results = self._process_application_batch(raw_data)

        logger.info(
            f"Application processing completed: "
            f"{results.total_processed} processed, "
            f"{results.valid_applications} valid, "
            f"{results.invalid_applications} invalid"
        )

        return results

    def process_applications_for_part(self, part_number: PartNumber) -> ApplicationProcessingResults:
        """Process applications for a specific part number."""
        applications = self.repository.find_by_part_number(part_number)

        # Convert to raw data format for processing
        raw_data = [self._application_to_raw_data(app) for app in applications]

        return self._process_application_batch(raw_data)

    def _process_application_batch(self, raw_data: List[Dict[str, Any]]) -> ApplicationProcessingResults:
        """Process a batch of raw application data."""
        correct_applications = []
        incorrect_applications = []
        validation_errors = []

        for record in raw_data:
            try:
                # Parse application data
                applications = self._parse_application_record(record)

                for app in applications:
                    # Validate application
                    validation_result = self.validator.validate(app)

                    if validation_result.is_valid:
                        correct_applications.append(app)
                    else:
                        incorrect_applications.append(app)
                        validation_errors.append({
                            'part_number': app.part_number.value,
                            'errors': validation_result.errors,
                            'warnings': validation_result.warnings
                        })

            except Exception as e:
                logger.error(f"Error processing application record: {e}")
                validation_errors.append({
                    'part_number': record.get('AS400_NumberStripped', 'Unknown'),
                    'errors': [f"Processing error: {e}"],
                    'warnings': []
                })

        return ApplicationProcessingResults(
            total_processed=len(raw_data),
            valid_applications=len(correct_applications),
            invalid_applications=len(incorrect_applications),
            correct_format_applications=correct_applications,
            incorrect_format_applications=incorrect_applications,
            validation_errors=validation_errors,
            lookup_statistics=self.lookup_service.get_usage_statistics()
        )

    def _parse_application_record(self, record: Dict[str, Any]) -> List[VehicleApplication]:
        """Parse raw application record into VehicleApplication objects."""
        # This would contain the complex parsing logic from the original parser
        # For now, returning a simplified version
        applications = []

        part_number = PartNumber(record.get('AS400_NumberStripped', ''))
        application_text = record.get('PartApplication', '')

        if not application_text:
            return applications

        # Split by newlines and process each application line
        lines = application_text.replace('\r\n', '\n').replace('\r', '\n').split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            try:
                app = self._parse_application_line(part_number, line, record)
                if app:
                    applications.append(app)
            except Exception as e:
                logger.warning(f"Failed to parse application line '{line}': {e}")

        return applications

    def _parse_application_line(self, part_number: PartNumber, line: str, record: Dict[str, Any]) -> Optional[
        VehicleApplication]:
        """Parse a single application line."""
        # Simplified parsing logic - the full implementation would include
        # year range parsing, lookup matching, note building, etc.
        from ...domain.models import YearRange

        # Handle universal applications
        if line.lower().startswith('universal'):
            return VehicleApplication(
                part_number=part_number,
                year_range=YearRange(1900, 2025),
                make="Universal",
                code="",
                model="",
                note="",
                original_text=line
            )

        # For other applications, would need complex parsing logic
        # This is a placeholder implementation
        return VehicleApplication(
            part_number=part_number,
            year_range=YearRange(2000, 2025),
            make="Unknown",
            code="",
            model="Unknown",
            note="",
            original_text=line
        )

    def _application_to_raw_data(self, app: VehicleApplication) -> Dict[str, Any]:
        """Convert VehicleApplication back to raw data format."""
        return {
            'AS400_NumberStripped': app.part_number.value,
            'PartApplication': app.original_text,
            'PartNotes_NEW': app.note
        }

    def generate_processing_report(self, results: ApplicationProcessingResults, output_path: str) -> ProcessingResult:
        """Generate application processing report."""
        report_data = {
            'summary': {
                'total_processed': results.total_processed,
                'valid_applications': results.valid_applications,
                'invalid_applications': results.invalid_applications,
                'success_rate': (
                            results.valid_applications / results.total_processed * 100) if results.total_processed > 0 else 0
            },
            'correct_applications': [
                {
                    'part_number': app.part_number.value,
                    'year_start': app.year_range.start_year,
                    'year_end': app.year_range.end_year,
                    'make': app.make,
                    'code': app.code,
                    'model': app.model,
                    'note': app.note,
                    'original': app.original_text
                }
                for app in results.correct_format_applications
            ],
            'incorrect_applications': [
                {
                    'part_number': app.part_number.value,
                    'year_start': app.year_range.start_year,
                    'year_end': app.year_range.end_year,
                    'make': app.make,
                    'code': app.code,
                    'model': app.model,
                    'note': app.note,
                    'original': app.original_text,
                    'validation_errors': app.validation_errors
                }
                for app in results.incorrect_format_applications
            ],
            'validation_errors': results.validation_errors,
            'lookup_statistics': results.lookup_statistics
        }

        return self.report_generator.generate_report(report_data, output_path)