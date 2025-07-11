# src/application/services/marketing_description_service.py
"""
Marketing description service for validation and report generation.
"""

import logging
from typing import List, Dict, Any
from dataclasses import dataclass

from ...domain.models import MarketingDescription, ValidationResult, ProcessingResult
from ...domain.validators.filemaker.marketing_description_validator import (
    FilemakerMarketingDescriptionValidator
)
from ...domain.interfaces import MarketingDescriptionRepository, ReportGenerator

logger = logging.getLogger(__name__)


@dataclass
class MarketingDescriptionAnalysis:
    """Analysis results for marketing descriptions."""
    total_descriptions: int
    missing_descriptions: int
    invalid_descriptions: int
    fallback_required: int
    validation_results: List[ValidationResult]
    missing_terminology_ids: List[str]
    invalid_descriptions_data: List[MarketingDescription]
    fallback_terminology_ids: List[str]


class MarketingDescriptionService:
    """Service for marketing description validation and processing."""

    def __init__(
            self,
            repository: MarketingDescriptionRepository,
            validator: FilemakerMarketingDescriptionValidator,
            report_generator: ReportGenerator
    ):
        self.repository = repository
        self.validator = validator
        self.report_generator = report_generator

    def validate_all_descriptions(self) -> MarketingDescriptionAnalysis:
        """Validate all marketing descriptions and return analysis."""
        logger.info("Starting marketing description validation")

        try:
            # Get all descriptions
            all_descriptions = self.repository.find_all()

            # Validate each description
            validation_results = []
            for description in all_descriptions:
                result = self.validator.validate(description)
                validation_results.append(result)

            # Get validator summary - safely handle missing keys
            validator_summary = self.validator.get_validation_summary()

            # Safely extract summary data with defaults
            missing_count = validator_summary.get('filemaker_missing_descriptions', 0)
            invalid_count = validator_summary.get('filemaker_invalid_descriptions', 0)
            fallback_count = validator_summary.get('filemaker_fallback_required', 0)

            # Get detailed lists safely
            missing_ids = self.validator.get_missing_descriptions()
            invalid_data = self.validator.get_invalid_descriptions()
            fallback_ids = self.validator.get_fallback_required()

            # Create analysis
            analysis = MarketingDescriptionAnalysis(
                total_descriptions=len(all_descriptions),
                missing_descriptions=missing_count,
                invalid_descriptions=invalid_count,
                fallback_required=fallback_count,
                validation_results=validation_results,
                missing_terminology_ids=missing_ids,
                invalid_descriptions_data=invalid_data,
                fallback_terminology_ids=fallback_ids
            )

            logger.info(
                f"Marketing description validation completed: "
                f"{analysis.total_descriptions} total, "
                f"{analysis.missing_descriptions} missing, "
                f"{analysis.invalid_descriptions} invalid, "
                f"{analysis.fallback_required} requiring fallback"
            )

            return analysis

        except Exception as e:
            logger.error(f"Marketing description validation failed: {e}")
            # Return a safe default analysis
            return MarketingDescriptionAnalysis(
                total_descriptions=0,
                missing_descriptions=0,
                invalid_descriptions=0,
                fallback_required=0,
                validation_results=[],
                missing_terminology_ids=[],
                invalid_descriptions_data=[],
                fallback_terminology_ids=[]
            )

    def get_description_for_sdc(self, terminology_id: str, fallback_description: str) -> str:
        """Get marketing description for SDC template, with fallback logic."""
        try:
            description = self.repository.find_by_terminology_id(terminology_id)

            if description and description.has_jeep_description():
                return description.jeep_description
            else:
                # Log fallback usage for tracking
                if description:
                    logger.debug(f"Using fallback for terminology ID {terminology_id}: description exists but invalid")
                else:
                    logger.debug(f"Using fallback for terminology ID {terminology_id}: no description found")

                return fallback_description or ""
        except Exception as e:
            logger.warning(f"Error getting description for {terminology_id}: {e}")
            return fallback_description or ""

    def generate_validation_report(self, analysis: MarketingDescriptionAnalysis, output_path: str) -> ProcessingResult:
        """Generate marketing description validation report."""
        try:
            report_data = {
                'summary': {
                    'total_descriptions': analysis.total_descriptions,
                    'missing_descriptions': analysis.missing_descriptions,
                    'invalid_descriptions': analysis.invalid_descriptions,
                    'fallback_required': analysis.fallback_required,
                    'validation_rate': self._calculate_validation_rate(analysis)
                },
                'missing_descriptions': [
                    {'terminology_id': tid, 'status': 'Missing'}
                    for tid in analysis.missing_terminology_ids
                ],
                'invalid_descriptions': [
                    {
                        'terminology_id': desc.part_terminology_id,
                        'jeep_description': desc.jeep_description,
                        'validation_status': desc.validation_status.value,
                        'review_notes': desc.review_notes,
                        'needs_to_be_added': desc.needs_to_be_added
                    }
                    for desc in analysis.invalid_descriptions_data
                ],
                'fallback_required': [
                    {'terminology_id': tid, 'reason': 'Will use RTOffRoadAdCopy'}
                    for tid in analysis.fallback_terminology_ids
                ],
                'validation_details': [
                    {
                        'has_errors': len(result.errors) > 0,
                        'has_warnings': len(result.warnings) > 0,
                        'error_count': len(result.errors),
                        'warning_count': len(result.warnings),
                        'errors': result.errors,
                        'warnings': result.warnings
                    }
                    for result in analysis.validation_results
                ]
            }

            return self.report_generator.generate_report(report_data, output_path)
        except Exception as e:
            logger.error(f"Failed to generate validation report: {e}")
            return ProcessingResult(
                success=False,
                errors=[f"Report generation failed: {e}"]
            )

    @staticmethod
    def _calculate_validation_rate(analysis: MarketingDescriptionAnalysis) -> float:
        """Calculate validation success rate."""
        if analysis.total_descriptions == 0:
            return 0.0

        valid_count = (analysis.total_descriptions -
                       analysis.missing_descriptions -
                       analysis.invalid_descriptions)

        return (valid_count / analysis.total_descriptions) * 100.0