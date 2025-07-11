# src/infrastructure/factories/service_factory.py
"""
Factory for creating service instances.
"""

import logging
from typing import Dict, Any

from ...application.services.marketing_description_service import MarketingDescriptionService
from ...application.services.application_processing_service import (
    ApplicationProcessingService, ApplicationProcessingConfig
)
from ...application.services.sdc_template_service import SdcTemplateService
from ...application.services.popularity_service import PopularityCodeService, PopularityConfig
from ...application.services.simple_lookup_service import SimpleApplicationLookupService
from ..reporting.excel_report_generator import ExcelReportGenerator, ExcelReportConfig
from ..reporting.marketing_description_report_generator import MarketingDescriptionReportGenerator

logger = logging.getLogger(__name__)


class ServiceFactory:
    """Factory for creating service instances."""

    def __init__(self, repository_factory, validator_factory, report_factory):
        self.repository_factory = repository_factory
        self.validator_factory = validator_factory
        self.report_factory = report_factory

    def create_marketing_description_service(
            self,
            filemaker_connection,
            config: Dict[str, Any]
    ) -> MarketingDescriptionService:
        """Create a marketing description service."""
        repository = self.repository_factory.create_filemaker_marketing_description_repository(filemaker_connection)
        validator = self.validator_factory.create_filemaker_marketing_description_validator(
            config.get('marketing_descriptions', {}))
        report_generator = self.report_factory.create_marketing_description_report_generator()

        return MarketingDescriptionService(repository, validator, report_generator)

    def create_application_processing_service(
            self,
            filemaker_connection,
            config: Dict[str, Any]
    ) -> ApplicationProcessingService:
        """Create an application processing service."""
        repository = self.repository_factory.create_filemaker_application_repository(filemaker_connection)
        validator = self.validator_factory.create_vehicle_application_validator(config.get('validation', {}))

        # Create lookup service with default lookup file
        lookup_file = config.get('files', {}).get('lookup_file', 'data/application_replacements.json')
        lookup_service = SimpleApplicationLookupService(lookup_file)

        report_generator = self.report_factory.create_excel_report_generator()

        processing_config = ApplicationProcessingConfig(
            batch_size=config.get('batch_size', 1000),
            enable_parallel_processing=config.get('enable_parallel', False),
            max_workers=config.get('max_workers', 4)
        )

        return ApplicationProcessingService(
            repository, validator, lookup_service, report_generator, processing_config
        )

    def create_sdc_template_service(
            self,
            filemaker_connection,
            marketing_service: MarketingDescriptionService
    ) -> SdcTemplateService:
        """Create an SDC template service."""
        repository = self.repository_factory.create_filemaker_marketing_description_repository(filemaker_connection)

        return SdcTemplateService(repository, marketing_service)

    def create_popularity_code_service(
            self,
            iseries_connection,
            config: Dict[str, Any]
    ) -> PopularityCodeService:
        """Create popularity code service."""
        repository = self.repository_factory.create_iseries_sales_repository(iseries_connection)
        popularity_config = PopularityConfig(
            default_branch=config.get('default_branch', '1'),
            default_brand=config.get('default_brand', 'All'),
            default_start_date=config.get('default_start_date', '20250101'),
            thresholds=config.get('thresholds', {
                'top_tier': 60.0,
                'second_tier': 20.0,
                'third_tier': 15.0,
                'bottom_tier': 5.0
            })
        )

        return PopularityCodeService(repository, popularity_config)


class ReportGeneratorFactory:
    """Factory for creating report generator instances."""

    @staticmethod
    def create_excel_report_generator(config: Dict[str, Any] = None) -> ExcelReportGenerator:
        """Create an Excel report generator."""
        excel_config = ExcelReportConfig(
            include_formatting=config.get('include_formatting', True) if config else True,
            auto_filter=config.get('auto_filter', True) if config else True,
            freeze_headers=config.get('freeze_headers', True) if config else True,
            max_column_width=config.get('max_column_width', 50) if config else 50,
            add_summary_sheet=config.get('generate_summary', True) if config else True
        )
        return ExcelReportGenerator(excel_config)

    @staticmethod
    def create_marketing_description_report_generator(
            config: Dict[str, Any] = None) -> MarketingDescriptionReportGenerator:
        """Create marketing description report generator."""
        excel_config = ExcelReportConfig(
            include_formatting=config.get('include_formatting', True) if config else True,
            auto_filter=config.get('auto_filter', True) if config else True,
            freeze_headers=config.get('freeze_headers', True) if config else True,
            max_column_width=config.get('max_column_width', 50) if config else 50,
            add_summary_sheet=config.get('generate_summary', True) if config else True
        )
        return MarketingDescriptionReportGenerator(excel_config)