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
        """A marketing description service."""
        repository = self.repository_factory.create_filemaker_marketing_description_repository(filemaker_connection)
        validator = self.validator_factory.create_filemaker_marketing_description_validator(
            config.get('validation', {}))
        report_generator = self.report_factory.create_marketing_description_report_generator()

        return MarketingDescriptionService(repository, validator, report_generator)

    def create_application_processing_service(
            self,
            filemaker_connection,
            config: Dict[str, Any]
    ) -> ApplicationProcessingService:
        """An application processing service."""
        repository = self.repository_factory.create_filemaker_application_repository(filemaker_connection)
        validator = self.validator_factory.create_vehicle_application_validator(config.get('validation', {}))

        # This would need a lookup service - simplified for now
        lookup_service = None  # Would be created separately

        report_generator = self.report_factory.create_excel_report_generator()

        processing_config = ApplicationProcessingConfig(**config.get('processing', {}))

        return ApplicationProcessingService(
            repository, validator, lookup_service, report_generator, processing_config
        )

    def create_sdc_template_service(
            self,
            filemaker_connection,
            marketing_service: MarketingDescriptionService
    ) -> SdcTemplateService:
        """An SDC template service."""
        repository = self.repository_factory.create_filemaker_marketing_description_repository(filemaker_connection)

        return SdcTemplateService(repository, marketing_service)

    def create_popularity_code_service(
            self,
            iseries_connection,
            config: Dict[str, Any]
    ) -> PopularityCodeService:
        """Create popularity code service."""
        repository = self.repository_factory.create_iseries_sales_repository(iseries_connection)
        popularity_config = PopularityConfig(**config.get('popularity_codes', {}))

        return PopularityCodeService(repository, popularity_config)


class ReportGeneratorFactory:
    """Factory for creating report generator instances."""

    @staticmethod
    def create_excel_report_generator(config: Dict[str, Any] = None) -> ExcelReportGenerator:
        """An Excel report generator."""
        excel_config = ExcelReportConfig(**(config or {}))
        return ExcelReportGenerator(excel_config)

    @staticmethod
    def create_marketing_description_report_generator(
            config: Dict[str, Any] = None) -> MarketingDescriptionReportGenerator:
        """Create marketing description report generator."""
        excel_config = ExcelReportConfig(**(config or {}))
        return MarketingDescriptionReportGenerator(excel_config)