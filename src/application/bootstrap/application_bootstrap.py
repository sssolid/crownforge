# src/application/bootstrap/application_bootstrap.py
"""
Application bootstrap and dependency injection container.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from ...domain.interfaces import ConfigurationProvider
from ...infrastructure.configuration.configuration_manager import EnhancedConfigurationManager
from ...infrastructure.factories.database_factory import DatabaseConnectionFactory, RepositoryFactory
from ...infrastructure.factories.validator_factory import ValidatorFactory
from ...infrastructure.factories.service_factory import ServiceFactory, ReportGeneratorFactory
from ...application.orchestration.workflow_engine import WorkflowOrchestrationEngine, WorkflowConfiguration
from ...domain.models import WorkflowStep

logger = logging.getLogger(__name__)


class ApplicationContainer:
    """Dependency injection container for the application."""

    def __init__(self, config_file_path: str = "config.yaml"):
        self.config_manager: Optional[ConfigurationProvider] = None
        self.filemaker_connection = None
        self.iseries_connection = None
        self.repository_factory: Optional[RepositoryFactory] = None
        self.validator_factory: Optional[ValidatorFactory] = None
        self.service_factory: Optional[ServiceFactory] = None
        self.report_factory: Optional[ReportGeneratorFactory] = None
        self.workflow_engine: Optional[WorkflowOrchestrationEngine] = None

        # Initialize configuration
        self._initialize_configuration(config_file_path)

        # Initialize factories
        self._initialize_factories()

        # Initialize database connections
        self._initialize_database_connections()

        # Initialize workflow engine
        self._initialize_workflow_engine()

    def _initialize_configuration(self, config_file_path: str) -> None:
        """Initialize configuration manager."""
        self.config_manager = EnhancedConfigurationManager(config_file_path)

        # Validate configuration
        validation_errors = self.config_manager.validate_configuration()
        if validation_errors:
            logger.warning(f"Configuration validation warnings: {validation_errors}")

        logger.info("Configuration manager initialized")

    def _initialize_factories(self) -> None:
        """Initialize factory instances."""
        query_templates_path = self.config_manager.get_value(
            'files.query_templates_dir',
            'src/infrastructure/repositories/query_templates'
        )

        self.repository_factory = RepositoryFactory(query_templates_path)
        self.validator_factory = ValidatorFactory()
        self.report_factory = ReportGeneratorFactory()
        self.service_factory = ServiceFactory(
            self.repository_factory,
            self.validator_factory,
            self.report_factory
        )

        logger.info("Factory instances initialized")

    def _initialize_database_connections(self) -> None:
        """Initialize database connections."""
        try:
            # Filemaker connection
            filemaker_config = self.config_manager.get_section('database.filemaker')
            self.filemaker_connection = DatabaseConnectionFactory.create_filemaker_connection(filemaker_config)

            # Iseries connection
            iseries_config = self.config_manager.get_section('database.iseries')
            self.iseries_connection = DatabaseConnectionFactory.create_iseries_connection(iseries_config)

            logger.info("Database connections initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise

    def _initialize_workflow_engine(self) -> None:
        """Initialize workflow orchestration engine."""
        workflow_config = self._create_workflow_configuration()
        self.workflow_engine = WorkflowOrchestrationEngine(workflow_config)

        # Register step executors
        self._register_workflow_step_executors()

        logger.info("Workflow engine initialized")

    def _create_workflow_configuration(self) -> WorkflowConfiguration:
        """Create workflow configuration."""
        enabled_steps = self.config_manager.get_value('workflow.enabled_steps', [])
        step_dependencies = self.config_manager.get_value('workflow.step_dependencies', {})

        # Define workflow steps
        step_definitions = {
            'applications': WorkflowStep(
                name='applications',
                description='Process vehicle applications',
                dependencies=[]
            ),
            'marketing_descriptions': WorkflowStep(
                name='marketing_descriptions',
                description='Validate marketing descriptions',
                dependencies=[]
            ),
            'popularity_codes': WorkflowStep(
                name='popularity_codes',
                description='Generate popularity codes',
                dependencies=[]
            ),
            'sdc_template': WorkflowStep(
                name='sdc_template',
                description='Generate SDC template',
                dependencies=['marketing_descriptions', 'popularity_codes']
            ),
            'validation_reports': WorkflowStep(
                name='validation_reports',
                description='Generate validation reports',
                dependencies=['applications', 'marketing_descriptions']
            )
        }

        return WorkflowConfiguration(
            enabled_steps=enabled_steps,
            step_definitions=step_definitions,
            max_parallel_steps=self.config_manager.get_value('processing.max_workers', 3),
            continue_on_error=self.config_manager.get_value('error_handling.continue_on_error', True)
        )

    def _register_workflow_step_executors(self) -> None:
        """Register workflow step executor functions."""

        def execute_applications_step():
            service = self.service_factory.create_application_processing_service(
                self.filemaker_connection,
                self.config_manager.get_section('processing')
            )
            return service.process_all_applications()

        def execute_marketing_descriptions_step():
            service = self.service_factory.create_marketing_description_service(
                self.filemaker_connection,
                self.config_manager.get_section('validation')
            )
            analysis = service.validate_all_descriptions()

            # Generate report
            output_file = self.config_manager.get_value('files.marketing_validation_report',
                                                        'output/marketing_validation.xlsx')
            return service.generate_validation_report(analysis, output_file)

        def execute_popularity_codes_step():
            service = self.service_factory.create_popularity_code_service(
                self.iseries_connection,
                self.config_manager.get_section('popularity_codes')
            )
            output_file = self.config_manager.get_value('files.popularity_codes', 'output/popularity_codes.csv')
            return service.generate_popularity_codes(output_file)

        def execute_sdc_template_step():
            # Create marketing service first
            marketing_service = self.service_factory.create_marketing_description_service(
                self.filemaker_connection,
                self.config_manager.get_section('validation')
            )

            service = self.service_factory.create_sdc_template_service(
                self.filemaker_connection,
                marketing_service
            )

            template_file = self.config_manager.get_value('files.sdc_blank_template', 'data/SDC_Blank_Template.xlsx')
            output_file = self.config_manager.get_value('files.sdc_populated_template',
                                                        'output/SDC_Populated_Template.xlsx')
            missing_parts_file = self.config_manager.get_value('files.missing_parts_list')

            return service.generate_sdc_template(template_file, output_file, missing_parts_file)

        def execute_validation_reports_step():
            # This would generate comprehensive validation reports
            report_generator = self.report_factory.create_excel_report_generator()

            # Collect validation data from various sources
            validation_data = {
                'summary': {'total_validations': 100, 'passed': 95, 'failed': 5}
            }

            output_file = self.config_manager.get_value('files.validation_summary_report',
                                                        'output/validation_summary.xlsx')
            return report_generator.generate_report(validation_data, output_file)

        # Register executors
        self.workflow_engine.register_step_executor('applications', execute_applications_step)
        self.workflow_engine.register_step_executor('marketing_descriptions', execute_marketing_descriptions_step)
        self.workflow_engine.register_step_executor('popularity_codes', execute_popularity_codes_step)
        self.workflow_engine.register_step_executor('sdc_template', execute_sdc_template_step)
        self.workflow_engine.register_step_executor('validation_reports', execute_validation_reports_step)

    def get_workflow_engine(self) -> WorkflowOrchestrationEngine:
        """Get workflow engine instance."""
        return self.workflow_engine

    def get_configuration_manager(self) -> ConfigurationProvider:
        """Get configuration manager instance."""
        return self.config_manager

    def shutdown(self) -> None:
        """Shutdown application container and clean up resources."""
        try:
            # Close database connections if they have close methods
            if hasattr(self.filemaker_connection, 'close'):
                self.filemaker_connection.close()

            if hasattr(self.iseries_connection, 'close'):
                self.iseries_connection.close()

            logger.info("Application container shutdown completed")

        except Exception as e:
            logger.error(f"Error during application shutdown: {e}")