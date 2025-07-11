# src/infrastructure/factories/database_factory.py
"""
Factory for creating database connections and repositories.
"""

import logging
from typing import Dict, Any

from ..database.filemaker.connection import FilemakerDatabaseConnection
from ..database.iseries.connection import IseriesDatabaseConnection
from ..database.connection_manager import FilemakerConfig, IseriesConfig
from ..repositories.filemaker.application_repository import FilemakerApplicationRepository
from ..repositories.filemaker.marketing_description_repository import FilemakerMarketingDescriptionRepository
from ..repositories.iseries.sales_repository import IseriesSalesRepository
from ..repositories.iseries.kit_components_repository import IseriesKitComponentsRepository
from ..repositories.iseries.measurement_repository import IseriesMeasurementRepository

logger = logging.getLogger(__name__)


class DatabaseConnectionFactory:
    """Factory for creating database connections."""

    @staticmethod
    def create_filemaker_connection(config: Dict[str, Any]) -> FilemakerDatabaseConnection:
        """Create Filemaker database connection."""
        fm_config = FilemakerConfig(
            server=config['server'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            jdbc_jar_path=config['fmjdbc_jar_path'],
            connection_timeout=config.get('connection_timeout', 30),
            retry_attempts=config.get('retry_attempts', 3)
        )

        connection = FilemakerDatabaseConnection(fm_config)

        # Test connection
        if not connection.test_connection():
            raise RuntimeError("Failed to establish Filemaker database connection")

        logger.info("Filemaker database connection created successfully")
        return connection

    @staticmethod
    def create_iseries_connection(config: Dict[str, Any]) -> IseriesDatabaseConnection:
        """Create Iseries database connection."""
        iseries_config = IseriesConfig(
            server=config['server'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            jdbc_jar_path=config['jt400_jar_path'],
            connection_timeout=config.get('connection_timeout', 30),
            retry_attempts=config.get('retry_attempts', 3)
        )

        connection = IseriesDatabaseConnection(iseries_config)

        # Test connection
        if not connection.test_connection():
            raise RuntimeError("Failed to establish Iseries database connection")

        logger.info("Iseries database connection created successfully")
        return connection


class RepositoryFactory:
    """Factory for creating repository instances."""

    def __init__(self, query_templates_path: str):
        self.query_templates_path = query_templates_path

    def create_filemaker_application_repository(self,
                                                connection: FilemakerDatabaseConnection) -> FilemakerApplicationRepository:
        """Create Filemaker application repository."""
        return FilemakerApplicationRepository(connection, self.query_templates_path)

    def create_filemaker_marketing_description_repository(self,
                                                          connection: FilemakerDatabaseConnection) -> FilemakerMarketingDescriptionRepository:
        """Create Filemaker marketing description repository."""
        return FilemakerMarketingDescriptionRepository(connection, self.query_templates_path)

    def create_iseries_sales_repository(self, connection: IseriesDatabaseConnection) -> IseriesSalesRepository:
        """Create Iseries sales repository."""
        return IseriesSalesRepository(connection, self.query_templates_path)

    def create_iseries_kit_components_repository(self,
                                                 connection: IseriesDatabaseConnection) -> IseriesKitComponentsRepository:
        """Create Iseries kit components repository."""
        return IseriesKitComponentsRepository(connection, self.query_templates_path)

    def create_iseries_measurement_repository(self,
                                              connection: IseriesDatabaseConnection) -> IseriesMeasurementRepository:
        """Create Iseries measurement repository."""
        return IseriesMeasurementRepository(connection, self.query_templates_path)