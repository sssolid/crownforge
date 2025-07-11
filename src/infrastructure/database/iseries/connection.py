# src/infrastructure/database/iseries/connection.py
"""
AS400/Iseries database connection implementation with connection pooling.
"""

import logging
from typing import Dict, Any

from ..base_connection import BaseJdbcConnection
from ..connection_manager import IseriesConfig

logger = logging.getLogger(__name__)


class IseriesDatabaseConnection(BaseJdbcConnection):
    """AS400/Iseries database connection implementation with pooling."""

    def __init__(self, config: IseriesConfig):
        driver_class = "com.ibm.as400.access.AS400JDBCDriver"
        connection_url = (
            f"jdbc:as400://{config.server};"
            f"naming=system;libraries={config.database};"
            f"errors=full;date format=iso;access=read only"
        )

        super().__init__(config, driver_class, connection_url)
        logger.info(f"Iseries connection manager initialized for {config.server}")

    def _clean_record_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean string values from AS400 database."""
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        return cleaned

    def _get_test_query(self) -> str:
        """Get Iseries-specific test query."""
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1"