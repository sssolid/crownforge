# src/infrastructure/database/filemaker/connection.py
"""
Filemaker database connection implementation with connection pooling.
"""

import logging
from typing import Dict, Any

from ..base_connection import BaseJdbcConnection
from ..connection_manager import FilemakerConfig

logger = logging.getLogger(__name__)


class FilemakerDatabaseConnection(BaseJdbcConnection):
    """Filemaker database connection implementation with pooling."""

    def __init__(self, config: FilemakerConfig):
        driver_class = "com.filemaker.jdbc.Driver"
        connection_url = f"jdbc:filemaker://{config.server}:{config.port}/{config.database}"

        super().__init__(config, driver_class, connection_url)
        logger.info(f"Filemaker connection manager initialized for {config.server}")

    def _clean_record_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Java String objects to Python strings."""
        cleaned = {}
        for key, value in record.items():
            if value is None:
                cleaned[key] = None
            elif hasattr(value, 'toString'):
                # Java String object
                cleaned[key] = str(value.toString()).strip()
            elif isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        return cleaned

    def _get_test_query(self) -> str:
        """Get Filemaker-specific test query."""
        return "SELECT TableName FROM FileMaker_Tables FETCH FIRST 1 ROWS ONLY"