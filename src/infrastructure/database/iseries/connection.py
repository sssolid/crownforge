# src/infrastructure/database/iseries/connection.py
"""
AS400/Iseries database connection implementation.
"""

import logging
import jaydebeapi
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

from ..connection_manager import (
    DatabaseConnection, IseriesConfig, JvmManager,
    DatabaseConnectionError, ConnectionRetryMixin
)

logger = logging.getLogger(__name__)


class IseriesDatabaseConnection(DatabaseConnection, ConnectionRetryMixin):
    """AS400/Iseries database connection implementation."""

    def __init__(self, config: IseriesConfig):
        self.config = config
        self.jvm_manager = JvmManager()
        self._ensure_jvm_started()

    def _ensure_jvm_started(self) -> None:
        """Ensure JVM is started with JT400 JAR."""
        self.jvm_manager.add_jar_path(self.config.jdbc_jar_path)
        self.jvm_manager.start_jvm()

    def _create_connection(self) -> jaydebeapi.Connection:
        """Create new Iseries connection."""
        driver_class = "com.ibm.as400.access.AS400JDBCDriver"
        connection_url = (
            f"jdbc:as400://{self.config.server};"
            f"naming=system;libraries={self.config.database};"
            f"errors=full;date format=iso;access=read only"
        )

        try:
            connection = jaydebeapi.connect(
                driver_class,
                connection_url,
                [self.config.user, self.config.password],
                self.config.jdbc_jar_path
            )
            connection.jconn.setReadOnly(True)
            logger.info(f"Iseries connection established to {self.config.server}")
            return connection
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to Iseries: {e}") from e

    @contextmanager
    def get_connection(self):
        """Get connection context manager."""
        connection = None
        cursor = None

        try:
            connection = self._execute_with_retry(
                "Iseries connection",
                self._create_connection,
                self.config.retry_attempts
            )
            cursor = connection.cursor()
            yield cursor
        except Exception as e:
            logger.error(f"Iseries connection error: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute SQL query and return results."""
        if params:
            query = query.format(**params)

        with self.get_connection() as cursor:
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                headers = [desc[0] for desc in cursor.description]

                results = []
                for row in rows:
                    record = dict(zip(headers, row))
                    # Clean up string values
                    cleaned_record = self._clean_string_values(record)
                    results.append(cleaned_record)

                logger.debug(f"Iseries query returned {len(results)} records")
                return results

            except Exception as e:
                raise DatabaseConnectionError(f"Iseries query execution failed: {e}") from e

    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute non-query statement."""
        if params:
            query = query.format(**params)

        with self.get_connection() as cursor:
            try:
                cursor.execute(query)
                return cursor.rowcount
            except Exception as e:
                raise DatabaseConnectionError(f"Iseries non-query execution failed: {e}") from e

    def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            with self.get_connection() as cursor:
                cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                return True
        except Exception as e:
            logger.error(f"Iseries connection test failed: {e}")
            return False

    def _clean_string_values(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean string values from database."""
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        return cleaned