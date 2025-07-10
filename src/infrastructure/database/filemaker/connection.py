# src/infrastructure/database/filemaker/connection.py
"""
Filemaker database connection implementation.
"""

import logging
import jaydebeapi
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

from ..connection_manager import (
    DatabaseConnection, FilemakerConfig, JvmManager,
    DatabaseConnectionError, ConnectionRetryMixin
)

logger = logging.getLogger(__name__)


class FilemakerDatabaseConnection(DatabaseConnection, ConnectionRetryMixin):
    """Filemaker database connection implementation."""

    def __init__(self, config: FilemakerConfig):
        self.config = config
        self.jvm_manager = JvmManager()

    def _create_connection(self) -> jaydebeapi.Connection:
        """Create new Filemaker connection."""
        driver_class = "com.filemaker.jdbc.Driver"
        connection_url = f"jdbc:filemaker://{self.config.server}:{self.config.port}/{self.config.database}"

        try:
            connection = jaydebeapi.connect(
                driver_class,
                connection_url,
                [self.config.user, self.config.password],
                self.config.jdbc_jar_path
            )
            connection.jconn.setReadOnly(True)
            logger.info(f"Filemaker connection established to {self.config.server}")
            return connection
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to Filemaker: {e}") from e

    @contextmanager
    def get_connection(self):
        """Get connection context manager."""
        connection = None
        cursor = None

        try:
            connection = self._execute_with_retry(
                "Filemaker connection",
                self._create_connection,
                self.config.retry_attempts
            )
            cursor = connection.cursor()
            yield cursor
        except Exception as e:
            logger.error(f"Filemaker connection error: {e}")
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
                columns = [column[0] for column in cursor.description]

                results = []
                for row in rows:
                    record = dict(zip(columns, row))
                    # Convert Java strings to Python strings
                    cleaned_record = self._clean_java_strings(record)
                    results.append(cleaned_record)

                logger.debug(f"Filemaker query returned {len(results)} records")
                return results

            except Exception as e:
                raise DatabaseConnectionError(f"Filemaker query execution failed: {e}") from e

    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute non-query statement."""
        if params:
            query = query.format(**params)

        with self.get_connection() as cursor:
            try:
                cursor.execute(query)
                return cursor.rowcount
            except Exception as e:
                raise DatabaseConnectionError(f"Filemaker non-query execution failed: {e}") from e

    def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            with self.get_connection() as cursor:
                cursor.execute("SELECT TableName FROM FileMaker_Tables FETCH FIRST 1 ROWS ONLY")
                return True
        except Exception as e:
            logger.error(f"Filemaker connection test failed: {e}")
            return False

    def _clean_java_strings(self, record: Dict[str, Any]) -> Dict[str, Any]:
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