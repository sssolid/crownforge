# src/infrastructure/database/base_connection.py
"""
Base database connection with common JDBC functionality.
"""

import logging
from abc import abstractmethod
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import jaydebeapi

from .connection_manager import (
    DatabaseConnection, ConnectionRetryMixin, DatabaseConnectionError,
    ConnectionPool, JvmManager
)

logger = logging.getLogger(__name__)


class BaseJdbcConnection(DatabaseConnection, ConnectionRetryMixin):
    """Base class for JDBC database connections with connection pooling."""

    def __init__(self, config, driver_class: str, connection_url: str):
        self.config = config
        self.driver_class = driver_class
        self.connection_url = connection_url
        self.jvm_manager = JvmManager()
        self._connection_pool = None
        self._pool_initialized = False

    def _initialize_pool(self) -> None:
        """Initialize connection pool on first use."""
        if not self._pool_initialized:
            self._connection_pool = ConnectionPool(
                connection_factory=self._create_raw_connection,
                max_connections=5
            )
            self._pool_initialized = True
            logger.debug(f"Connection pool initialized for {self.__class__.__name__}")

    def _create_raw_connection(self) -> jaydebeapi.Connection:
        """Create a new raw JDBC connection."""
        try:
            connection = jaydebeapi.connect(
                self.driver_class,
                self.connection_url,
                [self.config.user, self.config.password],
                self.config.jdbc_jar_path
            )
            connection.jconn.setReadOnly(True)
            logger.debug(f"New {self.__class__.__name__} connection created")
            return connection
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create {self.__class__.__name__} connection: {e}") from e

    @contextmanager
    def get_connection(self):
        """Get connection from pool with context manager."""
        if not self._pool_initialized:
            self._initialize_pool()

        with self._connection_pool.get_connection() as connection:
            cursor = None
            try:
                cursor = connection.cursor()
                yield cursor
            except Exception as e:
                logger.error(f"Database operation error: {e}")
                raise
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception as e:
                        logger.warning(f"Error closing cursor: {e}")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute an SQL query and return results."""
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
                    # Clean data based on database type
                    cleaned_record = self._clean_record_data(record)
                    results.append(cleaned_record)

                logger.debug(f"Query returned {len(results)} records")
                return results

            except Exception as e:
                raise DatabaseConnectionError(f"Query execution failed: {e}") from e

    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute non-query statement."""
        if params:
            query = query.format(**params)

        with self.get_connection() as cursor:
            try:
                cursor.execute(query)
                return cursor.rowcount
            except Exception as e:
                raise DatabaseConnectionError(f"Non-query execution failed: {e}") from e

    def test_connection(self) -> bool:
        """Test if the connection is working."""
        try:
            with self.get_connection() as cursor:
                cursor.execute(self._get_test_query())
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    @abstractmethod
    def _clean_record_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean record data specific to database type."""
        pass

    @abstractmethod
    def _get_test_query(self) -> str:
        """Get test query specific to database type."""
        pass

    def close_all_connections(self) -> None:
        """Close all connections in the pool."""
        if self._connection_pool:
            self._connection_pool.close_all()
            logger.info(f"All {self.__class__.__name__} connections closed")