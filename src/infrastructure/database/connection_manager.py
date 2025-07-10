# src/infrastructure/database/connection_manager.py
"""
Database connection management with configuration and retry logic.
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class DatabaseConfig:
    """Base database configuration."""
    server: str
    user: str
    password: str
    connection_timeout: int = 30
    retry_attempts: int = 3


@dataclass
class FilemakerConfig(DatabaseConfig):
    """Filemaker database configuration."""
    port: int
    database: str
    jdbc_jar_path: str


@dataclass
class IseriesConfig(DatabaseConfig):
    """iSeries/AS400 database configuration."""
    database: str
    jdbc_jar_path: str


class DatabaseConnectionError(Exception):
    """Database connection related errors."""
    pass


class ConnectionRetryMixin:
    """Mixin for connection retry logic."""

    def _execute_with_retry(self, operation_name: str, operation_func: callable, max_attempts: int = 3) -> Any:
        """Execute operation with retry logic."""
        last_exception = None

        for attempt in range(max_attempts):
            try:
                return operation_func()
            except Exception as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff
                    logger.warning(
                        f"{operation_name} failed (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"{operation_name} failed after {max_attempts} attempts: {e}")

        raise DatabaseConnectionError(f"{operation_name} failed after {max_attempts} attempts") from last_exception


class JvmManager:
    """Singleton JVM manager for JDBC connections."""

    _instance = None
    _jvm_started = False
    _jar_paths = set()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def add_jar_path(self, jar_path: str) -> None:
        """Add JAR path to classpath."""
        self._jar_paths.add(jar_path)

    def start_jvm(self) -> None:
        """Start JVM with accumulated JAR paths."""
        if self._jvm_started:
            return

        try:
            import jpype

            if not jpype.isJVMStarted():
                # Build classpath
                classpath = ":".join(self._jar_paths) if self._jar_paths else ""

                jvm_args = []
                if classpath:
                    jvm_args.append(f"-Djava.class.path={classpath}")

                # Add memory settings
                jvm_args.extend([
                    "-Xms128m",
                    "-Xmx512m",
                    "-Dfile.encoding=UTF-8"
                ])

                jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args)
                logger.info("JVM started successfully")

            self._jvm_started = True

        except Exception as e:
            logger.error(f"Failed to start JVM: {e}")
            raise DatabaseConnectionError(f"JVM startup failed: {e}") from e

    def shutdown_jvm(self) -> None:
        """Shutdown JVM."""
        try:
            import jpype
            if jpype.isJVMStarted():
                jpype.shutdownJVM()
                self._jvm_started = False
                logger.info("JVM shutdown successfully")
        except Exception as e:
            logger.warning(f"Error during JVM shutdown: {e}")


class DatabaseConnection(ABC):
    """Abstract base class for database connections."""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> list:
        """Execute SQL query and return results."""
        pass

    @abstractmethod
    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute non-query statement."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection."""
        pass


class ConnectionPool:
    """Simple connection pooling for database connections."""

    def __init__(self, connection_factory: callable, max_connections: int = 5):
        self.connection_factory = connection_factory
        self.max_connections = max_connections
        self._pool = []
        self._active_connections = set()

    def get_connection(self):
        """Get connection from pool."""
        if self._pool:
            connection = self._pool.pop()
        else:
            connection = self.connection_factory()

        self._active_connections.add(connection)
        return connection

    def return_connection(self, connection) -> None:
        """Return connection to pool."""
        if connection in self._active_connections:
            self._active_connections.remove(connection)

            if len(self._pool) < self.max_connections:
                self._pool.append(connection)
            else:
                # Close excess connections
                try:
                    if hasattr(connection, 'close'):
                        connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

    def close_all(self) -> None:
        """Close all connections in pool."""
        # Close pooled connections
        for connection in self._pool:
            try:
                if hasattr(connection, 'close'):
                    connection.close()
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {e}")

        # Close active connections
        for connection in self._active_connections.copy():
            try:
                if hasattr(connection, 'close'):
                    connection.close()
            except Exception as e:
                logger.warning(f"Error closing active connection: {e}")

        self._pool.clear()
        self._active_connections.clear()


class HealthCheckMixin:
    """Mixin for database health checking."""

    def perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        health_info = {
            'status': 'unknown',
            'connection_test': False,
            'response_time_ms': None,
            'error': None
        }

        try:
            start_time = time.time()
            health_info['connection_test'] = self.test_connection()
            end_time = time.time()

            health_info['response_time_ms'] = round((end_time - start_time) * 1000, 2)
            health_info['status'] = 'healthy' if health_info['connection_test'] else 'unhealthy'

        except Exception as e:
            health_info['status'] = 'error'
            health_info['error'] = str(e)
            logger.error(f"Health check failed: {e}")

        return health_info