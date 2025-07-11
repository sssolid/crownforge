# src/infrastructure/database/connection_manager.py
"""
Database connection management with configuration, retry logic, and pooling.
"""

import time
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
from dataclasses import dataclass
from contextlib import contextmanager

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
    """Database connection-related errors."""
    pass


class ConnectionRetryMixin:
    """Mixin for connection retry logic."""

    @staticmethod
    def _execute_with_retry(operation_name: str, operation_func: callable, max_attempts: int = 3) -> Any:
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
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute an SQL query and return results."""
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
    """Thread-safe connection pooling for database connections."""

    def __init__(self, connection_factory: callable, max_connections: int = 5):
        self.connection_factory = connection_factory
        self.max_connections = max_connections
        self._pool: List[Any] = []
        self._active_connections = set()
        self._lock = threading.Lock()
        self._created_count = 0

    @contextmanager
    def get_connection(self):
        """Get connection from pool with context manager."""
        connection = None
        try:
            connection = self._acquire_connection()
            yield connection
        finally:
            if connection:
                self._release_connection(connection)

    def _acquire_connection(self):
        """Acquire a connection from the pool."""
        with self._lock:
            if self._pool:
                connection = self._pool.pop()
                self._active_connections.add(connection)
                return connection
            elif self._created_count < self.max_connections:
                connection = self._create_new_connection()
                self._active_connections.add(connection)
                self._created_count += 1
                return connection
            else:
                # Wait for a connection to become available
                # For simplicity, create a new one (could implement waiting)
                connection = self._create_new_connection()
                return connection

    def _release_connection(self, connection) -> None:
        """Return connection to pool."""
        with self._lock:
            if connection in self._active_connections:
                self._active_connections.remove(connection)

            if len(self._pool) < self.max_connections:
                self._pool.append(connection)
            else:
                # Close excess connections
                self._close_connection(connection)

    def _create_new_connection(self):
        """Create a new database connection."""
        return self.connection_factory()

    def _close_connection(self, connection) -> None:
        """Close a database connection."""
        try:
            if hasattr(connection, 'close'):
                connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            # Close pooled connections
            for connection in self._pool:
                self._close_connection(connection)

            # Close active connections
            for connection in self._active_connections.copy():
                self._close_connection(connection)

            self._pool.clear()
            self._active_connections.clear()
            self._created_count = 0