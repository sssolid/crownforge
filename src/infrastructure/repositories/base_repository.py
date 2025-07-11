# src/infrastructure/repositories/base_repository.py
"""
Base repository implementation with common functionality.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, TypeVar, Generic
from pathlib import Path

from ...domain.interfaces import DatabaseConnection

logger = logging.getLogger(__name__)

# Type variable for database connection types
ConnectionType = TypeVar('ConnectionType', bound=DatabaseConnection)


class BaseQueryRepository(Generic[ConnectionType], ABC):
    """Base repository with query template support."""

    def __init__(self, connection: ConnectionType, query_templates_path: str):
        self.connection = connection
        self.query_templates_path = Path(query_templates_path)
        self._template_cache: Dict[str, str] = {}

    @abstractmethod
    def load_query_template(self, template_name: str) -> str:
        """Load a query template from a file system."""
        pass

    def execute_template_query(self, template_name: str, params: Optional[Dict[str, Any]] = None) -> List[
        Dict[str, Any]]:
        """Execute a templated query with parameters."""
        try:
            # Load template (with caching)
            if template_name not in self._template_cache:
                self._template_cache[template_name] = self.load_query_template(template_name)

            query = self._template_cache[template_name]

            # Execute query
            return self.connection.execute_query(query, params)

        except Exception as e:
            logger.error(f"Failed to execute template query '{template_name}': {e}")
            raise

    def execute_direct_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a direct SQL query."""
        return self.connection.execute_query(query, params)

    def get_template_path(self, template_name: str) -> Path:
        """Get a full path to a template file."""
        return self.query_templates_path / f"{template_name}.sql"

    def clear_template_cache(self) -> None:
        """Clear the template cache."""
        self._template_cache.clear()
        logger.debug("Template cache cleared")


class BaseEntityRepository(BaseQueryRepository[ConnectionType], ABC):
    """Base repository for entity operations."""

    def __init__(self, connection: ConnectionType, query_templates_path: str, table_name: str):
        super().__init__(connection, query_templates_path)
        self.table_name = table_name

    def find_by_id_direct(self, entity_id: str, id_column: str = "id") -> Optional[Dict[str, Any]]:
        """Find entity by ID using a direct query."""
        query = f"SELECT * FROM {self.table_name} WHERE {id_column} = ?"
        params = {"entity_id": entity_id}
        results = self.connection.execute_query(query, params)
        return results[0] if results else None

    def count_all(self) -> int:
        """Count all records in the table."""
        query = f"SELECT COUNT(*) as count FROM {self.table_name}"
        result = self.connection.execute_query(query)
        return result[0]['count'] if result else 0

    def exists(self, entity_id: str, id_column: str = "id") -> bool:
        """Check if an entity exists."""
        query = f"SELECT 1 FROM {self.table_name} WHERE {id_column} = ? LIMIT 1"
        params = {"entity_id": entity_id}
        results = self.connection.execute_query(query, params)
        return len(results) > 0