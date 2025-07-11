# src/infrastructure/repositories/filemaker/application_repository.py
"""
Filemaker application data repository.
"""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ...database.filemaker.connection import FilemakerDatabaseConnection
from ....domain.models import VehicleApplication, PartNumber
from ....domain.interfaces import ApplicationRepository
from ..base_repository import BaseQueryRepository

logger = logging.getLogger(__name__)


class FilemakerApplicationRepository(BaseQueryRepository, ApplicationRepository):
    """Filemaker application data repository implementation."""

    def __init__(self, connection: FilemakerDatabaseConnection, query_templates_path: str):
        super().__init__(connection, query_templates_path)
        self.filemaker_queries_path = Path(query_templates_path) / "filemaker"

    def load_query_template(self, template_name: str) -> str:
        """Load Filemaker-specific query template."""
        template_file = self.filemaker_queries_path / f"{template_name}.sql"

        if not template_file.exists():
            raise FileNotFoundError(f"Filemaker query template not found: {template_file}")

        with open(template_file, 'r', encoding='utf-8') as f:
            return f.read()

    def find_by_id(self, entity_id: str) -> Optional[VehicleApplication]:
        """Find an application by ID (not applicable for applications)."""
        return None

    def find_all(self) -> List[VehicleApplication]:
        """Find all vehicle applications."""
        results = self.execute_template_query('fm_application_data')
        return [self._map_to_application(record) for record in results]

    def find_by_part_number(self, part_number: PartNumber) -> List[VehicleApplication]:
        """Find applications by part number."""
        params = {'part_number': part_number.value}
        results = self.execute_template_query('fm_application_by_part_number', params)
        return [self._map_to_application(record) for record in results]

    def find_by_make(self, make: str) -> List[VehicleApplication]:
        """Find applications by vehicle make."""
        params = {'make': make}
        results = self.execute_template_query('fm_application_by_make', params)
        return [self._map_to_application(record) for record in results]

    def get_raw_application_data_for_processing(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get raw application data for processing pipeline."""
        template_name = 'fm_application_data_active' if active_only else 'fm_application_data_all'
        return self.execute_template_query(template_name)

    def get_raw_application_data(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get raw application data (alias for processing method)."""
        return self.get_raw_application_data_for_processing(active_only)

    def save(self, entity: VehicleApplication) -> None:
        """Save the application (not implemented for read-only access)."""
        raise NotImplementedError("Filemaker application saving not implemented")

    def delete(self, entity_id: str) -> None:
        """Delete the application (not implemented for read-only access)."""
        raise NotImplementedError("Filemaker application deletion not implemented")

    @staticmethod
    def _map_to_application(record: Dict[str, Any]) -> VehicleApplication:
        """Map Filemaker record to VehicleApplication domain model."""
        from ....domain.models import YearRange

        # This would contain proper parsing logic based on Filemaker data structure
        return VehicleApplication(
            part_number=PartNumber(record.get('AS400_NumberStripped', '')),
            year_range=YearRange(1900, 2025),  # Would parse from record
            make=record.get('Make', ''),
            code=record.get('Code', ''),
            model=record.get('Model', ''),
            note=record.get('Note', ''),
            original_text=record.get('PartApplication', '')
        )