# src/infrastructure/repositories/filemaker/marketing_description_repository.py
"""
Filemaker marketing description repository.
"""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ...database.filemaker.connection import FilemakerDatabaseConnection
from ....domain.models import MarketingDescription, ValidationStatus
from ....domain.interfaces import MarketingDescriptionRepository
from ..base_repository import BaseQueryRepository

logger = logging.getLogger(__name__)


class FilemakerMarketingDescriptionRepository(BaseQueryRepository, MarketingDescriptionRepository):
    """Filemaker marketing description repository implementation."""

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

    def find_by_id(self, entity_id: str) -> Optional[MarketingDescription]:
        """Find marketing description by terminology ID."""
        return self.find_by_terminology_id(entity_id)

    def find_all(self) -> List[MarketingDescription]:
        """Find all marketing descriptions."""
        results = self.execute_template_query('fm_marketing_descriptions_all')
        return [self._map_to_marketing_description(record) for record in results]

    def find_by_terminology_id(self, terminology_id: str) -> Optional[MarketingDescription]:
        """Find marketing description by terminology ID."""
        params = {'terminology_id': terminology_id}
        results = self.execute_template_query('fm_marketing_description_by_id', params)

        if results:
            return self._map_to_marketing_description(results[0])
        return None

    def find_missing_descriptions(self) -> List[str]:
        """Find terminology IDs without marketing descriptions."""
        results = self.execute_template_query('fm_missing_marketing_descriptions')
        return [record['SDC_PartTerminologyID'] for record in results]

    def get_master_data_with_descriptions_for_sdc(self) -> List[Dict[str, Any]]:
        """Get master data joined with marketing descriptions for SDC template."""
        return self.execute_template_query('fm_master_data_with_marketing_descriptions')

    def get_sdc_template_data(self, missing_part_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get data for SDC template population."""
        results = self.execute_template_query('fm_sdc_template_data')

        if missing_part_numbers:
            # Filter to only include specified part numbers
            results = [r for r in results if r.get('AS400_NumberStripped') in missing_part_numbers]

        return results

    def get_upc_validation_data(self) -> List[Dict[str, Any]]:
        """Get UPC data for validation."""
        return self.execute_template_query('fm_upc_validation')

    def get_measurement_validation_data(self) -> List[Dict[str, Any]]:
        """Get measurement data for validation against Iseries."""
        return self.execute_template_query('fm_measurement_validation')

    def save(self, entity: MarketingDescription) -> None:
        """Save marketing description (not implemented for read-only access)."""
        raise NotImplementedError("Filemaker marketing description saving not implemented")

    def delete(self, entity_id: str) -> None:
        """Delete marketing description (not implemented for read-only access)."""
        raise NotImplementedError("Filemaker marketing description deletion not implemented")

    def _map_to_marketing_description(self, record: Dict[str, Any]) -> MarketingDescription:
        """Map Filemaker record to MarketingDescription domain model."""
        return MarketingDescription(
            part_terminology_id=record.get('PartTerminologyID', ''),
            jeep_description=record.get('Jeep'),
            non_jeep_description=record.get('NonJeep'),
            jeep_result=record.get('JeepResult'),
            non_jeep_result=record.get('NonJeepResult'),
            validation_status=self._map_validation_status(record.get('Validation')),
            non_jeep_validation_status=self._map_validation_status(record.get('NonJeepValidation')),
            review_notes=record.get('ReviewNotes'),
            needs_to_be_added=bool(record.get('PartTerminologyIDToBeAdded'))
        )

    def _map_validation_status(self, status_value: str) -> ValidationStatus:
        """Map database validation status to enum."""
        if not status_value:
            return ValidationStatus.MISSING

        status_lower = status_value.lower().strip()
        if status_lower in ['valid', 'validated', 'ok']:
            return ValidationStatus.VALID
        elif status_lower in ['invalid', 'error', 'failed']:
            return ValidationStatus.INVALID
        elif status_lower in ['review', 'needs review', 'pending']:
            return ValidationStatus.NEEDS_REVIEW
        else:
            return ValidationStatus.MISSING