# src/infrastructure/repositories/iseries/measurement_repository.py
"""
Iseries measurement data repository.
"""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ...database.iseries.connection import IseriesDatabaseConnection
from ....domain.models import Measurement
from ..base_repository import BaseQueryRepository

logger = logging.getLogger(__name__)


class IseriesMeasurementRepository(BaseQueryRepository):
    """Iseries measurement data repository."""

    def __init__(self, connection: IseriesDatabaseConnection, query_templates_path: str):
        super().__init__(connection, query_templates_path)
        self.iseries_queries_path = Path(query_templates_path) / "iseries"

    def load_query_template(self, template_name: str) -> str:
        """Load Iseries-specific query template."""
        template_file = self.iseries_queries_path / f"{template_name}.sql"

        if not template_file.exists():
            raise FileNotFoundError(f"Iseries query template not found: {template_file}")

        with open(template_file, 'r', encoding='utf-8') as f:
            return f.read()

    def get_measurement_data_for_validation(self) -> List[Dict[str, Any]]:
        """Get measurement data for validation against Filemaker."""
        return self.execute_template_query('as400_measurement_data')

    def get_dimensional_weight_data(self) -> List[Dict[str, Any]]:
        """Get dimensional weight calculation data."""
        return self.execute_template_query('as400_dimensional_weight_data')

    def get_shipping_data_for_validation(self) -> List[Dict[str, Any]]:
        """Get shipping measurement data."""
        return self.execute_template_query('as400_shipping_measurement_data')

    def map_to_measurement(self, record: Dict[str, Any]) -> Measurement:
        """Map Iseries record to Measurement domain model."""
        return Measurement(
            length=self._safe_float_conversion(record.get('Length_AS400')),
            width=self._safe_float_conversion(record.get('Width_AS400')),
            height=self._safe_float_conversion(record.get('Height_AS400')),
            weight=self._safe_float_conversion(record.get('Weight_AS400'))
        )

    def _safe_float_conversion(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None