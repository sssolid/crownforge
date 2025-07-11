# src/infrastructure/repositories/iseries/kit_components_repository.py
"""
Iseries kit components and assembly data repository.
"""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass

from ...database.iseries.connection import IseriesDatabaseConnection
from ..base_repository import BaseQueryRepository

logger = logging.getLogger(__name__)


@dataclass
class IseriesKitComponent:
    """Kit component data from Iseries system."""
    assembly: str
    component: str
    quantity: int
    level: int
    cost_from_insmfh: Optional[float]
    latest_component_cost: Optional[float]
    cost_discrepancy: Optional[float]


class IseriesKitComponentsRepository(BaseQueryRepository):
    """Iseries kit components and assembly hierarchy repository."""

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

    def get_kit_components_hierarchy(self, assembly_numbers: Optional[List[str]] = None) -> List[IseriesKitComponent]:
        """Get kit component hierarchy with cost analysis."""
        params = {}
        if assembly_numbers:
            assembly_list = "', '".join(assembly_numbers)
            params["assembly_filter"] = f"AND ch.Assembly IN ('{assembly_list}')"
        else:
            params["assembly_filter"] = ""

        results = self.execute_template_query('as400_kit_components_hierarchy', params)
        return [self._map_to_kit_component(record) for record in results]

    def get_cost_discrepancies(self, part_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get cost discrepancies between component systems."""
        params = {}
        if part_numbers:
            part_list = "', '".join(part_numbers)
            params["part_filter"] = f"AND ch.Component IN ('{part_list}')"
        else:
            params["part_filter"] = ""

        return self.execute_template_query('as400_cost_discrepancies', params)

    def get_assembly_data_for_validation(self) -> List[Dict[str, Any]]:
        """Get assembly data for validation processes."""
        return self.execute_template_query('as400_assembly_validation_data')

    @staticmethod
    def _map_to_kit_component(record: Dict[str, Any]) -> IseriesKitComponent:
        """Map database record to kit component data."""
        return IseriesKitComponent(
            assembly=record.get('Assembly', ''),
            component=record.get('Component', ''),
            quantity=record.get('Quantity', 0) or 0,
            level=record.get('Level', 0) or 0,
            cost_from_insmfh=record.get('CostFromINSMFH'),
            latest_component_cost=record.get('LatestComponentCost'),
            cost_discrepancy=record.get('CostDiscrepancy')
        )