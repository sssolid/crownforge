# src/infrastructure/repositories/iseries/sales_repository.py
"""
Iseries sales and popularity data repository.
"""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass

from ...database.iseries.connection import IseriesDatabaseConnection
from ..base_repository import BaseQueryRepository

logger = logging.getLogger(__name__)


@dataclass
class IseriesSalesData:
    """Sales data from Iseries system."""
    part_number: str
    description: str
    units_sold: int
    revenue: float
    cost: float
    stock_level: int
    allocated: int
    available_stock: int
    jobber_price: float


class IseriesSalesRepository(BaseQueryRepository):
    """Iseries sales and stock data repository."""

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

    def get_popularity_sales_data(self, start_date: str, branch: str = "1") -> List[IseriesSalesData]:
        """Get sales data for popularity calculations."""
        params = {
            'date': start_date,
            'branch': branch,
            'nobranch': "" if branch != "None" else "-- "
        }

        results = self.execute_template_query('as400_popularity_codes', params)
        return [self._map_to_sales_data(record) for record in results]

    def get_stock_data(self, branch: str = "1") -> List[Dict[str, Any]]:
        """Get current stock data."""
        params = {'branch': branch}
        return self.execute_template_query('as400_stock_data', params)

    def get_cost_data_for_validation(self) -> List[Dict[str, Any]]:
        """Get cost data for validation against other systems."""
        return self.execute_template_query('as400_cost_validation_data')

    def _map_to_sales_data(self, record: Dict[str, Any]) -> IseriesSalesData:
        """Map database record to sales data."""
        return IseriesSalesData(
            part_number=record.get('Number', ''),
            description=record.get('Description', ''),
            units_sold=record.get('Sold', 0) or 0,
            revenue=record.get('Revenue', 0.0) or 0.0,
            cost=record.get('Cost', 0.0) or 0.0,
            stock_level=record.get('Stock', 0) or 0,
            allocated=record.get('Allocated', 0) or 0,
            available_stock=record.get('Stock Less Allocated', 0) or 0,
            jobber_price=record.get('Jobber', 0.0) or 0.0
        )