"""
Iseries Database Service
Handles connections and queries to AS400/Iseries database
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from libs.conn_iseries import Iseries
from ..utils import performance_monitor

logger = logging.getLogger(__name__)


@dataclass
class IseriesConfig:
    """Iseries database configuration"""
    server: str
    user: str
    password: str
    database: str
    jt400_jar_path: str
    connection_timeout: int = 30
    retry_attempts: int = 3


class IseriesService:
    """Service for Iseries (AS400) database operations"""

    def __init__(self, config: Dict[str, Any], query_templates_dir: str):
        self.config = IseriesConfig(**config)
        self.query_templates_dir = Path(query_templates_dir)

    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        try:
            with Iseries(
                    server=self.config.server,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    jt400_jar_path=self.config.jt400_jar_path
            ) as iseries:
                yield iseries

        except Exception as e:
            logger.error(f"Failed to establish Iseries connection: {e}")
            raise

    def load_query_template(self, template_name: str) -> str:
        """Load SQL query from template file"""
        template_path = self.query_templates_dir / f"{template_name}.sql"

        if not template_path.exists():
            raise FileNotFoundError(f"Query template not found: {template_path}")

        with open(template_path, 'r') as f:
            return f.read()

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        if params:
            query = query.format(**params)

        with self.get_connection() as iseries:
            with performance_monitor(f"Iseries Query") as monitor:
                iseries.cursor.execute(query)
                rows = iseries.cursor.fetchall()
                headers = [desc[0] for desc in iseries.cursor.description]

                # Convert to list of dictionaries
                results = []
                for row in rows:
                    record_dict = dict(zip(headers, row))
                    # Clean up string values (strip whitespace)
                    for key, value in record_dict.items():
                        if isinstance(value, str):
                            record_dict[key] = value.strip()
                    results.append(record_dict)

                monitor.increment_processed(len(results))
                logger.info(f"Iseries query returned {len(results)} records")

                return results

    def get_popularity_sales_data(self, start_date: str, branch: str = "1") -> List[Dict[str, Any]]:
        """Get sales data for popularity code calculation"""
        query = self.load_query_template("popularity_codes")

        params = {
            'date': start_date,
            'branch': branch,
            'nobranch': "" if branch != "None" else "-- "
        }

        logger.info(f"Getting popularity data from {start_date}, branch {branch}")
        return self.execute_query(query, params)

    def get_stock_data(self, branch: str = "1") -> List[Dict[str, Any]]:
        """Get current stock data"""
        query = self.load_query_template("stock_data")
        params = {'branch': branch}

        return self.execute_query(query, params)

    def get_kit_components_hierarchy(self, assembly_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get kit component hierarchy with cost discrepancies"""
        query = self.load_query_template("kit_components_hierarchy")

        params = {}
        if assembly_numbers:
            assembly_list = "', '".join(assembly_numbers)
            params["assembly_filter"] = f"AND ch.Assembly IN ('{assembly_list}')"
        else:
            params["assembly_filter"] = ""

        return self.execute_query(query, params)

    def get_measurement_data(self) -> List[Dict[str, Any]]:
        """Get measurement data for validation"""
        query = self.load_query_template("measurement_data")
        return self.execute_query(query)

    def validate_cost_discrepancies(self, part_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get cost discrepancies between systems"""
        query = self.load_query_template("cost_discrepancies")

        params = {}
        if part_numbers:
            part_list = "', '".join(part_numbers)
            params["part_filter"] = f"AND ch.Component IN ('{part_list}')"
        else:
            params["part_filter"] = ""

        return self.execute_query(query, params)