"""
Filemaker Database Service
Handles connections and queries to Filemaker database
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from dataclasses import dataclass

from libs.conn_filemaker import Filemaker
from ..utils import JavaStringConverter, performance_monitor

logger = logging.getLogger(__name__)


@dataclass
class FilemakerConfig:
    """Filemaker database configuration"""
    server: str
    port: int
    user: str
    password: str
    database: str
    fmjdbc_jar_path: str
    connection_timeout: int = 30
    retry_attempts: int = 3


class FilemakerService:
    """Service for Filemaker database operations"""

    def __init__(self, config: Dict[str, Any], query_templates_dir: str):
        self.config = FilemakerConfig(**config)
        self.query_templates_dir = Path(query_templates_dir)
        self._connection = None

    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        try:
            with Filemaker(server=self.config.server,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    fmjdbc_jar_path=self.config.fmjdbc_jar_path) as fm:
                yield fm

        except Exception as e:
            logger.error(f"Failed to establish Filemaker connection: {e}")
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

        with self.get_connection() as fm:
            with performance_monitor(f"Filemaker Query") as monitor:
                fm.cursor.execute(query)
                rows = fm.cursor.fetchall()
                columns = [column[0] for column in fm.cursor.description]

                # Convert to list of dictionaries with Java string conversion
                results = []
                for row in rows:
                    record_dict = dict(zip(columns, row))
                    # Convert Java strings to Python strings
                    converted_dict = JavaStringConverter.batch_convert_record(record_dict)
                    results.append(converted_dict)

                monitor.increment_processed(len(results))
                logger.info(f"Query returned {len(results)} records")

                return results

    def get_master_data(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get master product data"""
        query = self.load_query_template("master_data")
        params = {"active_filter": "WHERE m.ToggleActive='Yes'" if active_only else ""}

        return self.execute_query(query, params)

    def get_application_data(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get application processing data"""
        query = self.load_query_template("application_data")

        return self.execute_query(query)

    def get_popularity_data(self, missing_part_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get data for popularity codes generation"""
        query = self.load_query_template("popularity_data")
        results = self.execute_query(query)

        if missing_part_numbers:
            # Filter to only include specified part numbers
            results = [r for r in results if r.get('AS400_NumberStripped') in missing_part_numbers]

        return results

    def get_sdc_template_data(self, missing_part_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get data for SDC template population"""
        query = self.load_query_template("sdc_template_data")
        results = self.execute_query(query)

        if missing_part_numbers:
            # Filter to only include specified part numbers
            results = [r for r in results if r.get('AS400_NumberStripped') in missing_part_numbers]

        return results

    def get_interchange_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get interchange data organized by part number"""
        query = self.load_query_template("interchange_data")
        results = self.execute_query(query)

        # Organize by part number
        interchange_data = {}
        for row in results:
            part_number = row['IPTNO']
            if part_number not in interchange_data:
                interchange_data[part_number] = []
            interchange_data[part_number].append(row)

        return interchange_data

    def get_upc_validation_data(self) -> List[Dict[str, Any]]:
        """Get UPC data for validation"""
        query = self.load_query_template("upc_validation")
        return self.execute_query(query)

    def get_kit_components_data(self, assembly_numbers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get kit component data"""
        query = self.load_query_template("kit_components")
        params = {}

        if assembly_numbers:
            assembly_list = "', '".join(assembly_numbers)
            params["assembly_filter"] = f"WHERE Assembly IN ('{assembly_list}')"
        else:
            params["assembly_filter"] = ""

        return self.execute_query(query, params)

    def validate_measurements(self) -> List[Dict[str, Any]]:
        """Get measurement data for validation against Iseries"""
        query = self.load_query_template("measurement_validation")
        return self.execute_query(query)