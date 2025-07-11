# src/application/services/popularity_service.py
"""
Popularity codes service with improved data processing.
"""

import logging
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ...domain.models import ProcessingResult, PopularityCode
from ...infrastructure.repositories.iseries.sales_repository import IseriesSalesRepository, IseriesSalesData

logger = logging.getLogger(__name__)


@dataclass
class PopularityConfig:
    """Popularity codes configuration."""
    default_branch: str = "1"
    default_brand: str = "All"
    start_date_format: str = "%Y%m%d"
    default_start_date: str = "20250101"
    thresholds: Dict[str, float] = None

    def __post_init__(self):
        if self.thresholds is None:
            self.thresholds = {
                'top_tier': 60.0,  # A: Top 60%
                'second_tier': 20.0,  # B: Next 20%
                'third_tier': 15.0,  # C: Next 15%
                'bottom_tier': 5.0  # D: Last 5%
            }


class PopularityCodeService:
    """Service for generating popularity codes."""

    def __init__(
            self,
            iseries_repository: IseriesSalesRepository,
            config: PopularityConfig
    ):
        self.iseries_repository = iseries_repository
        self.config = config

    def generate_popularity_codes(
            self,
            output_file: str,
            branch: Optional[str] = None,
            brand: Optional[str] = None,
            start_date: Optional[str] = None
    ) -> ProcessingResult:
        """Generate popularity codes CSV file."""
        branch = branch or self.config.default_branch
        brand = brand or self.config.default_brand
        start_date = start_date or self.config.default_start_date

        logger.info(f"Generating popularity codes for branch {branch}, brand {brand}, from {start_date}")

        try:
            # Get sales data
            sales_data = self.iseries_repository.get_popularity_sales_data(start_date, branch)

            # Get stock data
            stock_data_raw = self.iseries_repository.get_stock_data(branch)
            stock_lookup = {record.get('SNSCHR', '').strip(): record for record in stock_data_raw}

            # Process and assign popularity codes
            processed_data = self._process_popularity_data(sales_data, stock_lookup, brand)

            # Write CSV file
            self._write_popularity_csv(processed_data, output_file)

            # Calculate statistics
            total_products = len(processed_data)
            category_counts = {}
            for code in PopularityCode:
                category_counts[code.value] = len([
                    item for item in processed_data
                    if item.get('PopularityCode') == code.value
                ])

            return ProcessingResult(
                success=True,
                items_processed=total_products,
                data={
                    'output_file': output_file,
                    'total_products': total_products,
                    'category_counts': category_counts,
                    'branch': branch,
                    'brand': brand,
                    'start_date': start_date
                }
            )

        except Exception as e:
            logger.error(f"Popularity code generation failed: {e}")
            return ProcessingResult(
                success=False,
                errors=[f"Popularity code generation failed: {e}"]
            )

    def _process_popularity_data(
            self,
            sales_data: List[IseriesSalesData],
            stock_lookup: Dict[str, Any],
            brand_filter: str
    ) -> List[Dict[str, Any]]:
        """Process sales data and assign popularity codes."""
        # Filter by brand if specified
        if brand_filter != "All":
            # Would need brand info in sales data or separate lookup
            pass

        # Calculate total units sold
        total_units_sold = sum(item.units_sold for item in sales_data)

        if total_units_sold == 0:
            logger.warning("No sales data found - all products will be assigned 'D' codes")
            return self._assign_default_codes(sales_data, stock_lookup)

        # Sort by units sold (descending)
        sorted_sales = sorted(sales_data, key=lambda x: x.units_sold, reverse=True)

        # Assign popularity codes based on cumulative percentage
        processed_data = []
        cumulative_percentage = 0.0

        for sales_item in sorted_sales:
            percentage = (sales_item.units_sold / total_units_sold) * 100
            cumulative_percentage += percentage

            # Assign popularity code
            popularity_code = self._determine_popularity_code(cumulative_percentage)

            # Get stock information
            stock_info = stock_lookup.get(sales_item.part_number, {})

            processed_item = {
                'Part Number': sales_item.part_number,
                'Description': sales_item.description,
                'Sold': sales_item.units_sold,
                'Stock': stock_info.get('Stock', 0),
                'Allocated': stock_info.get('Allocated', 0),
                'Stock Less Allocated': stock_info.get('Stock', 0) - stock_info.get('Allocated', 0),
                'Jobber': stock_info.get('SRET1', 0),
                'Revenue': sales_item.revenue,
                'Cost': sales_item.cost,
                'PopularityCode': popularity_code.value,
                'Brand': '',  # Would need to be populated from product data
                'Tertiary Category': ''  # Would need to be populated from product data
            }

            processed_data.append(processed_item)

        return processed_data

    def _determine_popularity_code(self, cumulative_percentage: float) -> PopularityCode:
        """Determine popularity code based on cumulative percentage."""
        thresholds = self.config.thresholds

        if cumulative_percentage <= thresholds['top_tier']:
            return PopularityCode.A
        elif cumulative_percentage <= thresholds['top_tier'] + thresholds['second_tier']:
            return PopularityCode.B
        elif cumulative_percentage <= thresholds['top_tier'] + thresholds['second_tier'] + thresholds['third_tier']:
            return PopularityCode.C
        else:
            return PopularityCode.D

    @staticmethod
    def _assign_default_codes(
            sales_data: List[IseriesSalesData],
            stock_lookup: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Assign default 'D' codes when no sales data is available."""
        processed_data = []

        for sales_item in sales_data:
            stock_info = stock_lookup.get(sales_item.part_number, {})

            processed_item = {
                'Part Number': sales_item.part_number,
                'Description': sales_item.description,
                'Sold': 0,
                'Stock': stock_info.get('Stock', 0),
                'Allocated': stock_info.get('Allocated', 0),
                'Stock Less Allocated': stock_info.get('Stock', 0) - stock_info.get('Allocated', 0),
                'Jobber': stock_info.get('SRET1', 0),
                'Revenue': 0,
                'Cost': 0,
                'PopularityCode': PopularityCode.D.value,
                'Brand': '',
                'Tertiary Category': ''
            }

            processed_data.append(processed_item)

        return processed_data

    @staticmethod
    def _write_popularity_csv(data: List[Dict[str, Any]], output_file: str) -> None:
        """Write popularity codes to CSV file."""
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Write header
            headers = [
                'Brand', 'Tertiary Category', 'Part Number', 'Description',
                'Sold', 'Stock', 'Allocated', 'Stock Less Allocated',
                'Jobber', 'Revenue', 'Cost', 'Popularity Code'
            ]
            writer.writerow(headers)

            # Write data
            for item in data:
                writer.writerow([
                    item.get('Brand', ''),
                    item.get('Tertiary Category', ''),
                    item.get('Part Number', ''),
                    item.get('Description', ''),
                    item.get('Sold', 0),
                    item.get('Stock', 0),
                    item.get('Allocated', 0),
                    item.get('Stock Less Allocated', 0),
                    item.get('Jobber', 0),
                    item.get('Revenue', 0),
                    item.get('Cost', 0),
                    item.get('PopularityCode', 'D')
                ])

        logger.info(f"Popularity codes written to {output_file}")

    @staticmethod
    def load_popularity_mapping(csv_file: str) -> Dict[str, str]:
        """Load popularity codes from CSV file as a mapping."""
        mapping = {}

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    part_number = row.get('Part Number', '').strip()
                    popularity_code = row.get('Popularity Code', 'D').strip()
                    if part_number:
                        mapping[part_number] = popularity_code

            logger.info(f"Loaded {len(mapping)} popularity code mappings from {csv_file}")

        except FileNotFoundError:
            logger.warning(f"Popularity codes file not found: {csv_file}")
        except Exception as e:
            logger.error(f"Error loading popularity codes: {e}")

        return mapping